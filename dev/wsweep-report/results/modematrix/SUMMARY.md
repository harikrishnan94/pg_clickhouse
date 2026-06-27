# Transport-mode matrix vs optimal-native PostgreSQL — W=8, SF=10 / full ClickBench

**Method:** best-of-3 (min) wall, ClickBench-style. Each offload transport timed against the SAME
same-session optimal-native baseline (offload off, JIT, parallel costs 0, `max_parallel_workers_per_gather=8`;
`work_mem`=2GB TPC-H / 1GB ClickBench). Native + all 6 modes measured adjacent per query (drift control).
Both PG tree + ClickHouse in one cgroup capped to `cpu.max=8`. pg_clickhouse `streamed-table-shm-offload`
@ phase-3 HEAD; ClickHouse `streamed_table` @ C1. Phase split from one separate `shm_log_stream_stats` run
(kept out of the timed min — it adds ~38% on scan-bound queries).

## Headline (fair common-set geomean: queries ALL 6 modes completed correctly)

| mode | ClickBench (30 q) | TPC-H (11 q) |
|------|--:|--:|
| **SHM adopt (zero-copy)** | **2.41×** | **1.13×** |
| SHM copy | 2.41× | 1.13× |
| TCP epoll | 2.38× | 1.06× |
| TCP blocking | 2.37× | 1.06× |
| Arrow IPC | 2.36× | 1.06× |
| TCP msg_zerocopy | 2.34× | 1.04× |

**Transport ranking (consistent across both benches): SHM-adopt ≈ SHM-copy > TCP-epoll ≈ TCP-blocking ≈
Arrow > TCP-msg_zerocopy.** The spread is small (~3% ClickBench, ~8% TPC-H) — on loopback the transport is
nearly a wash; SHM zero-copy is marginally best, TCP-zerocopy marginally worst.

- **ClickBench: offload is ~2.3–2.4× faster than optimal-native PG** (consumer/CH-compute-bound → transport
  overhead is a small fraction). Best: Q17 7.9×, Q33 ~14× (top-N), Q30 4.7×, Q10 4.5×, Q9 4.2×, Q6 4.0×.
  Only loss: Q24 0.58–0.76× (fast SELECT-* query, offload fixed cost dominates).
- **TPC-H: ~1.1× (break-even to modest win)** — big wins on CPU-heavy aggs (Q14 6.0×, Q9 3.0×, Q13 2.8×,
  Q1 2.6×), big losses on tiny/scan-bound (Q19 0.10×, Q6 0.36×, Q4 0.43×) where native PG is already fast.

## Where the time goes (median over offloaded queries; producer phases Σ across W workers)

### ClickBench
| mode | prod READ | DEFORM | PUBLISH | STALL | cons ms | cons copytime | tx method | zc_sends |
|------|--:|--:|--:|--:|--:|--:|:--|--:|
| SHM adopt | 2449 | 698 | **23** | 0 | 429 | **0** | – | 0 |
| SHM copy | 2461 | 704 | 23 | 0 | 429 | 11 | – | 0 |
| TCP epoll | 2458 | 706 | **47** | 0 | 414 | 18 | epoll | 0 |
| TCP zerocopy | 2467 | 709 | **70** | 0 | 417 | 24 | msg_zerocopy | 173 |
| TCP blocking | 2461 | 716 | 46 | 0 | 418 | 18 | mixed | 0 |
| Arrow | 2493 | 735 | 51 | 0 | 406 | 19 | epoll | 0 |

### TPC-H
| mode | prod READ | DEFORM | PUBLISH | STALL | cons ms | cons copytime | tx method | zc_sends/overlap |
|------|--:|--:|--:|--:|--:|--:|:--|--:|
| SHM adopt | 4441 | 1460 | **74** | **1487** | 1717 | **0** | – | 0 |
| SHM copy | 4487 | 1475 | 73 | 1356 | 1731 | 81 | – | 0 |
| TCP epoll | 4539 | 5100 | **301** | 0 | 1833 | 164 | epoll | 0 |
| TCP zerocopy | 4510 | 5100 | **463** | 0 | 1877 | 203 | msg_zerocopy | 966 / 834 |
| TCP blocking | 4572 | 5111 | 305 | 0 | 1841 | 162 | mixed | 0 |
| Arrow | 4549 | 5123 | 302 | 0 | 1853 | 164 | epoll | 0 |

**Why the transports are close:** READ + DEFORM (the producer scan/visibility/columnize) dominate and are
transport-agnostic. The transport-specific cost is just PUBLISH (SHM ring-memcpy ~23–74 ms vs TCP
serialize+send ~47–463 ms) + consumer copytime (adopt 0, copy ~11–81 ms memcpy, TCP ~18–204 ms socket-recv)
— tens-to-hundreds of ms against multi-second queries. SHM-adopt is cheapest end-to-end (zero-copy: PUBLISH
≈ memcpy-free on consumer, copytime 0). TCP-zerocopy has the *highest* producer PUBLISH (errqueue/RLIMIT
drain overhead) → marginally slowest on loopback, exactly as Phase-3 predicted. STALL is SHM-only (ring
backpressure — large on TPC-H heavy joins where the CH consumer is the bottleneck; ~0 on ClickBench).

## Honest caveats / exclusions

- **msg_zerocopy robustness gap (real):** stalls out (`Code 781 … no producer progress for 30000ms …
  SHM_PRODUCER_STALL`) on **4 ClickBench queries (Q23, Q24, Q38, Q40)** that every other mode completes.
  Its raw single-mode geomean (2.34×) is over a smaller set; the common-set numbers above are apples-to-apples
  and show it is in fact marginally the *slowest*, not fastest.
- **TPC-H Q8 = the one real result mismatch** (`DIFF(2)`, identical in all 6 modes) — the known
  Decimal/Decimal ratio precision issue, transport-agnostic, pre-existing. Excluded from the geomean.
- **9 ClickBench "⚠" queries are top-N tie nondeterminism** — all `ORDER BY … DESC LIMIT 10 [OFFSET N]`;
  tie-boundary rows differ identically across all 6 transports (so it varies on the baseline too, not a
  transport bug). Their timings are valid (and include big wins, e.g. Q33 ~14×), so the common-set geomean
  is conservative. (Q20/Q42 = both-empty, i.e. native and offload agree.)
- **Excluded from offload entirely (don't produce ClickHouseShmScan or stall):** TPC-H Q2, Q11 (HAVING
  param), Q15–18, Q20–22 (correlated subqueries / unsupported shapes). TPC-H native was NOT run on the
  memory-monster non-offloadable queries (they OOM'd PG at work_mem=2GB×8) — they are irrelevant to the
  mode comparison.
- **Box state:** measured on a host that had OOM-crashed + restarted earlier this session; absolute times
  run ~10–20% slower than a cold-clean box (e.g. Q6 adopt 1290→1589 ms), but native + all modes are measured
  adjacent per query with min, so the *ratios* (what's reported) are drift-controlled.
- Raw per-query data: `dev/wsweep-report/results/modematrix/{tpch,clickbench}/matrix.tsv` (35 cols/row).
