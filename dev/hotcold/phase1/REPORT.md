# Hot-Cold Phase 1 — TCP transport: REPORT

**Verdict: GREEN.** A TCP transport is selectable per query via
`streamed_table('name','schema','tcp:<host>:<port>')`; each producer worker listens on its own
ephemeral 127.0.0.1 port (reported back to PG, emitted per worker — one TCP connection per CH
stream), serializes each block's data-region bytes frame-relative, and the consumer
(`TcpStreamSource`) recvs them into an owned buffer and reconstructs columns through the **unchanged
`adopt()`**. It is correct (passes the oracles, byte-identical to SHM within the documented bounds),
tears down cleanly (no leaked workers/sockets/fds), and its overhead vs SHM is bounded and matches
the pre-registration: **within noise on most ClickBench cells (median +0.6% vs adopt), and ~+7.5%
on TPC-H** where lower per-cell parallelism (4 producers at W=8) exposes the send/recv transfer cost.

## 1. Environment / reproduction
- AWS Graviton aarch64, 32c/61GiB; PG18 :5432; CH reldeb v26.6.1.1 :21002 (rebuilt with the TCP
  consumer; restarted onto it, live pid from `ss`); shared cgroup `cpu.max=8*100000/100000` at W=8.
- All three transports (adopt, copy, tcp) re-measured FRESH on the SAME binary in the SAME session
  (the binary changed when `TcpStreamSource` landed). Commands/SHAs/sysctl: `10-REPRODUCTION.md`.
  Decisions: `../DECISIONS.md` (D-HC-0101..0104). Pre-registration: `00-PRE-REGISTRATION.md`.
- TCP socket buffers `SO_SNDBUF`/`SO_RCVBUF`=32 MiB (the TCP analog of the SHM K=4×16 MiB=64 MiB
  ring), kernel cap raised `net.core.{w,r}mem_max=64 MiB`. Applied identically to every TCP cell.

## 2. Correctness (GREEN; gated before timing)
- **`verify_offload.sh TRANSPORT=tcp`: 137/137 PASS** — scans, aggregates, **SEMI/ANTI joins with one
  TCP connection per joined relation** (distinct per-worker ports in the dispatched SQL), NULLs,
  bounded streaming, decimal, fail-closed on out-of-domain, and **leak teardown (no leaked /dev/shm,
  sockets, or streaming workers)** on success/error/cancel. Asserts `ShmCopiedBlocks≥1`.
- **gtest `TcpStreamSource.DrainsHandshakeAndBlocks`** — handshake + N blocks + EOS reconstruct
  identically via `adopt()`.
- **`verify_columnar.sh` 12/12 + `verify_visibility.sh` 39/39 in TCP mode** — the column-major
  deform + MVCC-visibility producer paths are correct over TCP (transport-independent, confirmed).
- **3-way result agreement** — every comparable cell's tcp result == copy == adopt (== native within
  the same F1/F3 decimal/avg bounds); the harness `cmp.py` verdict is identical across all three
  transports cell-for-cell (the excluded Q8/Q11/TIEBREAK cells are excluded in all three, pre-existing,
  not transport-induced). No new `DIFF`.

## 3. Per-mode partition audit (`evidence/tcp-mode-audit.txt`)
Whole-sweep `system.query_log`, partitioned by the transport arg + counters:

| is_tcp | is_copy | queries | copied_blocks | adopted_blocks |
|---:|---:|---:|---:|---:|
| 0 | 0 | 1232 | 0 | 471,190 |  (adopt) |
| 0 | 1 | 1250 | 489,480 | 0 |  (copy) |
| 1 | 0 |  597 | 221,051 | 0 |  (tcp) |

**Zero mixed** rows; every offload query partitions to exactly one transport. TCP genuinely ran (597
queries, 221k blocks, the `tcp:127.0.0.1:<port>` arg present, zero adopted). (TCP shares the
`ShmCopied*` counter family with copy — it is distinguished from copy by the `tcp:` SQL arg.)

## 4. Overhead: TCP vs SHM-adopt vs SHM-copy at W=8 (the headline AC)
Full table: `evidence/overhead-table.md`. Cell shown only if eligible+live+correct in ALL THREE modes.

| benchmark | n | tcp vs adopt (median / geomean / range) | tcp vs copy (median / geomean) | within-noise |
|---|--:|---|---|--:|
| ClickBench | 35 | **+0.6% / +1.4% / [−11.5%, +26.6%]** | +0.2% / +0.9% | ~30/35 |
| TPC-H | 11 | **+7.5% / +7.6% / [+1.9%, +13.1%]** | +7.6% / +6.9% | 1/11 |

- **ClickBench TCP ≈ SHM** (median +0.6% vs adopt, within noise on ~30/35). The transfer overlaps
  the 16-thread consumer; only the widest cell stands out: **Q24 (`SELECT *` 105 cols, ~8 GB
  transferred) +26.6% vs adopt / +16.9% vs copy**, with by far the largest consumer-CPU delta
  `dCons=+0.525 cores` — the transfer dominates when the bytes are huge.
- **TPC-H TCP ~+7.5% vs SHM**, most cells out of the 5% band. The difference vs ClickBench is
  parallelism: TPC-H at W=8 runs **4 producers / `max_threads=4`** (OFF_PROD_DIV=2) vs ClickBench's
  8 producers / `max_threads=16`, so there are fewer consumer threads to overlap the per-block
  send/recv behind, and the transfer cost surfaces on the critical path.
- **Ordering `adopt ≤ copy ≤ tcp`** holds in aggregate (each step adds transfer work); all bounded,
  every offloaded query completes, and the offload wins vs native are preserved (e.g. ClickBench Q17
  8.44→8.07, Q9 4.66→4.68; TPC-H Q14 7.73→7.15).
- (Note: the auto-table's `tcp_recvms` column reads the producer `st_cpu`/PUBLISH_STALL phase, which
  is 0 for TCP by construction — TCP has no ring-full stall; backpressure is blocking `send()` folded
  into the PUBLISH phase. The real transfer cost is `dCons` + the wall overhead.)

## 5. In-query TCP transfer rate (T2) — `evidence/tcp-recvrate.tsv`
Consumer recv cost per query (W=8, `parallel_workers=8`, idle host): `ShmCopyTimeMicroseconds` is the
`TcpStreamSource` payload-recv time; rate = `ShmCopiedBytesLogical / recv_time`.

| q | bytes recv | recv time | rate |
|--:|--:|--:|--:|
| Q1  | 3.12 GB | 490 ms | **6.37 GB/s** |
| Q6  | 1.56 GB | 241 ms | **6.46 GB/s** |
| Q19 | 3.70 GB | 534 ms | **6.92 GB/s** |

In-query loopback recv ≈ **6.4–6.9 GB/s**, ~3× slower per byte than SHM-copy's in-query ~21 GB/s
(Phase 0) — the cost of TCP's extra kernel copy + per-block syscalls vs SHM-copy's single userspace
memcpy.

## 6. Loopback microbench (T2 anchor) — gtest `TcpStreamSource.LoopbackThroughputMicrobench`
Isolated 127.0.0.1 TCP, 1 GiB in 2 MiB chunks with the transport's 32 MiB socket buffers, no
scan/adopt/consumer: **7.90 GB/s, 0.127 ns/byte.** In the pre-registered 3–10 GB/s envelope. **C2/T2
convergence:** isolated 7.9 GB/s vs in-query 6.4–6.9 GB/s — agree in mechanism and magnitude (the
~1.2× gap is concurrent memory-bandwidth contention, same as Phase-0 C2). Versus the SHM-copy cold
microbench (29.7 GB/s), loopback TCP is ~3.7× slower per byte — two kernel copies + syscalls, as
predicted.

## 7. Mechanism — PMU + profile (Q1, CH consumer, copy vs tcp) — `evidence/perf-{stat,prof}-*.txt`
**Profile (decisive).** The TCP recv kernel path is a clear hotspot in the **tcp** consumer profile
and absent in **copy**:
```
tcp:   6.63% __arch_copy_to_user
              ← simple_copy_to_iter ← __skb_datagram_iter ← skb_copy_datagram_iter
              ← tcp_recvmsg_locked ← tcp_recvmsg ← inet_recvmsg ← sock_recvmsg
              ← TcpStreamSource::recvAll ← recvBlock                 (the kernel skb→user copy)
copy:  convertToFullColumnIfAdopted→cloneResized stacks + memcpy@plt 0.71%   (userspace memcpy)
       (no tcp_recvmsg data-path frames; only incidental tcp_ack/tcp_sack @0.01% from the HTTP/metrics
        sockets)
```
So the predicted transport mechanism is confirmed: TCP replaces the SHM/copy userspace memcpy with a
kernel network-stack copy (`tcp_recvmsg → skb_copy_datagram_iter → __arch_copy_to_user`) driven by
`TcpStreamSource::recvBlock`.

**PMU (8× Q1, whole CH process):**

| metric | copy | tcp | tcp/copy |
|---|--:|--:|--:|
| cycles | 52.59 B | 53.47 B | +1.7% |
| instructions | 200.75 B | 204.32 B | +1.8% |
| cache-misses | 717.7 M | 746.9 M | **+4.1%** |
| context-switches | 46,395 | 43,791 | flat |
| wall (8 iters) | 6.57 s | 7.05 s | **+7.3%** |

TCP adds memory traffic (+4.1% cache-misses — the kernel skb copy) and ~7% wall on this TPC-H cell;
context-switches are flat (bulk recvs over the 32 MiB buffers, not a per-byte syscall storm) — so the
cost is the extra kernel COPY (bandwidth), not syscall overhead, as pre-registered. (Caveat, as
Phase 0 §6: `perf stat -p <CHPID>` is whole-process / not background-subtracted; the call-graph
profile is the unambiguous mechanism evidence.)

## 8. Prediction vs observation (vs `00-PRE-REGISTRATION.md`)
- Predicted: `adopt ≤ copy ≤ tcp`; TCP adds the transfer cost; single-digit-to-low-double-digit % on
  compute-bound, tens of % on wide/high-byte cells; loopback memory-bandwidth + syscall bound.
  **Observed:** ordering holds; ClickBench median +0.6% (transfer hidden by 16 threads), TPC-H +7.5%
  (4 threads expose it), Q24 (8 GB) +26.6% — squarely in the predicted envelope.

## 9. Limitations (honest)
- **Single-stream / `max_threads=1` serialization.** `TcpStreamSource` is a *blocking* leaf source: a
  query with a single TCP stream and one consumer thread serializes recv with downstream processing
  (no overlap), which on a 60M-row scan measured ~26 s vs ~0.9 s with W producers. This is NOT the
  operating regime — the harness always streams W (or W/2) producers per relation, so there are
  multiple sources/threads and the transfer overlaps. The principled fix is an async `TcpStreamSource`
  (non-blocking socket + `schedule()`/`Async` + partial-frame buffering, like `PollableShmSource`);
  deferred as an optimization (correctness + the measured multi-stream performance do not require it).
  Liveness rests on the invariant **`max_threads = Σ producers ≥ #blocking TCP sources`** — confirmed
  deadlock-free by the review, and now pinned with a DEADLOCK-SAFETY comment at the `max_threads`
  forcing in `shm_customscan.c::shm_build_offload_settings`.
- TPC-H's +7.5% reflects the W/2-producer config, not a TCP defect; with ClickBench's W-producer/
  16-thread config the same transport is within noise.

## 10. Independent adversarial review — PASS
`evidence/ADVERSARIAL-REVIEW.md`. A fresh reviewer independently reran `verify_offload TRANSPORT=tcp`
(137/137), both gtests (incl. the 7.9 GB/s microbench), and re-derived every overhead number exactly;
verified the framing byte-for-byte against the known-good SHM layout, the `tcp_recvmsg` profile path,
the clean counter partition, and the absence of any connect/teardown deadlock. PASS on all 7 angles;
the one non-blocking item (the liveness invariant) was actioned (code comment).
