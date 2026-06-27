# P2 Adversarial Review — Merged Verdict (Lead Reviewer)

Branch `streamed-table-shm-offload` @ `a765218` (reactor) + `e4cd7c0` (two-cursor + frame/zerocopy fixes + instruments). D-HC-0303.
Merges four independent P2 adversarial reviews: CORRECTNESS (two-cursor reactor), FIDELITY, PERFORMANCE & EVIDENCE HONESTY, HOLISM (W=8 multi-stream).
Lead independently re-verified the contested claims against source (`src/shm_producer.c`, `src/shm_offload.c`), the committed evidence tree, the raw `results/*/walls.tsv`, `PROMPT.md`, and `METHODOLOGY-LOG.md`.

## Overall verdict: PASS (GREEN)

**Rationale.** P2 is GREEN: there are **no unresolved blocking findings** across any of the four angles. All four reviewers returned PASS or CONCERNS with **empty `blocking_findings`**; every CONCERNS item is a non-blocking follow-up, a documentation gap, or a defensible methodology caveat — none is a correctness defect, a silent DIFF, a UAF, a hang, or a fabricated result.

The two questions the lead was told to scrutinize both resolve in P2's favor:

1. **Is the injected-latency microbench a FAIR overlap proof, or a circular artifact of `tcp_send_delay_us`?** It is FAIR and non-circular. The delay gates frame **RECLAIM** (`reactor_reclaim`, `shm_producer.c:1294`; `ready_at_ns` set on send completion at `:1252`), **not** SEND — a faithful model of MSG_ZEROCOPY completion latency (the buffer is pinned/unreusable until completion). The delay GUC is applied identically to every K in the sweep (`p2_kbench.sh` sets the same `tcp_send_delay_us` for all KV), so it does not bias K=1. The non-tautology is independently anchored three ways, all reproduced by the lead from the raw walls: (a) the knee **scales with latency** — 20 ms → knee K=4, 40 ms → knee K=8 (`results/p2_kbench_injdelay20ms2/walls.tsv`, `…40ms/walls.tsv`), implying a constant ~5 ms per-block work; (b) a `delay=500us` run (below per-block work) is **FLAT at ~944 ms for all K** (`…injdelay500/walls.tsv`, K1..K8 = 928–958), and that floor equals the no-delay baseline — a pure knob-tautology would not converge on a work floor; (c) K=8/40 ms recovers to 952 ms ≈ that same floor. Walls are tight and monotonic (40 ms: K1≈4855, K2≈2518, K4≈1360, K8≈952), not a noisy tail. **It is a genuine BDP-pipelining signature, not a dressed-up knob.**

2. **Is the loopback null honest?** Yes. The bare-loopback K-sweep is a **pre-registered NULL** (`p2-ksweep-bare.txt`: "0faster/0slower" at every K beyond noise; L0024/L0025). The netem-epoll null is honestly reported and root-caused (copying `send` frees the frame on `send()`, redundant with kernel SO_SNDBUF on a single-threaded producer; L0027). The netem-**zerocopy** curve on the FIXED reactor is **flat** (`p2-kbench-netem-zc-fixed.txt`: K1=1911, K2 −0.4 %, K4 −0.1 %), and that flatness is honestly published, not suppressed. The implementer's self-correction history (a false R=2 netem "K≈2×" win caught and retracted at R=3 — `p2-ksweep-netem3g-r2.txt` vs `…netem3g.txt`, L0027; a pre-fix flat injdelay20ms kept as evidence of the single-cursor bug, L0028) is genuine adversarial honesty, not result-shopping.

The one item that could read as a hard FAIL — `PROMPT.md:220-221` "A flat K-curve for `msg_zerocopy` is a FAIL" against the flat netem-zc curve — is **resolved, not blocking**, because the same PROMPT explicitly sanctions the alternative: `PROMPT.md:163-164` ("a microbench that injects a fixed per-send delay is an **acceptable complementary instrument**") and `PROMPT.md:399-400` ("under a netem-emulated NIC regime **and/or** a fixed-send-delay microbench, the pipeline shows a throughput recovery"). The flat netem-zc curve is provably **not** a hidden synchronous-drain bug: the *same fixed reactor* demonstrably pipelines under injected delay, so the flatness is **host-intrinsic** (loopback MSG_ZEROCOPY is a fast deferred copy — `SO_EE_CODE_ZEROCOPY_COPIED`, `shm_producer.c:636,673` — whose completion is not netem-ACK-gated), exactly as `PROMPT.md:150-164` pre-registers. All four reviewers independently reached this conclusion and rated H4/H6/H7 SATISFIED. The literal-pin gap (no direct K>1-raises-msg_zerocopy-throughput measurement) is recorded as **non-blocking follow-up NB-1**.

## Blocking findings

**None.** No reviewer reported a blocking finding. The lead concurs after independent verification.

## Hardening pins

| Pin | Status | Basis (file:line) |
|-----|--------|-------------------|
| **H1** — snapshot serialize synchronous-in-publish; no `cz->bufs` aliasing into in-flight frame | SATISFIED | `tcp_serialize_block` memcpys payloads (which alias `cz->bufs`) into frame-owned `f->scratch` synchronously before `tcp_publish_block` returns (`shm_producer.c:1056/1066/1079`, called at `:1476`); `cz->bufs` reset afterward (`shm_offload.c:594-599`). iovec points at `f->scratch` (`:1488`), never `cz->bufs`. K-deep double-buffering is at the FRAME level (`tcp_reactor_init`, `:1185-1189`), not the columnizer. No torn snapshot / UAF. |
| **H2** — bounded waits, no hot-spin | SATISFIED | All three blocking primitives bounded + re-check `CHECK_FOR_INTERRUPTS`/`origin_backend_dead` each slice: `tcp_wait_writable` epoll/poll 100 ms (`:569-588`), `reactor_wait_delay` 5 ms nanosleep slices (`:1349-1368`), `tcp_zc_drain_until` 100 ms poll (`:690-706`). `reactor_pump` reclaims FIRST (`:1336`) so the drain loop cannot busy-spin; the K=1-zerocopy poll-spin (no O_NONBLOCK/epoll-fd) is fixed for the zc method too (`:885-886`, commit `e4cd7c0`). |
| **H4** — async MSG_ZEROCOPY completion deferred off the steady send path | SATISFIED (with NB-1 caveat) | `tcp_zc_drain_until` on the reactor appears ONLY at `reactor_acquire` (`:1395`, blocking on THIS head frame) and `reactor_drain_all` (`:1431`); `reactor_send_one`/`reactor_pump` call only non-blocking `tcp_zc_reap`. Frames tagged `zc_seq` (`:1238`), reclaimed only when acked (`:1287-1293`). The old synchronous per-send drain (`:790`) is OFF the data path (handshake/schema/Arrow only). RLIMIT_MEMLOCK cap uses the tight `tcp_frame_capacity` (`:1166-1180`). **Caveat (→ NB-1):** the literal acceptance "msg_zerocopy K-sweep shows K>1 raising throughput; flat = FAIL" is met by structural analogy (epoll-method injected-delay proof + host-intrinsic root-cause), not directly on the zerocopy reclaim path; the flat netem-zc curve is honestly reported, not a deferral bug. |
| **H5** — EOS enqueued after all data frames; drain all + reap before `eos_published` | SATISFIED (test gap → NB-2) | `shm_producer_signal_eos` → `tcp_publish_block(is_eos=true)`: EOS frame placed strictly after occupied data frames (FIFO), then `reactor_drain_all` (`:1505`) pumps until `send_count==0` (all frames+EOS sent AND, for zc, reaped) BEFORE `eos_published=true` (`:2036`). FIFO byte order guarantees EOS bytes hit the wire last. Bounded + backend-death-checked. Validated end-to-end (137/137); the PROMPT-mandated isolated multi-block-then-EOS gtest (`PROMPT.md:230`) is MISSING → NB-2. |
| **H6** — K-deep frame pool + per-frame zc_seq + lazy FIFO reclaim | SATISFIED (with NB-1 caveat) | Same async-completion mechanism as H4 (bundled in PROMPT). K × tight-frame bounded under 8 MiB RLIMIT_MEMLOCK on the zc path (`:1166-1180`). Correctness validated by verify_offload TCP 137/137 + K=2/8 zerocopy = native, no hang/UAF. K>1 zerocopy THROUGHPUT win is unmeasured-on-loopback (host-intrinsic null) — see NB-1. |
| **H7** — teardown/UAF of zerocopy-pinned frames | SATISFIED | `producer_cleanup` (`:460`) closes `tcp_conn_fd` (`:499`, releasing kernel zc page pins) and runs as a `MemoryContextRegisterResetCallback` on `owner_cxt` (`:1594-1596`), which PG invokes BEFORE freeing the owner_cxt-allocated frame buffers (`:1185-1188`) — on both normal-EOS (drain reaps first) and abnormal/cancel (close drops pins) paths. No UAF. Reactor waits all bounded (see H2). |
| **H8** — new-reactor teardown: no leaked fd, no UAF, socket closed before frames freed | SATISFIED | Verified against the PG allocator: `MemoryContextDeleteOnly` calls reset callbacks (`mcxt.c:518`) BEFORE `delete_context` (`mcxt.c:534`); `MemoryContextReset` likewise at `mcxt.c:415`. `producer_cleanup` closes `tcp_send_epoll_fd` (`:493`) and `tcp_conn_fd` (`:499`) before owner_cxt frees pooled frames. Frames never explicitly pfree'd (no double-free); arrow_enc buffers explicitly destroyed (`:497`); `cleaned` flag idempotent (`:464-466`). Residual kernel skb-pin-survives-close is kernel-inherent and mitigated by exactly this close-before-free ordering. |
| **H9** — coalesced 2-iovec sendmsg + partial-send iovec advance | SATISFIED (test gap → NB-2) | Data frames issue ONE sendmsg of a 2-iovec {bh, payload} (`:1232`, n_iov at `:1484-1493`); handshake/schema/EOS stay single sends. Partial-send resumes by skipping `f->sent` across the original iovec WITHOUT mutating it (`:1209-1221`); lead concurs with the per-boundary trace (mid-header → into iov[0]; boundary → iov[0] consumed; mid-payload → iov[1] offset). The PROMPT-mandated forced-1-byte unit test of all three boundaries (`PROMPT.md:238-244`) is MISSING → NB-2; the RESUME path (`f->sent>0`) is essentially unexercised by committed evidence because loopback sendmsg accepts the whole frame in one call. |
| **H14** — K=1 ≡ P1 single-frame-in-flight semantics, no special case | SATISFIED | No K==1 branch: same coalesced sendmsg + non-blocking publish. At K=1 the pool is one frame; the next `reactor_acquire` blocks in `while (send_count >= send_k)` (`:1386`) until it drains → single-in-flight. `PROMPT.md:279-281` defines H14 as single-in-flight SEMANTICS/parity, not literal P1 blocking, so K=1 deform-of-N+1 overlapping a partially-sent frame N is in-spec. Corroborated by bare K-sweep K1==native and injdelay K1 as the slow baseline. |

## Non-blocking follow-ups

- **NB-1 (perf/evidence — central CONCERNS item).** The binding overlap proof, the injected-delay microbench (`evidence/p2-injdelay-knee.txt`; raw `results/p2_kbench_injdelay{20ms2,40ms,500}/walls.tsv`), was run with **METHOD=epoll** (confirmed first line of `results/p2_injdelay{20ms2,40ms,500}.log`), not `METHOD=msg_zerocopy`. It exercises the method-agnostic two-cursor send-ahead + delay-gated reclaim, validating the zerocopy path's pipelining STRUCTURE by analogy (the zc reclaim gate `:1287-1293` and the delay gate `:1294` are parallel break conditions sharing `reactor_send_pos`). There is NO measurement of K>1 directly raising msg_zerocopy throughput; the literal `PROMPT.md:220-221` flat-curve-is-FAIL is met only by structural analogy + the host-intrinsic rebuttal. **Not blocking** (PROMPT sanctions the microbench as a complementary instrument; flatness is provably not a deferral bug), but a real rate-limited-NIC zerocopy K-sweep is the missing direct confirmation, and `REPORT-P2.md:30,70-74` should add a one-line disclosure that the microbench is epoll-method so a reader does not infer the zerocopy reclaim path was measured directly under latency.

- **NB-2 (correctness/fidelity — missing mandated isolated tests).** Two PROMPT-mandated gtests do not exist (confirmed: `grep` over `test/` finds zero `reactor_send_one`/`SendFrame`/partial-send references): (a) the forced-1-byte partial-send test at all three boundaries (`PROMPT.md:238-244`) — the one path the PROMPT singled out is the least-covered; (b) the isolated multi-block-then-EOS H5 gtest (`PROMPT.md:230`). Both logic paths are correct by inspection and validated end-to-end (137/137), but the isolated forced-condition coverage the PROMPT required is absent.

- **NB-3 (evidence completeness — no committed P2 parity artifact).** Unlike C1 (`evidence/c1-parity-table.txt`) and P1 (`evidence/p1-interleave-parity.txt`), there is **no committed P2 verify_offload output or parity DIFF file** — every committed P2 artifact is throughput (k-bench walls, injdelay, overlap counters). The "verify_offload TRANSPORT=tcp 137/137" and "K=1/2/8 = native (count+sum)" claims (`REPORT-P2.md:21-23`, commit messages) appear only in prose; the byte-stream-integrity claim is currently unfalsifiable from committed evidence. Recommend committing the P2 verify_offload / parity output to match the C1/P1 evidence bar.

- **NB-4 (holism — uncapped String-schema frame-pool memory on the DEFAULT path).** `tcp_frame_capacity` falls back to `tcp_scratch_cap` = the full 64 MiB data region for any String column (`shm_producer.c:1146-1147`). The RLIMIT_MEMLOCK 8 MiB cap in `tcp_reactor_init` is guarded by `if (p->tcp_send_method == PGCH_TCP_SEND_MSG_ZEROCOPY …)` (`:1167`) — it does NOT apply on the DEFAULT epoll path (`pgch_tcp_send_method = PGCH_TCP_SEND_EPOLL`, `shm_offload.c:57`), and K clamps to [1,64] (`shm_offload.c:1202`). So a String-schema query on the default transport allocates **K × 64 MiB per stream uncapped**: at W=8 / default K=2 ≈ 1 GiB; K=8 ≈ 4 GiB; K=64 ≈ 32 GiB. Un-budgeted (no work_mem-style accounting, no W-aware cap) and not recorded in `REPORT-P2.md`; the closing "Default K=2 = cheap real-NIC insurance at ~0 loopback cost" (`REPORT-P2.md:86`) omits the memory cost. Recommend applying the frame-cap budget to the epoll path too and recording the per-stream / W-aggregate footprint. **Non-blocking** (no correctness impact; bounded by K clamp; default K=2 keeps it ~1 GiB at W=8).

- **NB-5 (holism — dead 64 MiB allocation).** `tcp_producer_setup` still palloc's a 64 MiB single `tcp_scratch` (`shm_producer.c:1540`) on every TCP and Arrow stream, but P2 no longer reads it as a serialize buffer — `tcp_serialize_block` writes into the per-frame `f->scratch` (`:1476`), and only `tcp_scratch_cap` is consulted (for `tcp_frame_capacity`). Verified: remaining `tcp_scratch` references are the alloc (`:1540`), the cap field, and comments (`:207,1030,1129-1130,1147,1152,1567`). That is 64 MiB/stream of waste (512 MiB at W=8) on top of the frame pool. Newly-dead for TCP in P2 (was the live serialize buffer pre-P2); already dead for Arrow. Recommend dropping the `tcp_scratch` alloc for socket transports. Freed by context reset (correctness unaffected).

- **NB-6 (correctness/fidelity — latent frame-sizing coupling, fails-closed).** `tcp_frame_capacity` hard-codes `rows_pb = PGCH_SHM_ROWS_PER_BLOCK` (65536, `:1137`) while the columnizer's `rows_per_block` is a runtime parameter (clamped ≤ 1<<20, `shm_offload.c:529-530`). Benign today because the only TCP-path driver always passes `PGCH_SHM_ROWS_PER_BLOCK` (`shm_worker.c:569,648`) and the user-tunable `rows_per_block` path (`clickhouse_stream_relation`) creates an SHM-transport producer (`shm_offload.c:1111`), never the tight-frame TCP path. If a future change routed a larger `rows_per_block` into a TCP producer the tight all-fixed frame would be undersized — but it **fails closed**: `tcp_serialize_block`'s bounds checks (`if (cursor > cap) goto overflow`, `:1055/1060/1065/1078`) raise `ereport(ERROR)` (`:1086`) before any OOB write — a hard abort, never a silent DIFF or corruption. Recommend deriving the frame's row basis from the actual `rows_per_block` or adding a static-assert/comment. Same fail-closed coupling underlies the String-path `tcp_scratch_cap == slot_cap` invariant (`columnizer_should_flush_bytes`, `shm_offload.c:609-632`); document the dependency.

- **NB-7 (evidence hygiene — superseded file not marked in-file).** The retracted R=2 netem false-win (`p2-ksweep-netem3g-r2.txt`: Q9 −55 %, Q6 −53 % with K=1 ranges spanning [1578,5304]) sits next to the corrected R=3 NULL (`p2-ksweep-netem3g.txt`). L0027 honestly labels R=2 an artifact and supersedes it, but the stale file lacks an in-file "SUPERSEDED" marker; a future reader scanning `evidence/` could mistake it for a real win. Add a header.

- **NB-8 (perf — best-case framing).** The injected-delay microbench imposes no byte-rate floor; K hides 100 % of per-frame latency down to the pure-deform floor (~944 ms). On a real rate-limited NIC, K cannot hide latency below the serialization-rate floor, so the headline 2.7×/5.1× are **best-case (deform-floored) upper bounds**, not expected real-NIC speedups. `REPORT-P2.md` frames these as a "real-NIC expectation"/mechanism demo (defensible), but should qualify 2.7×/5.1× as an upper bound.

- **NB-9 (perf — uncaptured decisive datum).** The "loopback zerocopy is a fast deferred copy" root-cause (the linchpin of the host-intrinsic rebuttal) is asserted from the code comment (`SO_EE_CODE_ZEROCOPY_COPIED`, `shm_producer.c:636,673`) and a prior phase-2 measurement; the actual server-log `zc_copied>0`/`method=msg_zerocopy` accounting for THIS fixed-reactor netem-zc run is not in committed evidence (`results/p2_netem10gd500zc_fixed.log` holds only client walls + qdisc params). Plausible and code-consistent, but capture the decisive `zc_copied>0` datum for this run.

- **NB-10 (minor — stale `zc_seq` / handshake-time drain exceptions).** `reactor_acquire` does not reset `f->zc_seq` on fill (`:1407-1414`), but the pin check is gated on `f->zc_pending` (reset at `:1409`) and `reactor_send_one` re-assigns `zc_seq` before setting `zc_pending` (`:1238-1239`), so stale `zc_seq` is never consulted — comment only. Separately, handshake+schema sends still route through the synchronous `tcp_send_all_msg_zerocopy → tcp_zc_drain_until` (`:790`), a one-time benign exception to the steady-path drain-removal claim — noting only, no correctness issue.

## Per-angle summary

- **CORRECTNESS of the two-cursor reactor — CONCERNS (non-blocking only).** Byte-stream order, partial-send advance, K=1≡P1, EOS drain, and reclaim all verified correct by source trace; pins H9/H14/H5 SATISFIED. Concerns are all coverage/evidence gaps, not defects: missing forced partial-send gtest (NB-2), missing isolated H5 gtest (NB-2), no committed P2 parity artifact (NB-3), dead `tcp_scratch` alloc (NB-5), and the macro-vs-runtime `rows_per_block` coupling that fails-closed (NB-6). Byte-stream interleaving is shown impossible (`reactor_send_pos` returns earliest-unsent FIFO, single sendmsg at a time); reclaim never frees a mid-send/zc-pinned/within-delay frame and is signed-wrap-safe (`:1290`).

- **FIDELITY — PASS.** H1 verified from source: serialize is synchronous-in-publish into frame-owned `f->scratch`; the async reactor reads only `f->scratch`, never `cz->bufs`; no aliasing/UAF/torn snapshot. Frame cap is term-by-term ≥ serialize footprint for all-fixed schemas, and for String schemas the cap equals the columnizer's byte-flush bound (byte-identical layout to `shm_producer_block_footprint`), so serialize stays under cap; any divergence fails closed via the overflow `ereport(ERROR)`. Evidence honesty affirmed (bare null pre-registered; overlap win only via the sanctioned microbench). Findings are latent/observational (NB-6, NB-10).

- **PERFORMANCE & EVIDENCE HONESTY — CONCERNS (non-blocking only).** The injected-delay microbench is FAIR and non-circular (delay gates reclaim not send; applied identically per-K; knee scales with latency; 500 µs floor and 40 ms-K8 recovery both anchor on the no-delay baseline — lead reproduced all from raw walls). Bare and netem nulls honestly reported and root-caused; self-correction (retracted R=2 win, kept pre-fix flat run) is genuine. The central concern is the literal-pin-vs-rebuttal gap: the binding proof is epoll-method and the netem-zc curve is flat (NB-1); plus framing (NB-8) and uncaptured `zc_copied` (NB-9) and the superseded R=2 file (NB-7). H4/H6/H7 SATISFIED.

- **HOLISM (W=8 multi-stream) — CONCERNS (non-blocking only).** H8 (cleanup order, no leaked fd/UAF) and H2 (bounded waits, no hot-spin) verified against the PG allocator semantics. The W=8 dataflow is actually exercised (`p2_kbench.sh` runs `max_parallel_workers_per_gather=8`; the 2.7×/5.1× is the aggregate parallel-gather wall; `overlap_frames`=228/worker across all workers), so the reactor helps the whole dataflow under injected latency and is within-noise-neutral on bare/netem loopback. Main finding is the uncapped String-schema frame-pool memory on the default epoll path (NB-4) plus the dead 64 MiB alloc (NB-5) and the best-case microbench framing (NB-8).

---
**Conclusion.** Four independent angles, zero blocking findings, the two scrutiny questions (microbench fairness, loopback-null honesty) both resolved in P2's favor, and the apparent flat-curve FAIL resolved by the PROMPT's complementary-instrument clause plus a verified host-intrinsic control. **P2 is GREEN.** Ten non-blocking follow-ups (NB-1 missing direct zerocopy K-sweep + epoll-method disclosure; NB-2 missing mandated gtests; NB-3 missing committed parity artifact; NB-4 uncapped String memory on epoll path; NB-5 dead alloc) should be tracked for the hardening pass.

---

## Resolution (post-review) — 2026-06-27

The review returned **PASS (GREEN), zero blocking findings**; P2 stands as GREEN. The non-blocking
follow-ups were addressed as follows (mirrors the P1 resolution pattern). Code change (NB-4) is
correctness-green on the same binary that carries it; the rest are measurement/evidence/doc.

- **NB-1 — RESOLVED with a direct measurement.** Ran the injected-per-frame-latency microbench with
  **`METHOD=msg_zerocopy`** (was epoll), 20 ms/frame, `tpch99` transfer-bound, ROUNDS=2 × N=3 (n=6/level),
  on the live C1 consumer (`evidence/p2-injdelay-knee.txt`; raw `results/p2_kbench_injdelay20ms_zc/walls.tsv`).
  Result: **K=1 = 2587 ms → K=2 = 1400 ms (1.85×, −45.9 %) → K=4 = 1025 ms (2.52×, −60.4 %)** — tight
  (per-level spread < 1 %: K1 2582–2595, K2 1398–1407, K4 1019–1031), monotonic. This is a **direct
  K>1-raises-msg_zerocopy-throughput measurement on the zerocopy reclaim path**, closing the literal
  `PROMPT.md:220-221` pin (a flat zerocopy K-curve would be a FAIL; this is the opposite). The earlier
  epoll-method proof + host-intrinsic rebuttal already made H4/H6/H7 SATISFIED; this removes the "structural
  analogy only" caveat. (The loopback *netem*-zerocopy curve stays flat — host-intrinsic deferred copy, as
  pre-registered; the **injected-latency** instrument is what models real-NIC completion gating, and on it
  the zerocopy path pipelines exactly as predicted.)

- **NB-2 — addressed end-to-end + documented limitation.** The producer is a PostgreSQL **bgworker**, not
  a unit under the ClickHouse gtest harness — there is no PG-side gtest framework here, so the two PROMPT
  gtests (forced-1-byte partial-send boundaries; isolated multi-block-then-EOS) cannot be authored as
  isolated gtests without a new harness. Instead both forced conditions are now exercised **end-to-end on
  real 60M-row data** and the byte-stream result verified against native: a **forced-partial-send** run with
  `tcp_sndbuf_bytes=4096` fragments every ≥64 KiB data frame into ~16+ `sendmsg` calls — driving the
  `f->sent>0` RESUME path the reviewer flagged as "essentially unexercised" — at K=2 for **both** epoll and
  msg_zerocopy; both `count(*)||sum(l_orderkey)` == native (`59986052|1799465265420123`, MATCH;
  `evidence/p2-correctness.txt`). The EOS-drain (H5) path runs on every one of these full streams
  (`reactor_drain_all` before `eos_published`). This is a **stronger** correctness proof than the isolated
  gtests (real data + forced fragmentation + multi-stream W=8) but does NOT satisfy the PROMPT's literal
  "isolated gtest" wording — recorded as an honest residual limitation for the hardening pass.

- **NB-3 — RESOLVED.** Committed `evidence/p2-correctness.txt`: `verify_offload TRANSPORT=tcp` 137/137 +
  `TRANSPORT=arrow` 137/137 (no DIFF, clean teardown) + the K=1/2/8 × {epoll,msg_zerocopy} == native
  60M-row checks + the NB-2 forced-partial-send checks. The byte-stream-integrity claim is now falsifiable
  from committed evidence, matching the C1/P1 bar.

- **NB-4 — RESOLVED (code, committed `fa7e455`).** `tcp_reactor_init` now caps the per-stream frame pool to
  **128 MiB for ALL methods** (was: 8 MiB RLIMIT cap on the zerocopy branch only; the default epoll path was
  uncapped for String schemas — up to ~32 GiB at K=64). K is clamped accordingly and logged. All-fixed
  schemas (tight ~MB frames) are unaffected at any reasonable K; the zerocopy 8 MiB RLIMIT stays the tighter
  bound on that path. verify_offload 137/137 + K-native still pass on the capped binary.

- **NB-7 — RESOLVED.** Added an in-file `SUPERSEDED` header to `evidence/p2-ksweep-netem3g-r2.txt` (the
  retracted R=2 false-win), pointing at the corrected R=3 NULL.

- **NB-8 — RESOLVED (doc).** `REPORT-P2.md` now qualifies the 2.7×/5.1× (and the new 1.85×/2.52×) as
  **deform-floored best-case upper bounds** (the microbench imposes no byte-rate floor; a real rate-limited
  NIC cannot hide latency below the serialization floor), not expected real-NIC speedups.

- **NB-5 (dead 64 MiB `tcp_scratch` alloc), NB-6 (macro-vs-runtime `rows_per_block` coupling — fails closed
  via the overflow `ereport(ERROR)`), NB-9 (capture `zc_copied>0` for the netem-zc run), NB-10 (stale
  `zc_seq` / handshake-drain comments)** — tracked for the hardening pass. None is a correctness defect
  (NB-5/10 are waste/comments; NB-6 fails closed; NB-9 is a corroborating datum for an already-root-caused
  null). Not addressed in this resolution to keep the closing patch minimal and correctness-green.
