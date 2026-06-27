# Hot-Cold Phase 3 — Auditable methodology log

Append-only. Continues the **global L-number sequence** (phase2 ended at L0017 → phase3 starts L0018).
Template per entry: WHAT / HOW / VERIFIED / RESULT (raw) / INTERPRETATION / LEARNINGS / DONE-or-CONTINUE.
Holistic end-to-end note precedes each code-changing iteration (PROMPT process rule 3).

---

## L0018 — Phase 3 kickoff: verify-the-spec understanding pass (no code change)
**WHAT.** Before touching code, verified every PROMPT code claim across 6 subsystems via a parallel
deep-read workflow (consumer TcpStreamSource, executor async contract, producer shm_producer.c,
build/enum/worker, publish-path+arrow-encoder, tests/harness).
**HOW.** Workflow `phase3-understand` (6 Explore agents, structured schema, 363K tokens). Cross-checked
the consumer + executor findings against my own full read of `TcpStreamSource.{h,cpp}` and
`gtest_tcp_stream_source.cpp`.
**VERIFIED / RESULT (raw, the load-bearing ones).**
- Consumer: every C1 to-delete member/function confirmed at the exact lines the PROMPT cited
  (`ready_event_fd` .h:174 created .cpp:512; bridge thread spawned .cpp:1327; `onCancel` 1276-1286;
  `tryGenerate` early drain 1178-1183; `schedule` 1259-1263; `onAsyncJobReady` 1266-1273).
- Executor contract (load-bearing for C1 safety): `scheduleForEvent()` default returns
  `{schedule(), EPOLLIN|EPOLLERR}` (IProcessor.cpp:67-70); `Epoll::add` level-triggered, no EPOLLET
  (Epoll.cpp:39-42); **single-threaded `PullingPipelineExecutor` waits `async_task_queue.wait(timeout=-1)`
  and NEVER calls `onAsyncJobReady`** ⇒ the timerfd is the SOLE stall-budget wakeup in the infinite-wait
  path (not hygiene). Multi-threaded path's `processAsyncTasks` calls `onAsyncJobReady`.
- Producer: `tcp_send_all` dispatch @827 → msg_zerocopy(830)/iouring(836)/blocking(843); io_uring
  `IORING_OP_SEND` copying @611, submit@612, wait_cqe_timeout 100ms@623; ring init @569
  PGCH_TCP_IOURING_ENTRIES=8@257; `tcp_iouring_ensure`@560; `producer_cleanup` io_uring_queue_exit before
  close @480-495. Two sends/block: tcp_publish_block header@1110 payload@1112; arrow meta@1003 body@1005.
  `tcp_serialize_block`@1017 memcpys into single `tcp_scratch`@216/alloc@1147. msg_zerocopy:
  `tcp_zc_drain_until`@719 blocks at end of every publish @819; PGCH_ZC_SEND_CHUNK 1MiB@739.
- Build/enum: Makefile liburing detect @49-51 (-DPGCH_USE_LIBURING + -luring); enum shm_offload.h:82-91
  {BLOCKING=0,IOURING=1,MSG_ZEROCOPY=2}; default IOURING shm_offload.c:57; config_enum_entry @70-76; GUC
  reg @1178-1190; shm_producer.h set_tcp_send_method@150-157, tcp_send_stats(iouring_sends,blocking_sends,
  send_bytes)@159-168; shm_worker.c field@107, apply@519, LOG@620-627 (infers method from counters).
  56 io_uring refs total; shm_arrow.c has ZERO. No `tcp_send_inflight_blocks` GUC yet (P2 adds it).
- Harness: verify_offload.sh TRANSPORT=tcp = 137/137 (BLOCK_EVENT=ShmCopiedBlocks≥1); wsweep_split.sh
  measure()/stats() median-of-N≥5; CH port :21002; gtest `unit_tests_dbms --gtest_filter='TcpStreamSource.*'`.
**INTERPRETATION.** The PROMPT's code map is accurate. One discrepancy to resolve at P1: the build agent
claimed "no tcp_send_all_zerocopy function / SO_ZEROCOPY path unimplemented" but the producer agent read
`tcp_send_all_msg_zerocopy` in full (line 830, drain@819) — the build agent keyword-searched the wrong
name. Trust the producer agent (read the body); re-verify at P1.
**LEARNINGS.** C1 nested-epoll design is contract-valid and the timerfd is load-bearing for stall in the
single-threaded infinite-wait path. C1's H3 cancel-eventfd escape hatch is NOT needed: during
connect/handshake the executor is *inside* `ensureConnected` (not parked on schedule()'s fd, which doesn't
exist yet), and cancel is observed via the `cancelled` flag polled each connect-retry / SO_RCVTIMEO recv
slice; once async-parked the socket is in the inner epoll so `shutdown()` wakes it.
**DONE.** Understanding pass complete; pre-registration written (00-PRE-REGISTRATION.md). → C1 implement.

---

## L0019 — C1 W=8 parity campaign + the TPC-H between-run-variance trap (root-caused)
**Holistic note.** C1 changes only the consumer readiness mechanism (eventfd+thread → epoll fd); the
recv→adopt→emit data path is byte-identical, so any wall delta vs baseline can ONLY come from
thread-spawn elimination or measurement noise.
**WHAT.** Measured W=8 offload wall (TCP+Arrow × ClickBench+TPC-H) on C1 (c27379bcdaf) then baseline
(c286c6cc), each FRESH (revert files → rebuild `clickhouse` → restart CH → sweep). Compared with
compare_parity.py (band = max(5%, within-run sd)).
**RESULT (raw, first pass).** 67 parity / 38 "faster" (C1) / 2 "SLOWER" / 3 skipped. ClickBench
systematically C1-faster ~3–10%; TPC-H mixed, 2 cells (tcp Q7 +5.7%, Q14 +6.1%) over the 5% band.
Correctness verdicts: identical except CB Q22 DIFF(8)→DIFF(9) (both wires) and Q4 approx-tol jitter.
**ROOT-CAUSE (the key finding).** Re-measured the decisive cells on the LIVE BASELINE binary ~30 min
after its first sweep: **TPC-H Q7 1336→1660 (+24%), Q14 1148→1446 (+26%) — the SAME binary**. So TPC-H
sf10 has ±~25% BETWEEN-run variance that the within-sweep N=5 sd (~0.7%) massively underestimates
(page-cache / CH-server-warmth / memory state across restarts). C1's TPC-H numbers (1412/1218) sit
INSIDE the baseline envelope [1336–1660]/[1148–1446] ⇒ the "+5.7/+6.1% regressions" are NOT real; the
A-then-B (C1-first) ordering + this variance produced the artifact. ClickBench has much lower between-run
variance (~1–4%); C1 trends equal-or-faster there (consistent with eliminating the per-async thread spawn
— a real, mechanism-grounded consumer-side effect, bounded but not headline-claimed pending the
drift-controlled interleave).
**FIDELITY (Q22, blocking — RESOLVED).** Ran Q22 offload 8× on the baseline binary: the result hash
VARIED across all 8 runs (native stable across 3). Q22 = `... ORDER BY c DESC LIMIT 10` with COUNT ties
→ nondeterministic top-10 boundary → the DIFF cell-count flips 8↔9 run-to-run independent of C1. Q4 =
`AVG(UserID)` float-order jitter (both verdicts "approx" within machine epsilon). **C1 introduced NO new
DIFF** (it cannot — bytes are identical); the verdict shifts are query/float nondeterminism, demonstrated
ON THE BASELINE BINARY.
**LEARNING.** The within-run sd is the wrong noise estimate for TPC-H sf10; the real band is between-run
(±25%). Re-assessing C1 parity requires a drift-controlled INTERLEAVED repeated measurement (alternate
C1/baseline restarts each round) → L0020. compare_parity.py's within-run band is too tight for TPC-H.
**CONTINUE → L0020 (interleaved A/B).**

---

## L0020 — C1 parity: drift-controlled interleaved A/B (DECISIVE) → PARITY, no regression
**WHAT.** To break the L0019 A-then-B confound, ran an INTERLEAVED A/B: 5 rounds, each round restarts CH
from the saved C1 binary, measures a decisive subset (tcp; CB{2,17,33,38}, TPC-H{6,7,9,14}, N=5), then
restarts from the saved baseline binary and measures the same — so each C1 sample is adjacent-in-time to a
baseline sample (drift cancels). Binaries saved aside (scratchpad/ch_c1, ch_baseline) so no rebuild per round.
**RESULT (raw, across-round median [min,max], rel = C1 vs baseline).**
  tcp CB  Q2  bl 461[452,472]  c1 456[391,478]  -1.1%  parity
  tcp CB  Q17 bl 500[487,513]  c1 496[456,518]  -0.8%  parity
  tcp CB  Q33 bl 631[590,654]  c1 628[582,662]  -0.5%  parity
  tcp CB  Q38 bl 553[549,575]  c1 544[516,592]  -1.6%  parity
  tcp TPCH Q6  bl 1410[1378,1437] c1 1413[1389,1524] +0.2% parity
  tcp TPCH Q7  bl 1556[1536,1568] c1 1563[1511,1630] +0.4% parity   (first-pass had +5.7% "SLOWER")
  tcp TPCH Q9  bl 3002[2956,3052] c1 3003[2939,3052] +0.0% parity
  tcp TPCH Q14 bl 1360[1344,1377] c1 1355[1325,1416] -0.4% parity   (first-pass had +6.1% "SLOWER")
  → 8/8 PARITY, 0 regressions.
**INTERPRETATION.** Under drift control C1 == baseline within noise on EVERY cell, both directions. The
first-pass "2 TPC-H regressions" and "~5% ClickBench win" were BOTH artifacts of (A-then-B ordering) ×
(TPC-H ±25% between-run variance + smaller CB drift). The honest verdict is the pre-registered one: C1 is
PARITY. The thread-spawn elimination is real (gtest-proven, deterministic) but does NOT produce a
measurable loopback wall change — consistent with the pre-registration ("the bridge thread lives in
poll(), removing it is not predicted to move the wall").
**LEARNING.** For TCP-offload parity on this host, interleaved-adjacent A/B is mandatory; sequential
campaigns are confounded by host/CH-warmth drift that swamps a refactor's true (null) effect. Banked as the
phase3 parity methodology (reuse for P1).
**DONE — C1 parity PROVEN (parity + thread elimination). 3+ converging classes (gtest / interleaved W=8 /
correctness+review). → C1 review-fix build, then REPORT + commit.**

---

## L0021 — C1 independent adversarial review (GREEN) + review-fix
**WHAT.** Launched a clean-context 4-angle adversarial review (correctness / fidelity / perf-mechanism /
holism), each verifying the applicable hardening pins, then a synthesis (evidence/ADVERSARIAL-REVIEW.md).
Read-only (ran concurrently with the interleave; no DB/build perturbation).
**RESULT.** Overall **GREEN — PASS**, zero blocking findings (CORRECTNESS/FIDELITY/HOLISM PASS; PERF
CONCERNS only). Pins: H2 SATISFIED, H3 SATISFIED (with a process-gap: cancel-during-connect gtest missing),
H8 N/A (producer pin), H15 SATISFIED-by-proof but the advertised `threadsSpawned` counter was DEAD
(tautological). Non-blocking nits: dead counter; missing cancel-during-connect test; a c1-gtest.md wording
inaccuracy (`<thread>` is removed from the .h, retained in .cpp for std::this_thread::sleep_for).
**REVIEW-FIX (commit CH 97fc5b8b536).** (1) Removed the dead `threadsSpawned`/`g_threads_spawned` counter;
re-based thread-elimination on the deterministic asyncWaitCount=575 + the structural grep (zero std::thread
ctor in the TU). (2) Replaced the (flaky) tight /proc peak assert with a recorded diagnostic + loose
runaway bound — flakiness PROVEN: in isolation base=4/peak=6 (a lazily-spawned global/jemalloc bg thread)
vs peak<=base+1 in the full suite; a tight bound can't distinguish that jitter from a transient bridge
thread anyway. (3) Added C1CancelDuringConnect (200ms, pre-epoll cancelled-flag poll) + C1CancelDuringHandshake
(300ms, shutdown unblocks recvAll) — H3 (a)/(b). (4) Fixed stale comments.
**VERIFIED.** unit_tests 20/20 (C1 suite 6/6 deterministic in isolation); verify_offload tcp 137/137 +
arrow 137/137 on the review-fixed binary. Server runtime unchanged (dead-code + test-only).
**DONE — C1 GREEN. Acceptance met: parity (drift-controlled) + thread elimination (deterministic) + no new
DIFF + clean teardown + stall fires + cancel across all phases + review passed. → P1.**

---

## L0022 — P1 implemented: producer epoll non-blocking send; io_uring + liburing removed
**Holistic note.** End-to-end: PG scan→deform→serialize into tcp_scratch→[P1: send() loop, epoll(EPOLLOUT)
wait on EAGAIN]→kernel send copy→wire→C1 consumer recv. Copy count UNCHANGED (serialize copy + kernel send
copy). Only the send SUBMISSION/readiness mechanism changes (no ring). On loopback (32MiB SO_SNDBUF, fast
C1 consumer) send rarely blocks ⇒ epoll_wait rarely hit ⇒ wall unchanged. No local pessimization: send() is
the same syscall io_uring wrapped, minus submit/CQE overhead.
**WHAT.** Replaced tcp_send_all_iouring with tcp_send_all_epoll (non-blocking send + tcp_wait_writable =
epoll_wait(EPOLLOUT,100ms), kqueue-structured, Linux-only impl, bounded poll(POLLOUT) degrade). Per-worker
tcp_send_epoll_fd created in tcp_accept_conn (only for the epoll method; conn set O_NONBLOCK then), closed
in producer_cleanup (H8). Deleted ring/tcp_iouring_ensure/tcp_send_all_iouring/PGCH_TCP_IOURING_ENTRIES/
io_uring_queue_exit/#include<liburing.h>/Makefile -luring. Enum IOURING->EPOLL (default); GUC epoll/async;
blocking+msg_zerocopy kept; iouring_sends->epoll_sends. Commit 8fe16dd.
**VERIFIED.** H13 zero-residual: grep io_uring/iouring/IOURING/PGCH_USE_LIBURING in src/+Makefile* → only
explanatory comments. Mechanism (the saved binaries): pgch_p1.so ldd liburing=0, nm io_uring syms=0, epoll
syms=3; pgch_baseline.so ldd liburing=1, io_uring syms=4. Live LOG: method=epoll epoll_sends=458/464
blocking_sends=0 (epoll path on the wire, not a silent fallback); method=msg_zerocopy epoll_sends=0
zc_sends=691 zc_copied=679 (msg_zerocopy still works, errqueue + SO_EE_CODE_ZEROCOPY_COPIED intact, composes
with the non-blocking loop). Gates: verify_offload tcp 137/137 + arrow 137/137 (P1 producer + C1 consumer),
both correct results. **CONTINUE → P1 parity interleave (L0023) + review.**

---

## L0023 — P1 parity: drift-controlled interleaved A/B (epoll vs io_uring) → PARITY, no regression
**WHAT.** Drift-controlled interleaved A/B of the PRODUCER send method: 5 rounds, each round atomically
installs the P1 .so (epoll) then the baseline .so (io_uring) and measures the decisive subset (tcp;
CB{2,17,33,38}, TPC-H{6,7,9,14}, N=5). Consumer = the live C1 ClickHouse (CONSTANT — isolates the producer
change); LTO bitcode constant (P1's) across both sides (not a confound — only the send method varies); no
PG restart (session_preload + dynamic bgworker reload the swapped on-disk .so per fresh psql).
**RESULT (raw, across-round median [min,max], rel = epoll vs io_uring).**
  tcp CB  Q2  bl 455[453,477]  p1 455[444,474]  +0.0%  parity
  tcp CB  Q17 bl 507[483,516]  p1 497[483,507]  -2.0%  parity
  tcp CB  Q33 bl 633[597,660]  p1 628[623,646]  -0.8%  parity
  tcp CB  Q38 bl 555[538,578]  p1 550[531,568]  -0.9%  parity
  tcp TPCH Q6  bl 1390[1374,1420] p1 1395[1379,1427] +0.4% parity
  tcp TPCH Q7  bl 1533[1497,1568] p1 1546[1529,1572] +0.8% parity
  tcp TPCH Q9  bl 2983[2980,2988] p1 3018[2966,3043] +1.2% parity
  tcp TPCH Q14 bl 1332[1290,1354] p1 1339[1317,1365] +0.5% parity
  → 8/8 PARITY, 0 regressions (rel -2.0%..+1.2%, all within max(5%, between-round sd)).
**INTERPRETATION.** epoll == io_uring within noise on every cell, both directions. Confirms the
pre-registered prediction (parity-by-construction: io_uring was used SYNCHRONOUSLY — submit→wait, one in
flight — so it never overlapped anything; epoll non-blocking send + EAGAIN-wait does the same copy + wire).
Arrow uses the identical producer send path (tcp_send_all), so the parity is wire-agnostic; tcp measured.
**MECHANISM (already in L0022).** P1 .so: 0 liburing / 0 io_uring syms; baseline: liburing + 4 syms. Live
LOG method=epoll epoll_sends>0 blocking_sends=0 (epoll path on the wire). H13 zero-residual.
**RESOLVES adversarial-review B1** (the premature "Measured: parity" claim now substantiated by this
across-round table on the committed P1 state).
**DONE — P1 parity PROVEN (parity + io_uring-off mechanism + msg_zerocopy intact). → P1 review-fix commit +
REPORT.**

---

## L0024 — P2 design + iteration-1 pre-registration (bare-loopback NULL)
**Holistic note (end-to-end).** P1 publish is BLOCKING: scan/deform(N) → publish(N) [serialize cz->bufs →
frame + send, blocks until kernel accepts all bytes] → deform(N+1). The snapshot boundary (verified
shm_offload.c:590-596: columnizer resets cz->bufs immediately after shm_producer_publish returns) is the
serialize copy. P2 makes publish NON-BLOCKING: serialize block N into a FREE pooled frame (the snapshot,
synchronous), hand the frame to a send reactor, RETURN so deform(N+1) refills cz->bufs while frame N drains
through the kernel→wire. Copy count UNCHANGED (1 serialize + 1 kernel send copy); we do NOT double-buffer
cz->bufs (H1). On bare loopback the kernel drains instantly (32MiB SO_SNDBUF, fast C1 consumer) ⇒ nothing to
overlap ⇒ NULL. Under netem rate the kernel buffer fills, send EAGAINs, and the K-deep pool lets deform run
K blocks ahead of the slow drain ⇒ throughput recovery (the binding proof, it2/it3).
**DESIGN (send reactor, ShmProducer).** K-deep frame pool (GUC pg_clickhouse.tcp_send_inflight_blocks,
default 2, ≥1); EXACTLY ONE frame SENDING at a time (byte-stream order) via a FIFO of READY frames. Each
frame = a coalesced 2-entry iovec {header/meta, payload/body} + total_len + sent-offset (partial-send
resumable, H9) + zc_seq + state{FREE,READY,SENDING[,ZC_PENDING]}. Bespoke: frame owns a tcp_scratch-sized
serialize buffer (tcp_serialize_block writes into it) + the TcpBlockHeader inline; ONE sendmsg of {bh,
payload} (was two send()s). Arrow: K encoders (encode into encoder[i] body/message; frame iovec = {message,
body}); reclaim encoder[i] when its frame's send completes. Per publish (non-blocking): pump the in-flight
send (advance, non-blocking) → acquire a FREE frame (if all K busy, pump + epoll_wait(EPOLLOUT) until one
frees — the backpressure point) → serialize/encode block into it → enqueue READY → pump → return. K=1 ⇒
pool of 1 ⇒ acquire waits for the single frame to drain ⇒ exactly P1 single-in-flight (H14, no special
case). EOS (H5): enqueue EOS after the last data frame, then reactor_drain_all (pump until ALL frames+EOS
fully written to kernel + zc reaped) before eos_published. Cleanup (H8): drain/reap zc seqs before freeing
pooled frames + closing tcp_conn_fd; close the send epoll fd. Async MSG_ZEROCOPY (H4/H6/H7): defer
tcp_zc_drain_until from the steady path; tag each frame with its zc seq; reclaim a frame only when its
completion is reaped (tcp_zc_reap); block on a specific frame's seq only when that exact buffer is needed
and none free; bound K×max_frame ≤ RLIMIT_MEMLOCK (8 MiB) — cap K + log if exceeded.
**ITERATION 1 — pre-registration.** Regime: BARE loopback (lo=noqueue, H12), W=8, tcp. Predicted magnitude:
**NULL / parity** of the K-sweep (K∈{1,2,4,8}) — K>1 ≈ K=1 within noise (no send latency to hide; producer
scan/deform-bound or consumer recv-bound). K=1 MUST equal P1. Correctness: verify_offload tcp+arrow 137/137,
no new DIFF; byte-stream integrity (partial sends, EOS order, cancel/death mid-send). Instruments: K-sweep
end-to-end wall (drift-controlled), producer phase split (overlap-honest), gtest byte-integrity. This is the
honest pre-registered null that confirms the loopback reality; it2 (netem rate) is the binding overlap proof.
**CONTINUE — implement the reactor, gate, then it1 measure.**

---

## L0025 — P2 iteration 1: bare-loopback K-sweep → NULL (pre-registered, confirmed)
**WHAT.** K-sweep (run-ahead K∈{1,2,4,8}, set per-query via the GUC, interleaved across R=2 rounds for drift
control — same P2 binary, consumer = live C1; tcp; W=8; CB{2,17,33}+TPC-H{6,9}; lo=noqueue asserted).
**Harness bug caught + fixed first:** the loop var `K` collided with the wsweep's own `K` (instrumented-run
count) env, routing every K-level to the same OUT dir; renamed the run-ahead loop var to `KV` (the wsweep
keeps `K=1`). Verified distinct k1/k2 dirs in a smoke test, re-ran.
**RESULT (across-round median, rel% vs K=1).**
  CB Q2  K1 462 / K2 +3.5 / K4 +0.8 / K8 +2.9     CB Q17 K1 516 / K2 +0.1 / K4 -1.9 / K8 +0.0
  CB Q33 K1 650 / K2 +1.3 / K4 +0.1 / K8 +2.8      TPCH Q6 K1 1446 / K2 +3.9 / K4 +1.7 / K8 +1.1
  TPCH Q9 K1 3146 / K2 +0.7 / K4 +0.3 / K8 +0.9
  → 0 cells faster, 0 slower beyond noise (all |rel| ≤ band). **NULL / parity.**
**INTERPRETATION.** On bare loopback K>1 ≈ K=1 — the pre-registered honest NULL. Reasons (pre-reg): (a) the
32 MiB SO_SNDBUF already buffers the cross-process pipeline; (b) `send` returns immediately when the buffer
has room ⇒ no send latency to hide when the producer is scan/deform-bound, and when the socket fills the
consumer is recv-bound where producer overlap can't raise throughput. K=1 ≡ P1 confirmed (the K=1 column is
the single-in-flight baseline; correctness already = native at K=1/2/8). This NULL is a valid logged
iteration; the binding overlap proof is the netem regime (it2, L0026).
**DONE (it1). CONTINUE → it2 (netem rate, SO_SNDBUF≈BDP).**

---

## L0026 — P2 it2 (netem rate): the SO_SNDBUF×BDP hinge + a sysctl trap (corrected)
**it2 attempt A (SO_SNDBUF=2MB via wmem_max=1MB, rate 3gbit delay 50us).** Queries ARE heavily rate-bound
(TPC-H Q6 1446→4380ms 3×, Q9 3146→6935ms 2.2×, CB Q17 516→798ms), but the K-curve is FLAT (K2/4/8 all
parity vs K1). DIAGNOSIS (H11 exactly): the 2MB socket buffer DWARFS the link BDP (3gbit×50us = 18.75KB), so
K=1 already runs far ahead via the kernel buffer; K-in-flight (userspace) adds nothing. SO_SNDBUF must be
≈ BDP (well under one frame) for K to gate. Kept as evidence (p2-ksweep-netem3g-sndbuf2M-inert.txt).
**it2 attempt B (clamp wmem_max=16384 to force SO_SNDBUF≈BDP) — TRAP, aborted.** net.core.wmem_max is
GLOBAL: clamping it to 16 KB starved NETLINK sockets → `tc`/`ss` failed ("No buffer space available"), netem
did not install, the run was garbage. Restored wmem_max/rmem_max=64MiB; lo=noqueue; CH+PG verified alive.
**FIX.** Add a PER-SOCKET producer SO_SNDBUF GUC (pg_clickhouse.tcp_sndbuf_bytes) so the experiment can
shrink the producer's send buffer toward the BDP WITHOUT touching the global wmem_max (netlink safe). Re-run
it2 with netem rate + a small per-socket SO_SNDBUF. CONTINUE.

---

## L0027 — P2 it2 (netem epoll) = NULL, root-caused; the K-win lives on the zerocopy path (→ it3)
**WHAT.** Re-ran the netem3g K-sweep (delay 50us rate 3gbit, per-socket SO_SNDBUF=64KB) at R=3 (R=2 was
too few samples) for a stable K=1 baseline. METHOD=epoll. Also captured the overlap mechanism directly.
**RESULT (R=3 medians, rel% vs K=1).** CB17 K1 969 / K2 +2.7 / K4 +5.3 / K8 +11.5(slower);
TPCH Q6 K1 4636 / K2 -0.6 / K4 -0.2 / K8 +0.5; TPCH Q9 K1 7409 / K2 -0.2 / K4 +0.5 / K8 +0.6.
→ FLAT (K>1 ≈ K=1; high-K slightly SLOWER from pool/pump overhead). The earlier R=2 "K>1 ~2x faster" was
an ARTIFACT of K=1's noisy slow tail (2 samples spanned 3378–11698 ms); the R=3 K=1 is stable+slow and K>1
does not beat it. Re-running caught a false win — recorded honestly.
**MECHANISM (decisive, deterministic — p2-overlap-mechanism.txt).** Same regime, TPC-H Q6, log_stream_stats:
K=1 overlap_frames=0 (all workers); K=8 overlap_frames=228–229 (all workers). So the structural overlap DOES
happen at K>1 (deform/serialize ran ahead ~228×/worker) — but it does NOT recover throughput for the
COPYING (epoll) send.
**ROOT-CAUSE (the key architectural insight).** For the copying send, `send()` frees the frame as soon as
the bytes are copied to the kernel SO_SNDBUF, so K=1 never blocks on buffer REUSE (only on the buffer being
full). The producer is single-threaded and pumps only at publish boundaries, so the link is fed at most one
pump's worth per deform-period regardless of K — a large SO_SNDBUF lets K=1 run ahead (K redundant), a small
one throttles BOTH K=1 and K>1 (the kernel is the only background drainer; no background sender thread). So
the K-deep USERSPACE pool is redundant with the kernel SO_SNDBUF for the copying send ⇒ epoll-netem NULL.
**The lever is MSG_ZEROCOPY (H4, matches Goal pt 3 "K matters most on the zero-copy path"):** there the
frame buffer is PINNED until the kernel reaps the completion, so K=1 MUST drain-per-frame (block) before
reusing the single buffer (≡ P1), while K>1 serializes into other buffers and pipelines. Under netem (slow
reap) K>1 should beat K=1. → it3 tests this (a flat zerocopy K-curve would be the FAIL per H4).
**DONE (it2: epoll-netem NULL, honest + root-caused). CONTINUE → it3 (msg_zerocopy netem).**

---

## L0028 — P2: a single-pointer reactor BUG (sends serialized behind completions) found+fixed; THE OVERLAP WIN
**WHAT (debugging chain).** The netem-rate (epoll+zerocopy) and the first injected-delay K-sweeps were all
FLAT even at valid K>1. Root-caused TWO bugs the correctness tests missed (correctness is K-independent):
 (1) frames sized to the 64 MiB data_region → zerocopy RLIMIT cap forced K=1 (fixed: tcp_frame_capacity
     sizes to the tight per-block max; all-fixed schema ⇒ ~1.5 MB ⇒ K up to 5 under the 8 MiB RLIMIT);
 (2) msg_zerocopy socket not O_NONBLOCK + no send-epoll-fd ⇒ poll-spin (fixed: O_NONBLOCK+epoll for zc too);
 (3) **the load-bearing one** — the reactor used ONE FIFO cursor (send_drain) for BOTH sending and
     reclaiming: after the head frame was fully sent, pump tried to reclaim it and, if it couldn't (zerocopy
     completion / injected latency not yet elapsed), STOPPED — so the next frame was not SENT until the
     head's completion arrived. That serialized sends behind completions, so K frames could be
     deform-serialized ahead (overlap_frames>0) but only ONE was ever in flight ⇒ K never pipelined.
**FIX.** Two-cursor reactor: reactor_send_pos() = the first not-fully-sent occupied frame (the send cursor),
advanced independently of send_drain (the reclaim cursor). Frames are sent back-to-back in FIFO order (one
sendmsg at a time = byte-stream order preserved); up to K frames sit IN FLIGHT (sent, awaiting completion)
concurrently; reclaim is lazy (frees completed frames from send_drain). verify_offload tcp 137/137 +
K=1/2/8=native preserved.
**RESULT — injected per-frame latency microbench (PROMPT-sanctioned complementary instrument; Q99 all-fixed,
no netem, the injected delay IS the send/completion latency; R=2 N=3).**
  delay=20ms/frame:  K=1 2554ms / K=2 1378 (-46%) / K=4 949 (-63%, 2.7x) / K=8 947 (-63%, knee).
  K=4 fully HIDES the 20 ms/frame latency, recovering to ~947 ms ≈ the no-delay baseline (944 ms). Clean,
  large, monotonic K-curve with a knee at K=4 (where K x per-block-work >= the latency).
**INTERPRETATION.** This is the BINDING overlap proof: the K-deep pipeline RECOVERS THROUGHPUT when send/
completion-latency exists — K hides the latency by keeping K frames in flight concurrently. It directly
PROVES the design works (and the two-pointer fix was essential). It also vindicates the loopback/netem
NULLS (it1/it2/it3): those have NO real send-latency to hide (loopback MSG_ZEROCOPY is a fast deferred copy
— the measured-null; the kernel SO_SNDBUF already pipelines the copying send), so K is correctly null there,
while the mechanism delivers the moment a real send-latency is present (the real-NIC expectation).
**CONTINUE → knee-moves-with-latency confirmation + re-run netem zerocopy on the fixed reactor.**

---

## L0029 — P2 knee-scaling + netem-on-fixed-reactor (host-intrinsic null confirmed)
**Knee scales with latency (the BDP/latency-pipelining signature).** Injected-delay microbench, Q99 all-fixed:
  delay=20ms: K1 2554 / K2 1378(-46%) / K4 949(-63%) / K8 947  → knee K=4
  delay=40ms: K1 4855 / K2 2518(-48%) / K4 1360(-72%) / K8 952(-80%, 5.1x) → knee K=8
Doubling the per-frame latency doubles the K needed to hide it (K4→K8); at the knee the wall recovers to the
~947 ms no-delay baseline. K_knee ∝ latency / per-block-work — exactly the H11 "knee moves with BDP" analog
(here the latency analog). evidence/p2-injdelay-knee.txt.
**netem zerocopy on the FIXED reactor = still FLAT** (delay 500us rate 10gbit, Q99: K1 1911 / K2 -0.4% /
K4 -0.1%). This is the decisive control: the two-pointer reactor DOES pipeline (the injected-delay microbench
proves it), so the netem zerocopy flatness is NOT a reactor bug — it is HOST-INTRINSIC: on loopback
MSG_ZEROCOPY performs a fast deferred COPY (SO_EE_CODE_ZEROCOPY_COPIED, the phase-2 measured-null), so the
completion is NOT gated by the netem packet delay → K=1 never stalls on completions → flat. netem can delay
the data path but cannot make the loopback zerocopy completion hardware/ACK-gated. evidence/
p2-kbench-netem-zc-fixed.txt. lo restored to noqueue, wmem_max=64MiB (H12).
**P2 VERDICT.** The K-deep pipeline is correctly implemented (two-cursor send-ahead, K-in-flight, coalesced
sendmsg, async-zerocopy-completion, tight frames, K=1≡P1, EOS/cleanup) and DEMONSTRABLY recovers throughput
when send/completion-latency exists (injected-delay microbench: 2.7x@20ms/K4, 5.1x@40ms/K8, knee scales with
latency). On THIS loopback host it is an honest NULL across bare + netem-rate + netem-delay for both epoll
and zerocopy — root-caused: (a) the kernel SO_SNDBUF already pipelines the copying send; (b) loopback
zerocopy is a fast deferred copy, so the real-NIC completion latency that K hides does not manifest, and
netem cannot reproduce it. The win is the real-NIC expectation; the loopback null is pre-registered + honest.
≥3 iterations logged (it1 bare null L0025, it2 netem-rate null L0027, it3 zerocopy bug-fix + injected-delay
win + knee-scaling L0026/L0028/L0029). DONE.
