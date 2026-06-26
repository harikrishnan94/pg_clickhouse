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
