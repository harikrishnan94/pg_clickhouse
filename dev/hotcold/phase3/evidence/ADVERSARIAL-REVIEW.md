# C1 (consumer epoll-fd readiness) — Consolidated Adversarial Review

Branch: `streamed-table-shm-offload` / Phase 3 C1. ClickHouse `streamed_table` @ `c27379bcdaf`.
Diff under review: `TcpStreamSource.cpp`, `TcpStreamSource.h`, `gtest_tcp_stream_source.cpp` (3 files).
Source paths verified against `/home/ubuntu/ClickHouse/src/Storages/SharedMemorySource/`.
Reviewers merged: CORRECTNESS (PASS), FIDELITY (PASS), PERFORMANCE & MECHANISM (CONCERNS), HOLISM (PASS).

---

## Overall verdict

**GREEN — PASS.** No unresolved blocking findings exist across any of the four angles; the lone
CONCERNS verdict (PERF) is driven entirely by a non-load-bearing dead instrument (H15 counter) and an
evidence-wording inaccuracy, neither of which is a defect in the shipped mechanism — thread elimination,
H2 no-spin, H3 cancel, and byte-fidelity are each independently proven.

---

## Blocking findings

**None.** All four reviewers returned `blocking_findings: []`. The C1 readiness rewrite (per-async
`std::thread` wake-bridge + 2 eventfds → source-owned `epoll_fd` + `timerfd`) does not alter the
recv→decode→adopt→emit data path, introduces no fd leak / double-free, and preserves stall/EOS/cancel
semantics. PERF's `CONCERNS` rests on non-blocking items below; it does not assert a defect.

---

## Hardening pins

| Pin | Scope | Consolidated status | Basis |
|---|---|---|---|
| **H2** (no epoll hot-spin) | consumer | **SATISFIED** (all 4 reviewers agree) | Inner `epoll_fd` aggregates `{sock_fd(EPOLLIN\|EPOLLRDHUP\|EPOLLERR), timerfd}`; executor registers it **level-triggered** (`Epoll.cpp:39-52`, no EPOLLET; `scheduleForEvent` default `IProcessor.cpp:67-70`). Both wake paths drain the timerfd (`tryGenerate:1201`, `onAsyncJobReady:1297`) and drive the socket to EAGAIN (`tryRecvInto`) before re-Async. RDHUP/PeerClosed → terminal `SHM_PRODUCER_DEATH_BEFORE_EOS` throw, never re-Async. Bounded-park gtests: 575 ≪ 2500 ceiling; half-close = 2. **Caveat (non-blocking):** spin bound is asserted **single-threaded only**; the multi-threaded `onAsyncJobReady` drain (`ExecutorTasks.cpp:316-346`) is reasoned-correct but not deterministically gtested (covered only by `verify_offload`, a correctness oracle blind to CPU spin). |
| **H3** (shutdown is sole cancel wake) | consumer | **SATISFIED with a documented PROCESS-GAP** | Cancellation is solely `::shutdown(sock_fd, SHUT_RDWR)` (`onCancel:1302-1312`); cancel-during-park proven by `C1CancelViaShutdownWakesParkedEpoll` (woke ~300ms ≪ 60s budget). Connect-retry (`:426`) and blocking-handshake-recv (`recvAll` body) windows poll `cancelled` each slice and are unblocked by shutdown — **correct by inspection**. **GAP:** the pin mandates a *cancel-during-connect gtest*; verified absent — the 4 C1 tests cancel only during the async-park phase. Missing required-proof artifact, not a defect (only CORRECTNESS assessed H3). |
| **H8** (producer fd/zerocopy cleanup) | producer (P1/P2) | **N/A for C1** | HOLISM: H8 is a producer-side pin; C1 is the consumer branch and touches zero producer code. The consumer-analog fd lifetime is correct — dtor closes `epoll_fd → timerfd → sock_fd.exchange(-1)` (`:346-352`), all members init `-1`, every partial-failure/error/EOS/cancel path is dtor-safe at W=8. (Only HOLISM assessed H8.) |
| **H15** (prove zero threads via controlled count) | consumer | **SATISFIED by proof, with a DEAD advertised instrument** | Thread elimination is **real for this commit**: structural grep finds zero `std::thread`/`jthread`/`async`/`pthread_create` ctor sites in the TU; `startAsyncWakeBridge`/`asyncWakeBridgeLoop`/`async_wake_thread` + both eventfds are deleted. The live proof is `asyncWaitCount()=575>0` (**genuinely** `fetch_add` @`TcpStreamSource.cpp:1259`) — the async path ran 575× while no thread spawned. **DEAD INSTRUMENT (3 reviewers: PERF, HOLISM, + noted by FIDELITY):** `g_threads_spawned` (`:139`) is **never incremented** anywhere (verified: only def `:139`, getter `:1334`, reset `:1338` — no `fetch_add`); so `EXPECT_EQ(threadsSpawned(), 0u)` @`gtest:425` is tautologically true and would NOT catch a re-introduced thread. The pin's positive requirement is met by grep + the live `asyncWaitCount`; the advertised regression guard is window dressing. |

---

## Non-blocking findings / follow-ups

Ranked by priority. Deduplicated across reviewers (the H15 dead-counter and the Q22/Q4 nondeterminism
were each raised by multiple angles and are merged into single items).

1. **[HIGH — fix or restate] H15 `g_threads_spawned` is a dead counter.** Confirmed by source read:
   defined `TcpStreamSource.cpp:139`, read `:1334`, reset `:1338`, **never `fetch_add`'d**. The
   `.h:148-149` comment ("counts std::thread constructions") is false — it counts nothing. The gtest
   assert at `gtest_tcp_stream_source.cpp:425` is tautological. **Action:** either wire the counter to a
   real instrumented `std::thread` ctor trap, or delete it and restate the claim as "structural grep +
   `asyncWaitCount>0`". (Raised by PERF, HOLISM; corroborated by FIDELITY/CORRECTNESS naming
   `asyncWaitCount` as the real proof.)

2. **[MED — process gap] H3 mandatory cancel-during-connect gtest is absent.** The pin says a
   cancel-during-connect gtest is mandatory; the suite only cancels during async-park
   (`C1CancelViaShutdownWakesParkedEpoll`). The connect-retry (`:426`) and blocking-handshake-recv
   windows are cancel-safe by inspection but unproven by test. **Action:** add a cancel-during-connect /
   cancel-during-handshake gtest. (CORRECTNESS.)

3. **[MED — unbacked evidence] L0019's "Q22 varied across 8 baseline runs" claim has no artifact.**
   `results/recheck_bl_cb.log` shows a single baseline Q22 = DIFF(8); the 8-run hash log does not exist
   on disk. The *conclusion* (Q22 DIFF(8)↔DIFF(9) is `ORDER BY c DESC LIMIT 10` count-tie nondeterminism,
   not C1 — corroborated by the tiebreak variant `evidence/phase-topn/tiebreak/q22.chsql.txt`) is
   independently correct, but the cited proof should be captured before being relied on. (FIDELITY;
   the verdict-shift root-cause itself is confirmed non-C1 by CORRECTNESS/PERF/HOLISM.)

4. **[MED — methodology gate] ClickBench "~5% win" not yet defensible vs between-run variance.**
   `c1-parity-table.txt` applies a fixed 5% within-run floor and prints "38 faster", but the same
   between-run-variance argument that dissolves the TPC-H "regressions" (Q7 +5.7%, Q14 +6.1% =
   ±25% same-baseline-binary drift, L0019/`recheck_bl_tpch.log`) would also erode a 3-5% CB win unless
   CB between-run variance is genuinely ~1-4% — asserted in L0019 but not yet in the on-disk first-pass
   table. The drift-controlled interleaved A/B (`agg_interleave.py`, band = `max(5%, sd/med)`) is the
   correct gate and is running. Honest in intent (pre-registration: "no throughput claim — resource/
   refactor branch"); the in-file summary line lacks the between-run caveat. **Action:** gate any CB
   win on the interleave result. (PERF; TPC-H dismissal judged HONEST by FIDELITY/HOLISM.)

5. **[LOW — evidence inaccuracy] `#include <thread>` still present in `TcpStreamSource.cpp:59`.**
   Confirmed by source read. `c1-gtest.md:33` claims thread machinery removal and the commit message
   lists it deleted — true for the `.h` (no include) but the `.cpp` still includes `<thread>`. Weakens
   the compile-guard against re-adding a thread (a `std::thread` could be constructed without an include
   error). **Action:** drop the include or correct the evidence wording. (PERF.)

6. **[LOW — test coverage] Multi-threaded executor path unit-untested.** All 4 C1 gtests use
   `PullingPipelineExecutor` (`num_threads==1`); `onAsyncJobReady` (`ExecutorTasks.cpp:316-346`) is
   exercised only by `verify_offload` W=8 (137/137 happy-path + clean teardown), which surfaces DIFF/
   hang but not RDHUP-terminal / cancel-wake / spin in isolation. Reasoning holds (drains timerfd +
   clears `is_async_state`; `work()→tryGenerate` drives socket to EAGAIN before re-park). **Action:**
   optional multi-threaded spin-bound gtest. (CORRECTNESS, FIDELITY, HOLISM — same gap as the H2 caveat.)

7. **[LOW — doc nuance] Fragile / cosmetic couplings worth a one-line comment.** (a) `armStallTimer`'s
   `elapsed >= stall_timeout_ms ? 1 : remaining` 1ms-floor (`:1325`, confirmed) is correct disarm-hang
   insurance but effectively unreachable in normal flow — the `:1253` stall check throws first; comment
   over-frames it as load-bearing. (b) Stale `is_async_state=true` on cancel wake (returns @`:1186-1187`
   before reset @`:1199-1203`) is safe only because `prepare()`'s cancel short-circuit @`:1267` returns
   Finished before the Async branch @`:1274` — fragile check-ordering coupling. (c) A previously-armed
   one-shot timerfd can fire while the source is Ready (not parked); benign (stall decision reads the
   `Stopwatch` @`:1253`, not the timerfd) but undocumented. (CORRECTNESS, HOLISM.)

8. **[LOW — framing] fd accounting is net-zero, not a win.** −2 eventfds, +1 epoll +1 timerfd per stream
   = net 0 long-lived fds; decision log D-HC-0301 states this honestly and retracts any fd-count win.
   The commit-message "fewer fds" framing is slightly generous vs the net-zero reality. The real
   resource win is the thread elimination. (PERF, HOLISM.)

---

## Per-angle summary

- **CORRECTNESS — PASS.** Data path byte-identical to baseline (diff touches only readiness machinery;
  `buildChunkFromPayload`/`tryRecvBlock`/adopt unchanged). H2 and H3 mechanisms verified SATISFIED.
  Stall fires (401ms vs 400ms), cancel-via-shutdown wakes a parked executor leak-free, no
  stall-timer/data-arrival hot-spin, nested-epoll PollingQueue/Epoll contract holds. Q22/Q4 verdict
  shifts attributed to pre-existing query nondeterminism, not C1. Flags the H3 cancel-during-connect
  test gap and three fragile-coupling doc nuances (non-blocking).

- **FIDELITY — PASS.** `git diff c286c6cc..c27379bcdaf` confirms only 3 files changed; recv/adopt/emit
  helpers byte-for-byte unchanged → no new DIFF possible from C1. Blocking mode unaffected
  (epoll/timerfd stay −1). EOS still observed, stall still fires, cancel wake correct, `pending_buf`
  freed exactly once (no UAF/double-free). Flags the unbacked L0019 8-run claim and the
  single-threaded-only coverage (non-blocking).

- **PERFORMANCE & MECHANISM — CONCERNS (no blocking findings).** Thread elimination is genuinely
  achieved (zero ctor sites; eventfds removed). ≥3 converging real instruments (grep + live
  `asyncWaitCount=575` + bounded park + stall/cancel probes) carry the proof. CONCERNS driven by:
  the **dead `g_threads_spawned` counter** (the advertised guard, not load-bearing), the residual
  `#include <thread>` in the `.cpp`, and the ClickBench-win presentation pending the interleave. TPC-H
  "regressions" judged HONESTLY root-caused as ±25% between-run drift. fd accounting honest.

- **HOLISM — PASS.** W=8 multi-stream dataflow sound: per-stream `epoll_fd`+`timerfd` created in
  `ensureConnected` (`:520-536`), closed dtor-safe in order (`:346-352`), no leak on any
  error/cancel/EOS partial-failure path; `ensureConnected` guarded to run at most once. `pending_buf`
  free-once preserved. Teardown latency improved (dtor no longer joins a bridge thread). `onCancel`
  touches only `sock_fd` (atomic) — no cross-thread race with `epoll_fd`/`timerfd`;
  `sock_fd.exchange(-1)` guards double-close. H8 = N/A (producer pin); H2 SATISFIED; H15 counter dead
  (same finding as PERF).
