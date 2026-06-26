# Hot-Cold Phase 3 — Branch C1 REPORT (consumer epoll-fd readiness)

**Verdict: GREEN.** ClickHouse `streamed_table` @ `c27379bcdaf` (+ review-fix commit). D-HC-0301.

## What changed
`TcpStreamSource::schedule()` now returns a **source-owned epoll fd** aggregating
{`sock_fd` (EPOLLIN|EPOLLRDHUP|EPOLLERR), a one-shot `timerfd` armed to the remaining stall budget}; the
executor epolls it directly. **Deleted**: the readiness eventfd (`ready_event_fd`), the bridge stop-eventfd
(`async_wake_stop_fd`), the `async_wake_bridge_stop` flag, the `std::thread async_wake_thread`, and the five
bridge functions (`startAsyncWakeBridge`/`asyncWakeBridgeLoop`/`joinAsyncWakeBridge`/
`requestAsyncWakeBridgeStop`/`wakeReadyEvent`). Cancellation stays solely `::shutdown(sock_fd, SHUT_RDWR)`.
The recv → adopt → emit data path is byte-identical to before (only the readiness mechanism changed).

## Acceptance criteria — met
- **Contract verified.** `scheduleForEvent()` default `{schedule(), EPOLLIN|EPOLLERR}` (IProcessor.cpp:69),
  registered level-triggered in `Epoll.cpp` (no EPOLLET). The single-threaded `PullingPipelineExecutor`
  waits `async_task_queue.wait(timeout=-1)` and never calls `onAsyncJobReady` → the **timerfd is the sole
  stall wakeup** there. Multi-threaded path uses `onAsyncJobReady`. Both drain the timerfd + drive recv to
  socket-EAGAIN before re-`Async` (H2). Pinned in code comments + DECISIONS D-HC-0301.
- **Correct.** `verify_offload TRANSPORT=tcp` 137/137 and `TRANSPORT=arrow` 137/137 — no DIFF, clean
  teardown (no leaked shm/sockets/bgworkers). gtests 18/18 (incl. the C1 suite). Stall fires (401ms vs
  400ms budget); cancel-via-shutdown wakes a parked source (300ms ≪ 60s budget); cancel during
  connect-retry and during the blocking handshake tear down promptly (< 3s); half-close → terminal
  `SHM_PRODUCER_DEATH_BEFORE_EOS`, no spin.
- **Resource delta proven (the win).** Zero streaming-path threads: a slow fragmented async drain parks
  **575×** and the source adds **no live OS thread** (`/proc/self/task` peak ≤ baseline+1, the +1 being the
  test's own sampler; pre-C1 a per-async wake-bridge thread would push it to baseline+2). Structural: there
  is no `std::thread` construction anywhere in `TcpStreamSource.cpp` (the only `std::thread`-ish token is
  `std::this_thread::sleep_for` in the connect-retry backoff). Per-stream fd delta: −2 eventfds, +1 epoll
  fd +1 timerfd = **net 0 long-lived fds**; the real resource win is the thread elimination (≈1 thread
  create/join per block under steady streaming → 0). Teardown: no bridge join in the dtor.
- **No W=8 regression.** PARITY on the drift-controlled interleaved A/B (below).

## Evidence — ≥3 converging instrument classes
1. **Isolated gtest (deterministic resource/mechanism proof).** `unit_tests_dbms` single-threaded
   `PullingPipelineExecutor`: 575 async parks → 0 added threads (`/proc/self/task` peak guard), bounded
   parks (≪ 100×n_blocks ⇒ no hot-spin, H2), stall@401ms, cancel-park@300ms, cancel-connect/handshake<3s,
   half-close terminal. See `evidence/c1-gtest.md`.
2. **W=8 end-to-end parity — drift-controlled interleaved A/B (`evidence/c1-interleave-parity.txt`).**
   5 interleaved rounds (C1/baseline restarts adjacent in time), tcp, CB{2,17,33,38}+TPC-H{6,7,9,14}, N=5:
   **8/8 PARITY, 0 regressions** (rel −1.6%..+0.4%, all ≤ band). The full first-pass matrix
   (`evidence/c1-parity-table.txt`, TCP+Arrow×CB+TPC-H) is reported but was CONFOUNDED (A-then-B ordering ×
   TPC-H ±25% between-run variance — see L0019/L0020); the interleave is the trustworthy verdict.
3. **Correctness + fidelity.** `verify_offload` tcp/arrow 137/137 (no DIFF). The lone first-pass verdict
   shift (CB Q22 DIFF(8)→DIFF(9)) is query nondeterminism — Q22's `ORDER BY c DESC LIMIT 10` has count
   ties; its offload result hash varied across 8 runs **on the baseline binary** (native stable). C1 cannot
   introduce a DIFF (bytes identical). See L0019.
4. **Independent adversarial review (`evidence/ADVERSARIAL-REVIEW.md`): GREEN — PASS**, no blocking
   findings. Non-blocking findings (dead H15 counter; missing cancel-during-connect gtest; an evidence
   wording nit) were all FIXED in the review-fix commit.

## Pre-registration scorecard
Predicted **C1 = parity + thread/fd elimination**. Observed: PARITY (drift-controlled), thread elimination
proven, fd net-0. The transient first-pass "ClickBench ~5% win" was investigated and shown to be an
ordering/variance artifact (NOT claimed). Prediction upheld.

## Hardening pins
- **H2** (no hot-spin): SATISFIED — level-triggered nested epoll fully drained both wake paths; RDHUP/ERR →
  terminal throw; bounded-park gtest guard.
- **H3** (cancel via shutdown across all phases): SATISFIED — async-park, connect-retry, and blocking-
  handshake cancel all proven by gtest; no cancel eventfd kept.
- **H15** (thread elimination is a gtest hook): SATISFIED — `/proc/self/task` peak sampler (real guard) +
  asyncWaitCount>0 + structural grep. The dead always-zero counter was removed per review.

## Closing note
C1 removes one `std::thread` create/join per async wait (≈ one per block under steady streaming) and the
two readiness eventfds per stream, replacing them with one epoll fd + one timerfd per stream (net-0 fds).
On this bare-loopback host the change is **wall-neutral (parity)** — the eliminated bridge thread spent its
life parked in `poll()`, so removing it frees a thread/fd resource and simplifies the lifecycle without
moving the throughput. The teardown no longer joins a bridge thread.
