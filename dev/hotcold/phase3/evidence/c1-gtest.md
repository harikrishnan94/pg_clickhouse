# C1 evidence — gtest (single-threaded PullingPipelineExecutor) + structural proof

Binary: ClickHouse `streamed_table` @ c27379bcdaf + review-fix. Build: reldeb.
Cmd: `./build/reldeb/src/unit_tests_dbms --gtest_filter='TcpStreamSource.*:ArrowStreamSource.*'`
Result: **20/20 PASSED**. Existing TcpStreamSource(3) + Arrow(10) + LoopbackThroughputMicrobench(1)
unchanged-green; 6 C1 tests below. The C1 suite also passes **in isolation** (`--gtest_filter=TcpStreamSource.C1*`,
6/6) — deterministic, not order-dependent.

## Instrument class: isolated gtest (the deterministic resource/mechanism proof)

| test | time | RecordProperty | meaning |
|---|---|---|---|
| C1AsyncEpollNoThreadsAndBounded | 1187 ms | c1_async_parks=**575**, c1_base_threads=4, c1_peak_threads=6 | async resumable-recv path parked **575×** across a slow fragmented 25-block drain; bounded (575 ≪ 100×25=2500) ⇒ **no epoll hot-spin (H2)**. The /proc/self/task peak is a recorded diagnostic only (see note). |
| C1CancelViaShutdownWakesParkedEpoll | 300 ms | c1_cancel_wake_ms=300 | parked on the epoll fd (60s budget); `executor.cancel()` at t=300ms → `onCancel` → `::shutdown(SHUT_RDWR)` → epoll fd fired → `pull()` false at ~300ms (≪ 60s). **shutdown() is the sole cancel wake (H3, async-park).** |
| C1StallTimeoutFires | 401 ms | c1_stall_fire_ms=401 | stalled producer, 400ms budget → `SHM_PRODUCER_STALL` at 401ms. The **timerfd is the sole stall wakeup** in the single-threaded `async_task_queue.wait(-1)` path. |
| C1HalfCloseBeforeEosThrows | 0 ms | c1_halfclose_parks=2 | producer half-closes (no EOS) → `SHM_PRODUCER_DEATH_BEFORE_EOS`; 2 bounded parks ⇒ **no spin on EPOLLRDHUP** (driven to terminal throw, H2). |
| C1CancelDuringConnect | 200 ms | c1_cancel_connect_ms=200 | cancel while in the connect-retry loop (no listener → ECONNREFUSED, 5s retry budget); torn down at ~200ms via the `cancelled` flag poll — **before any epoll fd exists (H3 (a))**. |
| C1CancelDuringHandshake | 300 ms | c1_cancel_handshake_ms=300 | cancel while blocked in the handshake `recvAll` (accept, no handshake sent); `::shutdown(SHUT_RDWR)` unblocks the blocking recv → torn down at ~300ms (**H3 (b)**). |

### Thread-elimination proof (deterministic — H15)
- **asyncWaitCount = 575** (genuinely incremented per async park): the resumable-recv async path was
  exercised 575× and the drain completed correctly (25/25 blocks).
- **Structural (airtight, CI-checkable):** `grep -nE 'std::thread' TcpStreamSource.cpp | grep -v
  'std::this_thread'` → only a comment; there is **zero `std::thread` construction** in the TU (the only
  thread-ish token is `std::this_thread::sleep_for` in the connect-retry backoff). All bridge/eventfd
  symbols (`startAsyncWakeBridge`/`asyncWakeBridgeLoop`/`joinAsyncWakeBridge`/`requestAsyncWakeBridgeStop`/
  `wakeReadyEvent`/`ready_event_fd`/`async_wake_stop_fd`/`async_wake_thread`) are deleted from both
  `.cpp` and `.h`. ⇒ the per-async wake-bridge thread cannot exist; 575 parks spawned nothing.
- **/proc/self/task peak: recorded, NOT hard-asserted.** A tight peak bound is flaky: a lazily-spawned
  global/jemalloc background thread appears mid-drain nondeterministically (observed base=4/peak=6 in
  isolation vs peak≤base+1 in the full suite), and could not distinguish a single transient bridge thread
  from that jitter anyway. The test keeps only a loose runaway-leak sanity bound (peak < base+10). The
  deterministic proofs above are the real guard; the dead always-zero `threadsSpawned` counter was removed
  per the adversarial review (it was tautological).

### `<thread>` accounting (precise)
`#include <thread>` was removed from **TcpStreamSource.h** (no more `std::thread` member). The **.cpp**
retains `#include <thread>` only for `std::this_thread::sleep_for` in the connect-retry backoff — there is
no `std::thread` construction.

## Correctness gates (own ephemeral CH instance from the review-fixed C1 binary)
- `verify_offload TRANSPORT=tcp`   → **PASS=137 FAIL=0**, no DIFF, clean teardown.
- `verify_offload TRANSPORT=arrow` → **PASS=137 FAIL=0**, no DIFF, clean teardown.
