# Hot-Cold Phase 3 — Pre-registration (mechanism + predicted magnitude)

**Registered:** 2026-06-26, BEFORE any code change or measurement. A prediction/result mismatch is a
finding to investigate, never to rationalize (PROMPT §NON-NEGOTIABLE EVIDENCE STANDARD pt 3).

## SHAs / environment of record (immediately-prior committed state — the parity baseline)
- pg_clickhouse: `streamed-table-shm-offload` @ `29a1e501efc002a3734c29e31d2e3c23f69c2359`
- ClickHouse:    `streamed_table`              @ `c286c6cc32e49df296eb505cd7c47b8f15e77ac6`
- Host: AWS Graviton aarch64, 32c / 61 GiB, dedicated/idle (load 0.08 at registration < 0.5).
- PG 18 :5432; ClickHouse reldeb live pid 447270 on 127.0.0.1:21002.
- sysctls: `net.core.wmem_max = net.core.rmem_max = 67108864` (64 MiB). SO_SNDBUF/SO_RCVBUF hard-set
  32 MiB each (producer `tcp_accept_conn`, consumer `ensureConnected`).
- `tc qdisc show dev lo` = `noqueue` (clean — required before every bare-loopback parity measure, H12).
- liburing.h present at /usr/include/liburing.h ⇒ the io_uring TCP send path is the compiled-in default
  today (the P1 parity baseline). GUC `pg_clickhouse.tcp_send_method` default = `PGCH_TCP_SEND_IOURING`.
- W=8 (cgroup-v2 `cpu.max = 8*100000/100000`), ClickBench `hits` 10M, TPC-H `tpch_sf10`.

## Noise band (binding accept/reject rule for all parity claims)
Effect is real only if relative diff > `max(5%, 1 stdev)` (PROMPT pt 4). At/below that ⇒ report exactly
as "within noise / parity". Baselines (io_uring for P1; K=1 for P2) measured FRESH on the
immediately-prior committed SHA, identical flags/caps/sysctls, both directions (H17).

---

## Branch C1 — Consumer epoll-fd readiness  (prediction: PARITY + thread/fd elimination)

**Mechanism.** `TcpStreamSource::schedule()` today returns a per-stream **eventfd** (`ready_event_fd`),
written by a **fresh `std::thread`** spawned *per async wait* (`startAsyncWakeBridge`→`asyncWakeBridgeLoop`)
that `poll()`s {`sock_fd`, `async_wake_stop_fd`} up to the remaining stall budget. C1 replaces this with a
source-owned **epoll fd** aggregating {`sock_fd` (EPOLLIN|EPOLLRDHUP|EPOLLERR), a `timerfd` armed to the
remaining stall budget}, returned from `schedule()`. The bridge thread, `ready_event_fd`,
`async_wake_stop_fd`, `async_wake_bridge_stop`, `wakeReadyEvent`, and the four bridge fns are deleted.
Cancellation stays `::shutdown(sock_fd, SHUT_RDWR)` (socket becomes readable/errored → epoll fd fires).

**Why epoll-fd is contract-valid (verified, executor-contract agent + IProcessor.h:209 / Epoll.cpp:39-42).**
The executor calls `IProcessor::scheduleForEvent()` whose default returns `{schedule(), EPOLLIN|EPOLLERR}`;
`Epoll::add` registers it **level-triggered** (ORs EPOLLPRI, no EPOLLET). An epoll fd is EPOLLIN-readable
whenever any contained fd has events — valid — but stays readable until *every* inner event is cleared.
**Single-threaded `PullingPipelineExecutor`** (`ExecutorTasks::tryGetTask`) waits on
`async_task_queue.wait(timeout=-1)` and **NEVER calls `onAsyncJobReady`**; the **timerfd is therefore the
sole stall-budget wakeup in the infinite-wait path** (not mere hygiene). Multi-threaded path's
`processAsyncTasks` monitor calls `onAsyncJobReady`. H2 invariant (drain fully before re-Async) enforced
in BOTH `tryGenerate` early-path and `onAsyncJobReady`: read/disarm the timerfd; drive recv to socket
EAGAIN; never re-Async on EPOLLRDHUP/EPOLLERR (recv drives those to a terminal EOS/throw).

**Predicted magnitude.**
- **End-to-end W=8 (ClickBench + TPC-H, TCP & Arrow-TCP):** PARITY (Δ within noise band, both directions).
  No throughput claim — this is a resource/refactor branch. The bridge thread spends its life in `poll()`
  (not CPU-bound), so removing it is not predicted to move the wall on this loopback host.
- **Resource (the claimable win):** **zero `std::thread` creations on the streaming path** (was 1 per
  async wait → ~1/block under steady streaming); **net per-stream fd delta**: −2 (eventfd + stop-eventfd)
  +2 (epoll fd + timerfd) = **net 0 long-lived fds, but −1 transient thread-stack + −1 thread per async
  wait**. Re-examined honestly: the fd count is ~neutral; the **thread-elimination is the real resource
  win** (no per-async clone/join, no thread-stack churn, no scheduler pressure). Teardown latency:
  predicted equal-or-better (no bridge join on dtor).
- **Instruments (≥3):** (1) gtest static thread-spawn counter = 0 across a slow async multi-block drain
  (H15); (2) gtest bounded `work()`/tryGenerate invocations across backpressure+EOS+half-close (H2, no
  hot-spin); (3) W=8 end-to-end parity table (TCP & Arrow) + (corroborating) `/proc/<ch_pid>/task` peak
  thread count under load before/after.

## Branch P1 — Producer epoll non-blocking send, io_uring removed  (prediction: PARITY + io_uring-off mechanism)

**Mechanism.** Replace `tcp_send_all_iouring` (io_uring `IORING_OP_SEND`, copying, submit-then-
`wait_cqe_timeout(100ms)`, one SQE in flight) with a **non-blocking `send()` + epoll readiness** loop, one
send in flight: set the connected socket O_NONBLOCK; on EAGAIN/EWOULDBLOCK `epoll_wait(EPOLLOUT, ~100ms)`
re-checking `CHECK_FOR_INTERRUPTS()` + `origin_backend_dead()` each slice. Delete the ring + `tcp_iouring_*`
+ the liburing build dep (`PGCH_USE_LIBURING` guard, Makefile `-luring`). `msg_zerocopy` and `blocking`
survive; enum default → the new epoll method.

**Why parity (≈0 by design).** io_uring was used **synchronously** (submit→wait, one in flight) — it never
overlapped anything. epoll non-blocking `send()` does the same copy + the same wire, just without the
ring submit/CQE machinery. The cost is the payload copy + loopback, not syscall submission. So no
throughput change is predicted on loopback.

**Predicted magnitude.**
- **End-to-end W=8 (ClickBench + TPC-H, TCP & Arrow-TCP):** PARITY (Δ within noise, both directions). If a
  *win* appears, root-cause it (likely removed per-send submit/CQE overhead or simpler buffer reuse) and
  prove it — do not assume.
- **Mechanism (claimable):** producer send path is now `sendto`/`send` + `epoll_wait`, **no
  `io_uring_enter`/`io_uring_setup`** (strace/perf); **liburing unlinked** (`ldd`/`nm` on the `.so` show no
  `-luring`/`io_uring_*`). `grep -nE 'io_uring|iouring|IOURING|PGCH_USE_LIBURING' src/ Makefile*` → empty.
- **Robustness (claimable, not measured):** epoll has no seccomp-disabled fallback degradation, unlike
  io_uring (which silently falls back to blocking in hardened/container deployments).
- **Instruments (≥3):** (1) W=8 parity table (TCP & Arrow); (2) strace/perf showing sendto+epoll_wait and
  zero io_uring syscalls; (3) `ldd`/`nm` + grep zero-residual; (4) verify_offload tcp/arrow 137/137 +
  msg_zerocopy mode still functions (errqueue SO_EE_CODE_ZEROCOPY_COPIED accounting unchanged).

## Branch P2 — Pipelined sender, K blocks in flight  (prediction: BARE-LOOPBACK NULL + netem-regime overlap win)

**Mechanism.** On the P1 epoll substrate: a **K-deep frame-buffer pool**
(`pg_clickhouse.tcp_send_inflight_blocks`, default 2, ≥1) — K `tcp_scratch` buffers (bespoke) / K nanoarrow
bodies (arrow); **non-blocking publish** (hand the serialized frame to a send reactor, return so
scan/deform of N+1 proceeds); **exactly one send on the socket at a time** (byte-stream order); the
per-block header+payload coalesced into **one `sendmsg`/`writev`** (iovec, partial-send-resumable). Async
`MSG_ZEROCOPY` completion (defer `tcp_zc_drain_until` out of the steady path; reclaim a pooled frame only
when its zc seq is reaped; block on a specific seq only when that buffer is needed and none free; bound
K×max_frame ≤ RLIMIT_MEMLOCK 8 MiB). K=1 ≡ P1 single-in-flight semantics exactly (no special-case code).

**Why the K-deep send-frame pool IS the deform-overlap (H1).** `tcp_serialize_block` /
`shm_arrow_encode_record_batch` copy out of `cz->bufs` **synchronously inside publish** — that copy is the
snapshot boundary. So K private frame buffers + deferred send let the scan/deform of N+1 refill `cz->bufs`
while the kernel drains frame N. We do NOT double-buffer `cz->bufs` (that only defers the serialize copy =
a copy-budget change, out of scope).

**Why bare-loopback is a NULL (pre-registered, honest, accepted).** (a) The cross-process pipeline already
exists via the 32 MiB `SO_RCVBUF`: `send(N)` already overlaps the consumer processing N−1; P2 adds only
*intra-producer* overlap of deform/serialize(N+1) with send(N). (b) On bare loopback `send` returns
immediately when the socket buffer has room ⇒ no send latency to hide when the producer is the bottleneck
(~70% scan/deform-bound, per phase2 wsweep memory), and when the socket fills the consumer is the
bottleneck (~35% recv-copy) where producer overlap cannot raise throughput.

**The binding overlap proof is netem, not a bare-loopback number (H11/H12).** `tc qdisc add dev lo root
netem delay 50us rate 3gbit` (same-AZ NIC regime; small BDP so the socket fills and K-in-flight gates
throughput), plus `delay 500us rate 10gbit` sensitivity. Size SO_SNDBUF to the link BDP (record effective
getsockopt). The K-sweep under netem **rate** must show K>1 raising throughput vs K=1, the knee moving with
BDP. A complementary fixed-per-send-delay microbench is acceptable. **MSG_ZEROCOPY K-sweep under netem must
show K>1 > K=1** (H4/H6/H7) — a flat zerocopy K-curve is a FAIL (proof the synchronous drain wasn't
deferred).

**Predicted magnitude.**
- **Bare loopback W=8 (ClickBench + TPC-H, TCP & Arrow):** NULL / PARITY vs K=1 (Δ within noise). Reported
  as such; a within-noise wiggle is NOT dressed as a win.
- **Netem rate regime:** K=2..4 recovers throughput vs K=1 by a margin > noise; the knee tracks BDP.
  Predicted default **K=2** unless the sweep shows a clear higher knee (justify by evidence).
- **Mechanism (binding):** perf timeline shows page-reader/deform symbols of block N+1 executing
  concurrently with frame N's `sendmsg`/kernel drain (H1 — a send-frame-only no-op is a FAIL); profile
  shows ONE `sendmsg`/`writev` per data frame (down from two `send`s).
- **Instruments (≥3):** (1) K-sweep end-to-end (bare loopback + netem) TCP & Arrow & msg_zerocopy; (2)
  overlap-honest producer phase split (send-wait wall overlapped by deform/serialize wall); (3) perf
  profile (deform(N+1) ∥ send(N); one sendmsg/frame); (4) gtests: bounded work() (H2), byte-stream
  integrity under partial/backpressure/EOS/cancel-mid-send, injected-delay pipeline microbench.
- **≥3 evidence-based iterations** required before P2 is declared done (each pre-registers regime +
  predicted magnitude). The bare-loopback null is itself a valuable logged iteration.

## Fidelity (all branches)
Transport/scheduling change must NOT change results. TCP, Arrow-TCP, SHM-adopt, SHM-copy agree
cell-for-cell within the documented bounds; `cmp.py` verdicts identical to the pre-change baseline. Any
NEW `DIFF` blocks the branch. Edge cases hammered: empty blocks, count() (no columns), subset projection,
multi-source joins, long/empty strings, Nullable, Decimal scale, DateTime64 precision, and stall / EOS /
producer-death / cancel paths.
