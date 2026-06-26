# Hot-Cold Phase 3 — epoll/kqueue transport + pipelined non-blocking sender (Branch C1 · P1 · P2)

## Mission & operating mode

You run **unattended**. Deliver the three branches below — **(C1) consumer epoll-fd readiness**,
**(P1) producer epoll/kqueue non-blocking send (io_uring removed)**, and **(P2) pipelined sender, K
(GUC-configurable) blocks in flight** — end to end, executed **C1 → P1 → P2**, and prove each with converging evidence to
the standard in this document. This is an **unreleased, experimental** product with **no
backward-compatibility constraint** — GUCs, ABIs, the `pgch_tcp_send_method` enum, the consumer async
fd model, build dependencies, anything may change if it serves the Goal. Whenever an ambiguity arises,
**exercise your own judgement and choose the option that most closely serves the Goal.** If a decision
genuinely cannot be made without a human, **record it in the Decision log**
(`dev/hotcold/DECISIONS.md`, format `D-HC-####`, dated, alternatives + rationale) and proceed with the
best-aligned default. **Never block; never wait.**

This phase changes the **I/O readiness + scheduling mechanism only** — it does **NOT** touch the wire
format (Arrow / bespoke), the type mapping, or the copy budget. The end-to-end copy count is unchanged
(producer = 1 serialize copy + 1 kernel send copy on loopback; consumer = single-copy recv + zero-copy
adopt). Scope creep into format/adoption work is out of bounds; if you find a copy-reduction
opportunity, log it as a `D-HC-####` follow-up and do not implement it here.

Three hard process rules specific to this task (in addition to the evidence standard below):

1. **Optimization iterations apply to Branch P2 only.** C1 and P1 are a **resource/refactor** branch and
   a **parity-preserving** refactor respectively — their bar is *correctness + parity + a proven
   resource/mechanism delta*, not a speed iteration count. **Branch P2 (the pipeline) MUST spend at
   least 3 evidence-based iterations** before returning (pre-register a regime + predicted magnitude →
   implement → gate correctness → measure ≥3 independent instrument classes → adversarial check → log
   DONE/CONTINUE). Declaring P2 done on one measurement is a failure.
2. **You MUST maintain an auditable methodology log** (`dev/hotcold/phase3/METHODOLOGY-LOG.md`, same
   template as phase2 §"Auditable methodology log"). Every experiment/change/measurement gets an
   append-only entry: what you did, how, how verified, the result (raw), interpretation, learnings,
   DONE-or-CONTINUE. The log is a primary deliverable.
3. **Think holistically — end to end — before optimizing.** Before touching code in an iteration, write
   (in the log) how the change affects the *whole* dataflow (PG scan → deform → serialize → kernel send
   → wire → kernel recv → column build → CH pipeline → drop). A local win that pessimizes the system is
   a regression.
4. **Never block for more than 30s at a time.** Poll long-running builds/sweeps/restarts at ~30s
   intervals; keep moving; kill + diagnose anything that has clearly hung.

## Source control & commit discipline (hard constraint)

Spans **two repos**. **All changes on the existing offload branches — no new feature branches, no
worktrees, no detached HEADs:**

- **pg_clickhouse** (`/home/ubuntu/pg_clickhouse`): commit on `streamed-table-shm-offload`.
- **ClickHouse** (`/home/ubuntu/ClickHouse`): commit on `streamed_table`.

Verify the branch in each repo before committing (`git rev-parse --abbrev-ref HEAD`).

**Commit as small, reviewable patches** — one logical change per commit, each independently reviewable
and (where it builds) buildable. Natural seams (in C1 → P1 → P2 order):
(1) consumer: epoll-fd `schedule()` aggregating {socket, timerfd}, delete eventfd + bridge thread;
(2) producer: epoll/kqueue non-blocking `send` loop, delete io_uring send + ring + liburing dep;
(3) producer: K-deep frame-buffer pool (`pg_clickhouse.tcp_send_inflight_blocks`) + non-blocking publish + the send reactor;
(4) tests/gtests; (5) each evidence/methodology doc with the change it justifies.

**Never commit on red.** Run the correctness gates (and build) for the touched repo before each commit.
Measure only on committed, green state. Follow repo git-safety rules: never touch git config; no
force-push; no rebasing/amending pushed commits; do not push unless explicitly asked.

## Context (system under test) — current state, **verified, but re-verify; do not trust blindly**

SHM offload streams a PG heap relation into a co-located ClickHouse over a transport, via
`streamed_table('<name>','<schema>'[,'<transport>'])`. One PG bgworker per CH stream produces rows
(`shm_worker.c`, `shm_producer.c`); a vectorized page reader does deform + MVCC visibility
(`shm_page_reader.c`). The TCP transports (`tcp:`, `arrow:`) bind an ephemeral `127.0.0.1` port per
worker; the consumer is `TcpStreamSource` in
`ClickHouse/src/Storages/SharedMemorySource/Source/`.

**What this phase rebuilds — the I/O readiness + send-scheduling machinery (verify each claim):**

- **Producer send is io_uring, used synchronously.** `tcp_send_all` (`shm_producer.c`) dispatches to
  `tcp_send_all_iouring` (default), `tcp_send_all_msg_zerocopy`, or `tcp_send_all_blocking`
  (`PgchTcpSendMethod` GUC `pg_clickhouse.tcp_send_method`, default `PGCH_TCP_SEND_IOURING`). The io_uring path
  uses `io_uring_prep_send` (**copying** `IORING_OP_SEND`, not `_zc`), ring depth
  `PGCH_TCP_IOURING_ENTRIES=8` but **one send in flight** — `io_uring_submit` then
  `io_uring_wait_cqe_timeout(100ms)` (the 100ms slice is to re-poll `CHECK_FOR_INTERRUPTS()` +
  `origin_backend_dead()`). It is **submit-then-wait, not pipelined**: the next block's serialize cannot
  begin until the current send completes (`p->tcp_scratch` / the nanoarrow encoder body is single and
  reused). Lazy ring init (`tcp_iouring_ensure`), torn down in `producer_cleanup`
  (`io_uring_queue_exit` before close). The `MSG_ZEROCOPY` path is **not** io_uring — it is
  `send(MSG_ZEROCOPY)` + `MSG_ERRQUEUE` reaping (`tcp_zc_reap`/`tcp_zc_drain_until`).
- **Per block the producer does TWO sends** (header/meta then payload/body) via `tcp_publish_block`
  (bespoke) / `arrow_publish_block` (Arrow); handshake/schema sent once; EOS once.
- **Consumer recv is plain `::recv` + an eventfd + a per-async bridge thread.** `TcpStreamSource` is an
  async `ISource`. `schedule()` returns `ready_event_fd` (an `eventfd`). On each `WouldBlock` it
  **spawns a fresh `std::thread`** (`startAsyncWakeBridge` → `asyncWakeBridgeLoop`) that `poll()`s
  {`sock_fd`, `async_wake_stop_fd`} up to the remaining stall budget and `write()`s `ready_event_fd`
  to wake the executor (on socket-ready/err, on the stall deadline, or on stop). `onCancel` does
  `wakeReadyEvent()` + `requestAsyncWakeBridgeStop()` + `::shutdown(fd, SHUT_RDWR)`. The single-threaded
  `PullingPipelineExecutor` never calls `onAsyncJobReady`, so `tryGenerate` also drains the eventfd +
  stops the bridge (idempotent). Recv itself: `recvAll` (blocking `::recv` with `SO_RCVTIMEO` slices,
  handshake) / `tryRecvInto` (non-blocking `::recv` on the `O_NONBLOCK` socket, streaming). **No
  io_uring on the consumer.**
- **Executor async-source contract (verify — the spec was corrected here by adversarial review).** The
  executor does **not** call `schedule()` directly; it calls `IProcessor::scheduleForEvent()`
  (`ExecutorTasks.cpp:176,227`), whose **default** returns `{schedule(), EPOLLIN|EPOLLERR}`
  (`IProcessor.cpp:67-70`) and `PollingQueue`/`Epoll::add` registers that fd **level-triggered**
  (`Epoll.cpp:42` ORs in `EPOLLPRI`, no `EPOLLET`; `IProcessor.h:209` — "executor epoll uses
  level-triggered notifications; read all available data before returning ASYNC"). So: **override
  `schedule()` only, keep the default `scheduleForEvent()`, return the source-owned epoll fd.** An epoll
  fd is `EPOLLIN`-readable whenever any contained fd has events — contract-valid — **but it stays
  readable until every inner event is cleared** (see hardening pin **H2**: this is a hot-spin trap). The
  `EPOLLIN|EPOLLRDHUP|EPOLLERR` socket mask + the timerfd live on the **inner** (source-owned) epoll,
  never on the fd handed to the executor. Re-verify single- vs multi-threaded executor paths before
  relying on any of this.

## Goal (north star) — the design intent and the honest scope

The decision (taken with the maintainer): **move the transport off io_uring and onto epoll/kqueue,
remove the consumer's readiness eventfd + bridge thread, and make the sender a non-blocking pipelined
reactor.** Three intentions:

1. **Drop io_uring on the producer; use epoll (Linux) / kqueue (BSD) non-blocking `send`.** For this
   workload — one socket, large sequential block writes, single-threaded bgworker — epoll vs io_uring is
   a **performance wash** (the cost is the payload copy + the wire, not syscall submission). The switch
   is justified by **simplicity** (no ring lifetime / in-flight-SQE teardown; `send()` returning ⇒ bytes
   copied ⇒ buffer immediately reusable, vs io_uring pinning the buffer until the CQE), **robustness**
   (io_uring is disabled by seccomp in many hardened/container deployments — the current path already
   silently falls back to blocking there), and **portability**. It is **NOT** justified by a loopback
   throughput number, and you must not claim one.

2. **Remove the consumer TCP eventfd + per-async bridge thread.** Replace them with an **epoll fd**
   (owned by the source) that aggregates **{`sock_fd` (EPOLLIN|EPOLLRDHUP|EPOLLERR), a `timerfd`
   armed to the remaining stall budget}**, returned from `schedule()`. **Drop the cancel eventfd**; keep
   cancellation via `::shutdown(sock_fd, SHUT_RDWR)` (which makes the socket readable/errored → the
   epoll fd fires → the executor wakes → the recv loop observes `cancelled`). This deletes the
   `std::thread` spawned **per async wait** (a thread create/join per block under steady streaming) and
   one fd per stream — a real per-stream resource + teardown-latency win, **independent of throughput**.

3. **Non-blocking sender with K (GUC-configurable) blocks in flight.** Restructure the producer so
   later blocks' deform+serialize **overlaps** earlier blocks' send. Concretely: **exactly one send on
   the socket at a time** (TCP is a byte stream — never two concurrent sends), plus a **pool of K frame
   buffers** (`pg_clickhouse.tcp_send_inflight_blocks`, default 2, ≥1) so the producer may run ahead by
   up to K blocks — one draining + up to K−1 already serialized/queued — without serialize(N+i)
   clobbering an in-flight frame; the columnizer's publish callback becomes **non-blocking** (hand the
   serialized frame to a send reactor and return so the scan/deform loop proceeds), pumping the sender
   only when all K buffers are busy. Per turn the producer does exactly one of: **prepare** the next
   block (deform+serialize into a free frame buffer), **send/continue** the single in-flight frame, or
   **wait** on the socket for writability (TCP backpressure) when a frame is in flight and no buffer is
   free. (K is producer run-ahead depth. With plain copying `send` a frame frees as soon as the kernel
   copies it, so K's run-ahead benefit is bounded by `SO_SNDBUF`; with `MSG_ZEROCOPY` each frame stays
   pinned until its completion notification, so K buffers are what keep K blocks genuinely in flight —
   **K matters most on the zero-copy / real-NIC path.** Memory cost = K × max frame size per stream;
   bound and record it. **K=1 MUST degrade to the P1 single-in-flight behavior exactly.**)

### Honest scope (binding — pre-register exactly this)

- **Production is a real NIC; testing is loopback.** The Branch P2 throughput win is a **real-NIC
  expectation**, not a loopback success criterion.
- **On bare loopback, P2 is predicted NEUTRAL/NULL** and that is the honest, accepted result. Reasons,
  to be stated in the pre-registration: (a) the cross-process pipeline **already exists** via the
  32 MiB socket buffer (`SO_RCVBUF`) — `send(N)` already overlaps the consumer processing block N−1;
  P2 only adds *intra-producer* overlap of deform/serialize(N+1) with send(N); (b) on loopback `send`
  returns immediately when the socket buffer has room, so there is **no send latency to hide** when the
  producer is the bottleneck (~70% scan/deform-bound), and when the socket *does* fill the consumer is
  the bottleneck (~35% recv-copy) where producer overlap cannot raise throughput.
- **The binding P2 evidence that the overlap actually works is a NIC-emulation experiment**, not a bare
  loopback number: inject send latency/bandwidth limits on `lo` with `tc qdisc … netem delay <d> rate
  <r>` to create the send-latency regime a real NIC exhibits, and show the pipeline recovers throughput
  vs the single-in-flight baseline under that regime. Record exact qdisc params; remove the qdisc after;
  treat netem as a controlled instrument. A microbench that injects a fixed per-send delay is an
  acceptable complementary instrument. Bare loopback parity + the netem/microbench overlap win together
  are the convergent proof.
- C1 and P1: the floor and the expectation are **parity** (no regression vs today at W=8, within the
  noise band). Their real, claimable deltas are **mechanism** (epoll on the path, not io_uring; epoll-fd
  not eventfd+thread) and **resource** (threads/fds/teardown).

## Hardening pins (BINDING — an independent adversarial review of this spec produced these; read before implementing)

These resolve traps an autonomous agent would otherwise discover mid-implementation and bodge. Each
cites the code that forces it. Treat as acceptance criteria. A `[BLOCKER]` failure mode is a hang, a
silent `DIFF`, a use-after-free, or a CPU hot-spin — none of which a results-only loopback test catches.

- **H1 — The K-deep send-frame pool IS the deform-overlap; do NOT double-buffer the columnizer staging.**
  `tcp_serialize_block` (`shm_producer.c:1016-1075`) and `shm_arrow_encode_record_batch`
  (`shm_arrow.c:272-362`) copy out of `cz->bufs` (`shm_offload.c:725,805`) into `tcp_scratch`/`enc->body`
  **synchronously inside the publish call** — that copy is the snapshot boundary. So K private frame
  buffers + non-blocking publish + deferred send already let the scan/deform of N+1 refill `cz->bufs`
  while the kernel drains frame N (overlapping the full ~70% deform). K-deep-buffering `cz->bufs` buys
  nothing extra (the serialize copy still serializes the prepare turns) and is only needed to *defer* the
  serialize copy = a copy-budget change, **out of scope**. **Required proof:** a perf timeline showing
  page-reader/deform symbols of block N+1 executing concurrently with frame N's `sendmsg`/kernel drain;
  a send-frame-only-no-op (deform turn strictly after send-N completes) is a FAIL.

- **H2 — `[BLOCKER]` The returned fd is polled LEVEL-triggered; drain the inner set fully every wake or
  the executor hot-spins.** `Epoll::add` (`Epoll.cpp:42`) registers without `EPOLLET`; `IProcessor.h:209`
  mandates "read all available data before returning ASYNC". The source's epoll fd stays `EPOLLIN`-ready
  while ANY inner fd is ready. Invariant: before returning `Status::Async`, (1) `read()`-drain the
  timerfd if it fired, and (2) drive the recv loop until the **socket** returns EAGAIN (no longer
  `EPOLLIN`-ready) — **never re-async with the socket still readable**. (3) `EPOLLRDHUP`/`EPOLLERR` on a
  half-closed/dead producer is **not clearable by reading** → drive recv to terminal (EOS/throw), do NOT
  return Async on it (else permanent spin). Do this in **both** `onAsyncJobReady` (multi-threaded) **and**
  the `tryGenerate` early-path (single-threaded `PullingPipelineExecutor`, which never calls
  `onAsyncJobReady`). **Required proof:** a gtest on the single-threaded `PullingPipelineExecutor` (the
  `LoopbackThroughputMicrobench` harness) asserting **bounded** `work()`/`epoll_wait` invocations across
  a backpressured + EOS + half-close stream. A hot-spin is a FAIL even when results are correct.

- **H3 — `[BLOCKER]` Cancel-during-connect/handshake wake.** `ready_event_fd` is created only *after* the
  blocking handshake (`TcpStreamSource.cpp:512`); `ensureConnected` connects + handshakes on a blocking
  socket (`SO_RCVTIMEO` slices) before any epoll exists. Prove `::shutdown(SHUT_RDWR)` is the SOLE cancel
  wake across **all** phases: (a) connect-retry, (b) blocking handshake `recvAll`, (c) async-parked in
  the inner epoll. Register `sock_fd` in the inner epoll the instant it is valid. **If** `shutdown()`
  cannot be proven to wake a parked executor in the socket-not-yet-in-inner-epoll window, **KEEP a single
  cancel eventfd registered in the inner epoll** — do not delete it. A cancel-during-connect gtest is
  mandatory.

- **H4/H6/H7 — `[BLOCKER]` MSG_ZEROCOPY is synchronous today; making K real on the zero-copy path is
  REQUIRED (maintainer decision — this is the real-NIC lever, do NOT descope it).**
  `tcp_send_all_msg_zerocopy` ends every call with `tcp_zc_drain_until(last_seq)`
  (`shm_producer.c:818-819`) — it blocks until the kernel releases the buffer, so kept as-is **K is inert
  on the zerocopy path**. You MUST convert it to async completion: remove the per-send drain from the
  steady path; tag each pooled frame with its zerocopy seq(s); reclaim a frame to the free pool only when
  its completion is reaped (`tcp_zc_reap`, errqueue `POLLERR` folded into the send reactor); block
  (`tcp_zc_drain_until`) on a *specific* frame's seq **only** when that exact buffer is needed and none is
  free (the K-deep analog of MEMLOCK backpressure); bound **K × max_frame_size ≤ RLIMIT_MEMLOCK** (8 MiB
  here — cap K and log `D-HC-####` if a configured K would exceed it); on cancel/error **drain all
  in-flight seqs before freeing any pooled frame** (UAF otherwise — ties to H8). **Acceptance:** under the
  netem `rate` regime (H11), the `msg_zerocopy` K-sweep MUST show K>1 raising throughput vs K=1 (K=1 still
  drains-per-frame ≡ P1). A **flat K-curve for `msg_zerocopy` is a FAIL** — proof the synchronous drain
  was not deferred. (Loopback still shows the deferred-copy null per the host reality; the K>1 effect is
  the *pipelining* win, measured under netem, not a copy elimination.)

- **H5 — `[BLOCKER]` EOS-before-close drain.** With non-blocking publish + K queued frames,
  `shm_producer_signal_eos` must enqueue the EOS frame **after** all data frames, then **synchronously
  pump the reactor until all K frames + EOS are fully written to the kernel** (and, for zerocopy,
  completions reaped) **before** returning / setting `eos_published`. `producer_cleanup` /
  `shm_producer_destroy` MUST NOT `close(tcp_conn_fd)` until the reactor pool is empty — else the consumer
  sees `SHM_PRODUCER_DEATH_BEFORE_EOS` / a missing-blocks `DIFF`. Mandatory multi-block-then-EOS gtest.

- **H8 — `[BLOCKER]` New-reactor teardown (`producer_cleanup`).** P1 removes the ring teardown; P2 adds a
  producer-side epoll fd, K pool buffers, and an in-flight frame's `(iovec, offset)` state.
  `producer_cleanup` MUST explicitly `close()` the new epoll fd (+ any producer timerfd) — a leaked
  per-stream fd directly contradicts C1's "fewer fds" win — and for zerocopy reap completions before the
  context-managed pool buffers are freed (UAF). Record per-stream producer fd count before/after.

- **H9 — Coalesce scope + iovec partial-send.** Build a 2-entry `iovec` and issue **one**
  `sendmsg`/`writev` per data frame for the bespoke header+payload (`shm_producer.c:1110-1112`) **and**
  the Arrow meta+body (`:1003-1005`). Handshake, schema, and EOS stay single sends. Use `writev`/`sendmsg`,
  **not** `TCP_CORK`/`MSG_MORE` (extra syscall + uncork-latency risk). Partial sends MUST advance the
  iovec (consume whole + partial entries by bytes already sent), not a single pointer; unit-test all three
  boundaries (mid-header, exactly-at-boundary, mid-payload) with 1-byte-at-a-time forced sends. Reconcile
  the older "two sends per block remain in order" phrasing: the per-block pair is now ONE scatter-gather
  write; "in order" refers to frame-to-frame ordering across blocks.

- **H10 — Producer waits with raw `epoll`, never PG's `WaitEventSet`.** `WaitEventSet`/`WaitLatchOrSocket`
  cannot wait on the `MSG_ZEROCOPY` errqueue `POLLERR`; raw epoll is also consistent with the existing
  raw `poll()` send path. **Every** `epoll_wait` uses a bounded ~100ms timeout and re-checks
  `CHECK_FOR_INTERRUPTS()` + `origin_backend_dead()` each slice — **never `epoll_wait(-1)`** (a bgworker
  must observe SIGTERM/postmaster-death within one slice; prove in the cancel-mid-send test).

- **H11 — SO_SNDBUF and the netem regime are the experiment's validity hinge — pin them.** SO_SNDBUF
  sizes the *socket* throughput ceiling (≈ SO_SNDBUF/RTT for copying-send); K sizes producer *deform*
  run-ahead — distinct knobs. SO_SNDBUF is hard-set 32 MiB (`shm_producer.c:885`); with that + `delay`-only
  netem the buffer swallows the latency and K looks inert. **Required:** size SO_SNDBUF to the emulated
  link's bandwidth-delay-product (record effective `getsockopt` value + the wmem_max=64 MiB cap), and run
  the K-sweep under netem **`rate`** (bandwidth-limited, small BDP — the regime where the socket fills and
  K-in-flight gates throughput), not delay-only. Show the K knee moves with BDP (rate×delay) — that is the
  mechanism proof. Primary regime: `netem delay 50us rate 3gbit` (representative same-AZ cloud NIC); plus
  `delay 500us rate 10gbit` sensitivity. Confirm one-send-in-flight + EPOLLOUT re-arm + SO_SNDBUF≥BDP
  saturates the emulated link (one send in flight is NOT the ceiling once SO_SNDBUF≥BDP).

- **H12 — netem hygiene.** `sudo tc qdisc add dev lo root netem …`; ALWAYS `sudo tc qdisc del dev lo root`
  in a trap/finally even on failure; assert `tc qdisc show dev lo` is `noqueue` before any **bare-loopback**
  parity measurement. netem on `lo` perturbs **all** loopback traffic (PG↔CH control plane included) — never
  measure parity with a qdisc installed; a leaked qdisc poisons every later number.

- **H13 — Complete the P1 delete/rename set + prove zero residual.** Beyond `shm_producer.c`: update
  `src/include/shm_offload.h` (remove `PGCH_TCP_SEND_IOURING`, renumber the enum, change the GUC default
  to the new method), `src/include/shm_producer.h` (retire/rename the `iouring_sends` out-param +
  `shm_producer_set_tcp_send_method` doc), and `shm_worker.c` (`:107` field, `:519`, the `tcp-send` LOG
  `:611-627` — report the new method name + a counter proving the epoll path ran, e.g. `epoll_sends>0`),
  and remove the `Makefile:44-51` liburing detection block (the `-DPGCH_USE_LIBURING` define + `-luring`
  link) so `-luring` never links regardless of `/usr/include/liburing.h`. Acceptance: `grep -nE
  'io_uring|iouring|IOURING|PGCH_USE_LIBURING' src/ Makefile*` returns nothing, and `ldd`/`nm` on the
  built `.so` show no `-luring` / `io_uring_*` symbol.

- **H14 — K=1 means single-frame-in-flight *semantics* (parity), not the literal P1 two-send blocking
  code.** At K=1 the reactor still uses the coalesced `sendmsg` + non-blocking publish; "degrades to P1"
  means no pipelining overlap and identical results, measured as parity — do NOT add a K==1 special case.

- **H15 — Thread-elimination proof is a gtest hook, not `/proc` on the live server.** Assert (in an
  isolated single-source gtest) the source spawns **zero** `std::thread`s across a multi-block async drain
  (static counter / unchanged `/proc/self/task`). `clone` / `/proc/<ch_pid>/task` counts on the live W=8
  server are corroborating-only (too noisy to isolate). Likewise prove the per-stream fd delta in a
  controlled count, not the noisy server.

- **H16 — Log the io_uring→`SEND_ZC` forfeit.** Removing liburing forfeits `IORING_OP_SEND_ZC` (the
  `Makefile` comment notes liburing was kept as its substrate) — a cleaner real-NIC zero-copy send than
  `send(MSG_ZEROCOPY)`+errqueue (CQE completion vs per-send `recvmsg`). Record this tradeoff in the
  `D-HC-####`, justify `MSG_ZEROCOPY` as the retained lever (note its per-send errqueue cost), and name
  "re-introduce io_uring solely for `SEND_ZC` if it proves the dominant real-NIC lever" as the follow-up.

- **H17 — Parity baselines measured at best config, both directions.** The io_uring baseline (P1) and the
  K=1 baseline (P2) MUST be the default, un-degraded config (`PGCH_TCP_IOURING_ENTRIES=8`,
  `shm_log_stream_stats` off, no extra instrumentation), measured FRESH on the immediately-prior committed
  SHA, identical flags/caps/sysctls. Parity = relative diff ≤ `max(5%, 1 stdev)` in **both** directions —
  matching a degraded baseline is not parity.

## The three branches (and how they converge)

Executed **C1 → P1 → P2** (land the low-risk independent win first, then the parity-preserving refactor,
then the perf change last so any regression is isolated to one commit):

### Branch C1 — Consumer epoll-fd readiness (implements your task **b**)

**Scope.** In `TcpStreamSource`: make `schedule()` return an **epoll fd** owned by the source, into which
are registered `sock_fd` (`EPOLLIN|EPOLLRDHUP|EPOLLERR`) and a `timerfd` (`timerfd_create`/`settime`)
armed to the **remaining stall budget** whenever the source goes async; re-arm the timerfd on each
async entry. **Delete** `ready_event_fd`, `async_wake_stop_fd`, `async_wake_bridge_stop`,
`async_wake_thread`, and the functions `startAsyncWakeBridge` / `asyncWakeBridgeLoop` /
`joinAsyncWakeBridge` / `requestAsyncWakeBridgeStop` / `wakeReadyEvent`. On async wake (in both
`onAsyncJobReady` and the `tryGenerate` early-path, mirroring the existing idempotent discipline): if the
timerfd fired, `read()` it to disarm (level-triggered hygiene — do not leave it readable) and enforce the
stall budget (`SHM_PRODUCER_STALL`); otherwise resume the non-blocking recv. **Cancellation keeps using
`::shutdown(sock_fd, SHUT_RDWR)`** — drop the cancel-eventfd write. Both wire formats (`Bespoke`, `Arrow`)
and both `async`/blocking modes must still work; the blocking-recv path (`recvAll` with `SO_RCVTIMEO`) is
unchanged.

**Acceptance.**
- **Contract verified:** re-read `PollingQueue.{h,cpp}` and confirm a `schedule()`-returned epoll fd is
  driven correctly in **both** the single-threaded `PullingPipelineExecutor` (`work()` direct, no
  `onAsyncJobReady`) and the multi-threaded path. Pin the finding in a code comment + the log.
- **Correct:** passes every oracle in TCP and Arrow-TCP mode, no new `DIFF`; clean teardown (no leaked
  fds/threads) on success/error/**cancel**; the stall-timeout still fires (a deliberately stalled
  producer raises `SHM_PRODUCER_STALL` within the budget); EOS/producer-death/cancel paths intact.
- **Resource delta proven (the win):** **zero `std::thread` creations** on the streaming path (was one
  per async wait) and **fewer fds per stream** (eventfd + stop-eventfd removed; one epoll fd + one
  timerfd added — net per-stream fd count recorded). Prove thread-creation elimination via
  `/proc/<ch_pid>/task` count under load, a clone/thread-create counter (`perf stat -e
  'syscalls:sys_enter_clone*'` or `task-clock` thread churn), or a gtest hook. Measure teardown latency
  vs before.
- **No regression** vs today at W=8 (within noise) on ClickBench + TPC-H, TCP and Arrow-TCP.

### Branch P1 — Producer epoll/kqueue non-blocking send, io_uring removed (implements your task **a**)

**Scope.** Replace the io_uring send with a **non-blocking `send()` + readiness-wait** loop, **one send
in flight** (NO pipeline yet — parity). Set the connected socket `O_NONBLOCK`; on `EAGAIN`/`EWOULDBLOCK`,
wait for writability via **epoll** (Linux) with a timeout slice (~100ms) that re-checks
`CHECK_FOR_INTERRUPTS()` + `origin_backend_dead()` (replacing both the io_uring `wait_cqe_timeout` slice
and the existing blocking-path `poll(POLLOUT)`). Provide the readiness wait behind a tiny internal helper
so **kqueue** (BSD/macOS) is a drop-in; if you do not have a BSD host to test, implement epoll for Linux,
structure the helper for kqueue, and keep `tcp_send_all_blocking` as the universal fallback (log the
decision `D-HC-####`). **Delete** `tcp_send_all_iouring`, `tcp_iouring_ensure`,
`PGCH_TCP_IOURING_ENTRIES`, the `tcp_ring*` fields + queue init/exit teardown, and the **liburing build
dependency** (find it: `PGCH_USE_LIBURING` guard + the Makefile/PGXS link). Update the
`pg_clickhouse.tcp_send_method` enum: drop `IOURING`; the default becomes the epoll non-blocking send (name it
e.g. `epoll`/`async`); keep `blocking`; **keep `msg_zerocopy`** (it is `send(MSG_ZEROCOPY)` + errqueue,
not io_uring — it survives and remains the **real-NIC zero-copy-send lever**; ensure it composes with the
non-blocking `EAGAIN`/epoll loop). Log the enum change `D-HC-####`.

**Acceptance.**
- **Correct:** passes every oracle (TCP + Arrow-TCP), no new `DIFF`; clean teardown on
  success/error/cancel; `msg_zerocopy` mode still functions (its errqueue accounting +
  `SO_EE_CODE_ZEROCOPY_COPIED` proof unchanged).
- **Mechanism proven:** profile/strace shows the producer send path is now `sendto`/`send` +
  `epoll_wait` (under backpressure) and **no `io_uring_enter`/`io_uring_setup`**; liburing no longer
  linked (`ldd`/`nm` on the built `.so`).
- **Parity:** no regression vs the io_uring baseline at W=8 (within noise) on ClickBench + TPC-H, both
  wires. (Expected ≈ 0 by design — io_uring was used synchronously; pre-register parity. If you observe
  a *win*, root-cause it — likely the removed per-send submit/CQE overhead or simpler buffer reuse — and
  prove it; do not assume.)
- **Robustness note (claimable, not measured here):** record that the epoll path has no seccomp-disabled
  fallback degradation, unlike io_uring.

### Branch P2 — Pipelined sender, K (GUC-configurable) blocks in flight (implements your task **c**)

**Scope.** On the P1 epoll substrate, overlap deform/serialize(N+1) with send(N). Requirements:
- **Exactly one send on the socket at a time; never two concurrent sends** (byte-stream ordering). "K
  blocks in flight" means a **K-deep frame-buffer pool** (`pg_clickhouse.tcp_send_inflight_blocks`,
  default 2, ≥1): one frame draining + up to K−1 serialized/queued. For bespoke this is K `tcp_scratch`
  buffers; for Arrow it is K nanoarrow encoder bodies (or K encoders) — the body/message buffers are
  currently single + reused, so this is the real change. Per-stream serialize memory = K × max frame
  size (accepted; bound it and record it). **K=1 must degrade to the P1 single-in-flight path exactly.**
- **Coalesce the per-block header + payload into ONE `sendmsg`/`writev` (iovec) per frame**, not two
  `send`s — with `TCP_NODELAY` already set, two separate writes emit a tiny header segment then the
  payload (an extra syscall + a runt packet + possible interaction with the peer's delayed-ACK). One
  scatter-gather write per frame is the correct, lower-latency shape. (Pre-register this; it is a
  mechanism change the profile must show — fewer `sendto`/`send` syscalls per block.)
- **Non-blocking publish.** The columnizer's publish callback (`columnizer_publish_block` →
  `shm_producer_publish` → `tcp_/arrow_publish_block`) must hand the serialized frame to a **send
  reactor** and return so the scan/deform loop proceeds, instead of blocking until the send completes.
  Per turn the producer does exactly one of {prepare next block into a free frame buffer; submit/continue
  a non-blocking send of a ready frame; `epoll_wait(EPOLLOUT)` for backpressure when a send is in flight
  and no frame buffer is free}. Partial sends resume from the saved offset. Both sends per block (header
  then payload / meta then body) remain in order on the one in-flight stream.
- Preserve `CHECK_FOR_INTERRUPTS()` + `origin_backend_dead()` during any `epoll_wait`; preserve clean
  teardown (a frame may be mid-send at cancel/error — drain or abandon safely, no use-after-free of the
  pooled buffers). EOS ordering after the last data frame is preserved.
- **Phase accounting:** the per-phase stopwatch (`shm_log_stream_stats` READ/DEFORM/PUBLISH/SERIALIZE/
  send) now measures *overlapped* stages — make the accounting express overlap honestly (e.g. send-wait
  wall vs serialize wall can no longer simply sum). Update the instrument and say what it now means.

**Acceptance.**
- **Correct:** passes every oracle (TCP + Arrow-TCP), no new `DIFF`; byte-stream integrity under the
  pooled/pipelined sender (hammer multi-block streams, partial sends, backpressure, EOS,
  producer-death/cancel mid-send); clean teardown.
- **Overlap proven (the binding evidence):** under a **netem-emulated NIC regime** on `lo` (and/or a
  fixed-send-delay microbench), the pipeline shows a throughput recovery vs the P1 single-in-flight
  baseline, with the mechanism shown (producer send-wait wall overlapped by deform/serialize wall;
  profile shows the scan/deform of N+1 progressing while frame N is in flight). ≥3 independent classes
  converge.
- **Bare-loopback honesty:** at W=8 on bare loopback, **parity is the accepted result** (pre-registered
  null) — report it as such; do **not** dress a within-noise wiggle as a win.
- ≥3 evidence-based iterations logged: a **K sweep** to find the run-ahead knee (K∈{1,2,4,8,…}) under
  bare loopback **and** under netem, reporting the chosen default K with evidence; the K interaction with
  `SO_SNDBUF` and with `MSG_ZEROCOPY` pinning (see hardening pins H4/H6/H7). **Overlap-mechanism note
  (binding — see H1):** the K-deep *send-frame* pool (K `tcp_scratch` / K nanoarrow bodies) IS the
  deform-overlap mechanism — because `tcp_serialize_block` / `shm_arrow_encode_record_batch` copy out of
  the columnizer staging **synchronously inside publish**, that copy is the snapshot boundary, so once
  block N is serialized into its private frame the scan/deform loop refills the staging for N+1 while the
  kernel drains frame N. Do **NOT** double-buffer the columnizer `cz->bufs` staging — that is only needed
  to *defer* the serialize copy (a copy-budget change, out of scope per the §"changes the readiness
  mechanism only" rule) and buys nothing here. Each iteration pre-registers a regime + predicted
  magnitude.

## Fidelity policy

The transport/scheduling change **must not change results.** Outputs in TCP, Arrow-TCP, SHM-adopt,
SHM-copy must agree cell-for-cell within the *same* documented bounds; the harness `cmp.py` verdicts
(`exact`/`approx`/`topN`/`empty-both`/`DIFF`) must be identical to the pre-change baseline. Any **new**
`DIFF` is a transport/scheduling bug that **blocks the branch**, not an allowed deviation. Re-derive any
non-`exact` verdict independently before accepting it. Edge cases to hammer: empty blocks, `count()` (no
columns), subset projection, multi-source joins (each relation its own stream/socket), long/empty
strings, Nullable, Decimal scale, DateTime64 precision, and the **stall / EOS / producer-death / cancel**
paths (the readiness machinery is exactly what those exercise).

## NON-NEGOTIABLE EVIDENCE STANDARD

Identical to phase2 (read `dev/hotcold/phase2/PROMPT.md` §"NON-NEGOTIABLE EVIDENCE STANDARD"); the
binding points restated for this phase:

1. **No claim** unless **≥3 INDEPENDENT instrument classes converge** (direction + rough magnitude). One
   source is a lead, not a conclusion.
2. **Independent = different layer that can fail differently.** Use, at minimum:
   - **End-to-end timing** — `dev/wsweep-report/wsweep_split.sh` `measure()`/`stats()`: median of **N≥5
     warm** runs + min/max/stdev, under the shared cgroup `cpu.max = W*period` cap, **W=8**, ClickBench
     `hits` 10M + TPC-H `tpch_sf10`. Re-measure **every** compared mode FRESH on the SAME binary in the
     SAME session.
   - **Producer phase split** — the in-code stopwatch (READ/DEFORM/PUBLISH/SERIALIZE/send), now made to
     express overlap honestly; cross-checked against `/proc` `off_prod` and CH `query_log` consumer CPU.
   - **PMU (`perf stat`)** on the producer workers + CH consumer threads (Graviton aarch64: `cycles` or
     `task-clock`): context-switches/syscalls (epoll vs io_uring; thread-create churn for C1), cycles/IPC
     for parity.
   - **Profiles (perf/flamegraph)** showing the mechanism: producer `sendto`+`epoll_wait` and **no
     `io_uring_enter`** (P1); consumer **no per-async `clone`/thread spawn** and the epoll-fd path (C1);
     deform(N+1) overlapping send(N) (P2). Absence of the old path is necessary, not sufficient — pair
     with a counter.
   - **Isolated microbenchmarks / gtests** — extend `ClickHouse/.../SharedMemorySource/tests/`
     (`gtest_tcp_stream_source.cpp` has `LoopbackThroughputMicrobench`): add a thread-create-count assert
     (C1), an epoll-readiness liveness test (C1: stall fires; cancel via shutdown wakes), a
     send-with-injected-delay pipeline microbench (P2). PG-side: a small producer send-loop timer.
   - **NIC emulation** (P2, binding): `tc qdisc add dev lo root netem delay <d> [rate <r>]`; record params;
     remove after.
3. **PRE-REGISTER before measuring** (`dev/hotcold/phase3/00-PRE-REGISTRATION.md`): mechanism + predicted
   magnitude. For this phase the headline predictions are **C1 = parity + thread/fd elimination**,
   **P1 = parity + io_uring-off mechanism**, **P2 = bare-loopback null, netem-regime overlap win**. A
   prediction/result mismatch is a finding to investigate, never to rationalize.
4. **If sources disagree, you have NO result.** Root-cause first. Effect must exceed the noise band
   (**relative diff ≤ `max(5%, 1 stdev)`**); if ≤ noise, say exactly that.
5. **Control variance:** idle host (`uptime` < 0.5 pre-sweep), fixed datasets, warm/cold stated,
   identical build flags + DB tuning + CPU cap across modes, **sysctl / socket-buffer settings identical
   and recorded** (`net.core.{w,r}mem_max`, `SO_SNDBUF`/`SO_RCVBUF`, and any `netem` qdisc state), exact
   commands + env + SHAs (`dev/hotcold/phase3/10-REPRODUCTION.md`).
6. **BANNED:** "should be faster", "likely", unproven causal stories, single-run numbers, cherry-picking,
   claiming an epoll/pipeline win from end-to-end timing alone with no mechanism source, claiming a
   loopback throughput win for P2 (pre-registered null), declaring done without the correctness oracle.

## Auditable methodology log

Maintain `dev/hotcold/phase3/METHODOLOGY-LOG.md`, append-only, same `L####` template as phase2
(continue the global L-number sequence if these logs share it; otherwise start phase3-local and say so).
Log failed/null experiments too — the P2 loopback null that confirms the prediction is a valuable entry.

## PER-BRANCH LOOP

For each branch (C1 → P1 → P2) and each P2 iteration:
a. **Restate** scope + acceptance. **Pre-register** hypotheses + predicted magnitudes. Write the holistic
   end-to-end note in the log.
b. **Implement** per spec; write/extend the oracle + tests first where it fits (the C1 liveness/teardown
   gtest, the thread-create-count assert, the P2 injected-delay pipeline microbench, `streamed_table`
   parsing). Keep diffs minimal and reviewable.
c. **Correctness gates** — `test/shm/verify_offload.sh` (+ `verify_columnar.sh`, `verify_visibility.sh`)
   and the harness offload/liveness/correctness oracles in the touched mode(s). The offload oracle must
   prove the heavy fragment offloaded over the transport (right counter ≥1), not a base scan. **If red,
   fix and repeat — never measure on red.**
d. **Measure** with **≥3 independent classes**; produce an evidence log with **RAW** outputs (commands,
   env, numbers, medians+spread, counter/PMU tables, profiles, prediction-vs-observation). Store under
   `dev/hotcold/phase3/` mirroring phase1/2 layout (`results/<mode>/<bench>/{cells.tsv,RESULTS.md}`,
   `evidence/<artifact>.txt`, `REPORT.md`), plus the cross-mode tables (C1: thread/fd/teardown before-vs-
   after; P1: epoll-TCP vs io_uring-TCP at W=8; P2: the K-sweep (incl. K=1 single-in-flight baseline) on
   bare loopback **and** under netem).
e. **INDEPENDENT ADVERSARIAL REVIEW** (below).
f. If the review raises blocking findings OR evidence does not converge OR an acceptance criterion is
   unmet OR (P2) you have not completed 3 iterations → loop. Else mark green, proceed.

## Independent adversarial review (after EVERY step)

Launch a **subagent with clean context** that did NOT write the code, instructed to **attack** the work
assuming the implementer was over-optimistic, along four angles. **The reviewer MUST explicitly verify
every applicable hardening pin (H1–H17) is satisfied** — in particular the ones a results-only test
misses: H2 (no executor hot-spin — assert bounded `work()` invocations, not just correct output), H4/H6/H7
(MSG_ZEROCOPY drain deferred so K>1 raises zerocopy throughput under netem — a flat K-curve is a FAIL),
H5 (EOS-before-close), H8 (no leaked producer fd / no UAF of zerocopy-pinned frames), H1 (the overlap is
real deform-vs-send, not serialize-only):

- **Correctness:** does the offloaded plan still compute the query over the new readiness/send machinery?
  Stall timeout still fires? Cancel via `shutdown()` actually wakes the epoll-fd and tears down with no
  leak? Byte-stream integrity under the pipelined pooled sender (partial sends, backpressure, EOS order,
  producer-death mid-send)? Is the `query_log` oracle proving the heavy fragment offloaded + the right
  mode, or a base scan? Re-verify the `PollingQueue` epoll-fd contract holds in BOTH executor paths.
- **Fidelity:** any new `DIFF` vs the pre-change baseline, in any mode? Re-derive independently.
- **Performance & mechanism:** ≥3 converging classes, pre-registration honored, noise vs effect
  quantified, right baseline measured FRESH under identical caps + sysctls. **Attack the mechanism
  claims specifically:** is io_uring truly gone from the producer (no `io_uring_enter`, liburing
  unlinked) — P1? Is the consumer per-async **thread spawn truly eliminated** (not merely reduced) and
  the eventfd/stop-fd gone — C1? For **P2**, is the bare-loopback result honestly reported as a
  **null/parity** (not a dressed-up wiggle), and is the netem/microbench overlap win real (mechanism:
  deform(N+1) overlapping send(N) in the profile, not just an end-to-end number)? Is "2 in flight"
  actually **one send in flight + one prepared** (NOT two concurrent socket sends that could reorder
  bytes)?
- **Holism:** does the change help/neutralize the *whole* dataflow at W=8 multi-stream (not a single
  microbench)? Memory peak / lifetime correct (frame-buffer pool bounded, dropped when done, no
  use-after-free on cancel mid-send)? Teardown latency improved, not regressed?

Record the verdict + blocking findings in `dev/hotcold/phase3/evidence/ADVERSARIAL-REVIEW.md`. A
branch/iteration is **green only after the review passes.**

## Repo map (verified starting points — re-verify, don't trust blindly)

- **Producer (PG, `src/`):** `shm_producer.c` — `tcp_send_all` + `tcp_send_all_iouring` /
  `tcp_iouring_ensure` / `tcp_send_all_blocking` / `tcp_send_all_msg_zerocopy` / `tcp_zc_reap` /
  `tcp_zc_drain_until`; `PGCH_TCP_IOURING_ENTRIES`, `p->tcp_ring*`, `producer_cleanup`
  (`io_uring_queue_exit`); `tcp_serialize_block` / `tcp_publish_block` / `arrow_publish_block` /
  `tcp_accept_conn` / `tcp_producer_setup`; `p->tcp_scratch`, `p->tcp_conn_fd`. `shm_arrow.c` (the
  nanoarrow encoder `body`/`message` buffers — double-buffer for P2). `shm_offload.c` (`pg_clickhouse.tcp_send_method`
  enum + GUC). The build dep: `PGCH_USE_LIBURING` guard + Makefile/PGXS link (remove). The
  columnizer/publish path: `shm_offload.c` (`columnizer_publish_block` → `shm_producer_publish`),
  `shm_page_reader.c` (the scan/deform loop that calls publish).
- **Consumer (ClickHouse, `ClickHouse/src/`):**
  `Storages/SharedMemorySource/Source/TcpStreamSource.{cpp,h}` — `schedule()`, `prepare()`,
  `onAsyncJobReady()`, `onCancel()`, `tryGenerate()`, `recvAll`, `tryRecvInto`, `tryRecvBlock` /
  `tryRecvArrowMessage`; the to-delete `ready_event_fd` / `async_wake_stop_fd` / `async_wake_thread` /
  `startAsyncWakeBridge` / `asyncWakeBridgeLoop` / `joinAsyncWakeBridge` / `requestAsyncWakeBridgeStop` /
  `wakeReadyEvent`; `ensureConnected` (socket setup, `O_NONBLOCK`, `SO_RCVBUF`). Executor contract:
  `IProcessor::scheduleForEvent()` (`IProcessor.cpp:67-70`, `IProcessor.h:209`) →
  `Processors/Executors/ExecutorTasks.cpp:176,227` → `PollingQueue.{h,cpp}` / `Epoll.cpp:42`
  (level-triggered, no `EPOLLET`) and its callers in `PipelineExecutor` / `ExecutingGraph` (verify single-
  vs multi-threaded async handling — see hardening pins H2/H5 in this doc).
- **Tests / oracles:** `test/shm/verify_offload.sh` (+ `verify_columnar.sh`, `verify_visibility.sh`);
  gtests under `ClickHouse/src/Storages/SharedMemorySource/tests/` (esp. `gtest_tcp_stream_source.cpp`).
- **Harness / conventions:** `dev/wsweep-report/wsweep_split.sh`; mirror
  `dev/hotcold/phase{1,2}/{00-PRE-REGISTRATION,10-REPRODUCTION,REPORT}.md`, `evidence/`,
  `results/<mode>/<bench>/{cells.tsv,RESULTS.md}`; decision log `dev/hotcold/DECISIONS.md` (`D-HC-####`);
  adversarial-review format `dev/hotcold/phase2/evidence/ADVERSARIAL-REVIEW.md`.
- **Environment of record:** AWS Graviton aarch64, 32c/61 GiB, dedicated/idle; PG 18 (`:5432`); CH build
  `reldeb` (resolve the **live** pid from `ss -ltnp`); shared cgroup-v2 `cpu.max` cap; build/restart/
  sysctl recipe in `dev/hotcold/phase1/10-REPRODUCTION.md`. Confirm + re-record SHAs at pre-registration.

## Deliverables

1. The work landed as **small, reviewable commits** on `streamed-table-shm-offload` (pg_clickhouse) and
   `streamed_table` (ClickHouse) — C1 (consumer epoll-fd), P1 (producer epoll send + io_uring/liburing
   removed), P2 (pipelined sender), tests, and the observability updates — each green before commit.
2. Green correctness gates (regression suite + harness oracles) in TCP and Arrow-TCP modes.
3. `dev/hotcold/phase3/00-PRE-REGISTRATION.md` (mechanism + predicted magnitude; the explicit
   C1-parity+resource / P1-parity+mechanism / P2-loopback-null+netem-win predictions).
4. Evidence log with RAW outputs across ≥3 classes: W=8 ClickBench + TPC-H `cells.tsv`/`RESULTS.md`,
   producer phase split (overlap-honest), PMU tables, perf profiles, microbench, and the cross-mode
   tables (C1 thread/fd/teardown before-vs-after; P1 epoll vs io_uring; P2 K-sweep vs single-in-flight
   K=1, bare loopback **and** netem) — each with prediction-vs-observation.
5. `dev/hotcold/phase3/METHODOLOGY-LOG.md` — complete append-only log (≥3 P2 iterations, incl. nulls).
6. `dev/hotcold/phase3/evidence/ADVERSARIAL-REVIEW.md` with a passing verdict per step.
7. Decision-log entries (`D-HC-####`): the io_uring→epoll/kqueue switch + its justification (simplicity/
   robustness/portability, not loopback speed); the `pg_clickhouse.tcp_send_method` enum change; dropping the
   liburing dep; the consumer eventfd+bridge-thread → epoll-fd{socket,timerfd} + cancel-via-shutdown
   change; the `pg_clickhouse.tcp_send_inflight_blocks` GUC + default K (justified by the K-sweep) +
   whether the columnizer staging is double-buffered; the header+payload `sendmsg`/`writev` coalesce;
   any bounded deviation.
8. `dev/hotcold/phase3/10-REPRODUCTION.md` (exact commands, env, SHAs, GUCs, sysctls, netem qdisc params,
   cgroup + port resolution, build/restart recipe).
9. A `REPORT.md` per branch with the GREEN verdict, and a closing note: how much per-stream resource /
   teardown latency C1+P1 removed, the P1 parity result + io_uring-off mechanism, and the P2 honest
   verdict (bare-loopback null, netem-regime overlap win, real-NIC expectation).

## Definition of done (all required)

- **Branch C1 green:** consumer `schedule()` returns an epoll fd {socket, timerfd}; eventfd + stop-eventfd
  + per-async bridge thread deleted; cancel via `shutdown()`; correct in both executor paths, no new
  `DIFF`, clean teardown, stall-timeout fires; **zero streaming-path thread creations** and fewer fds
  per stream proven; no W=8 regression; evidence converges; review passed.
- **Branch P1 green:** producer send on epoll (kqueue-structured) non-blocking `send`, one in flight;
  io_uring send + ring + liburing dependency removed; `msg_zerocopy` still works; no new `DIFF`, clean
  teardown; mechanism proven (no `io_uring_enter`, liburing unlinked); parity at W=8 both wires; evidence
  converges; review passed.
- **Branch P2 green:** non-blocking pipelined sender, exactly one send on the socket + a K-deep
  frame-buffer pool (`pg_clickhouse.tcp_send_inflight_blocks`, K=1 ≡ P1), header+payload coalesced into
  one `sendmsg`/`writev`, non-blocking publish; **async `MSG_ZEROCOPY` completion implemented (H4/H6/H7) so
  K>1 raises `msg_zerocopy` throughput under netem (a flat K-curve is a FAIL)**; no executor hot-spin (H2,
  bounded `work()` invocations); byte-stream integrity + clean teardown (cancel mid-send drains in-flight
  zerocopy seqs before freeing pooled frames — no UAF, no leaked producer fd, H8) proven; EOS-before-close
  drain (H5); the chosen default K justified by the K-sweep; the overlap win demonstrated under
  netem/injected-delay with mechanism shown (deform(N+1) overlapping send(N), not merely serialize, H1);
  bare-loopback result honestly reported as the pre-registered **parity/null**; ≥3 iterations logged;
  evidence converges; review passed.
- Every claim backed by ≥3 independent converging instruments; every deviation logged; reproduction
  recorded.
- All work committed as small, reviewable patches on the two offload branches — each commit green, no
  mega-diff, history is the audit trail.
