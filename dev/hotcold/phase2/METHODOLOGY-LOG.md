# Hot-Cold Phase 2 — Methodology log (append-only, auditable)

Every experiment/change/measurement gets an entry, in order, never edited after the fact (corrections are
new entries referencing the old). Template in PROMPT.md §"Auditable methodology log". This is a primary,
graded deliverable. Null results that killed a hypothesis are logged too.

---

### L0001 — Phase 2 setup: orient, verify infra, fresh baseline  [branch 0]  [iteration 0]  2026-06-26
- **Goal / hypothesis:** Establish the green starting state before any change, and confirm the binding
  constraints from the three feasibility reviews are correctly absorbed. No performance claim yet.
- **What I did:**
  - Read all binding docs: `PROMPT.md`, `evidence/PROMPT-FEASIBILITY-REVIEW{,-v2,-v3}.md`,
    `evidence/FINDINGS-format-and-sequencing.md`, `phase1/{REPORT,10-REPRODUCTION}.md`, `DECISIONS.md`.
  - Verified live infra: PG18 `:5432` (pid 2024103), CH reldeb v26.6.1.1 `:21002` (live pid 2205376),
    CH build dir `build/reldeb` intact. liburing-dev **installed** (2.14, `/usr/include/liburing.h`).
    nanoarrow NOT vendored (Branch A). Arrow contrib 23.0.1. Sysctls `net.core.{w,r}mem_max`=64 MiB set.
  - Read the code I will edit: `shm_producer.c` (TCP path), `TcpStreamSource.{cpp,h}`, `TcpFrame.h`,
    `StorageShm.cpp` dispatch. Dispatched an Explore agent that mapped the async-source contract
    (`PollableShmSource` is the precedent: eventfd + async-wake bridge + `prepare/schedule/onAsyncJobReady`),
    confirmed CH io_uring (`IOUringReader`) is **file-only + NOT epoll-integrated** (dedicated monitor
    thread; zero `IORING_OP_RECV` in tree) — so the consumer's measurable win is the **async overlap**, with
    io_uring as substrate.
  - Wrote `00-PRE-REGISTRATION.md` (all three branches: mechanism + predicted magnitude + copy-budget predictions).
- **How I did it:** read tools + `git rev-parse` + `ss`/`ethtool`/`sysctl` + one Explore subagent.
- **How verified (≥3 classes not applicable to a setup step; correctness gate only):**
  - Correctness oracle (the floor every branch must keep): `CH_BIN=…/reldeb/programs/clickhouse PG_DB=shmdemo
    TRANSPORT=tcp bash test/shm/verify_offload.sh` → **PASS=137 FAIL=0** on green HEAD (exit 0). Log at
    `/tmp/baseline_verify_tcp.log`.
- **Result (RAW):**
  - `verify_offload TRANSPORT=tcp`: `PASS=137  FAIL=0  ALL CHECKS PASSED  EXIT=0`.
  - SHAs: pg_clickhouse `3ac38402f917`, ClickHouse `2847326da921`. Kernel `7.0.0-1006-aws aarch64`, 32c.
  - `ethtool -g lo` → Operation not supported; ENA `TCP data split: n/a` (re-confirmed binding: no kernel zc recv).
- **Interpretation:** Starting state is green and matches the feasibility reviews. The current bespoke-TCP
  path is the parity target. No deviation.
- **Learnings:** (1) Consumer async should mirror `PollableShmSource` exactly (eventfd readiness). (2) For
  the consumer, the socket fd is itself epollable, but the stall/producer-death budget makes the
  PollableShmSource bridge pattern the safe precedent. (3) io_uring on the consumer needs a completion
  thread that signals the readiness eventfd (ring not epoll-integrated). (4) Producer io_uring send is
  self-contained and is the SEND_ZC substrate for Branch B.
- **Verdict:** DONE (setup + baseline green; ready to implement Branch 0).

---

### L0002 — Branch 0: io_uring producer send (IORING_OP_SEND)  [branch 0]  [iteration 1]  2026-06-26
- **Goal / hypothesis:** Replace the producer's blocking `send()` in `tcp_send_all` with io_uring
  `IORING_OP_SEND` (the substrate for Branch B's `SEND_ZC`), keeping the bespoke wire UNCHANGED.
  Pre-registered prediction (00-PRE-REGISTRATION Branch 0): correctness preserved; multi-stream W=8
  throughput delta ≈ 0 (cost is the kernel copy, not syscalls); the copy count is unchanged (this is
  `_SEND`, not `_ZC`). This entry establishes CORRECTNESS + io_uring-on-path; the W=8 perf null is
  measured in the Branch-0 measurement step (task #4) alongside the async consumer.
- **What I did (files):**
  - `Makefile`: link `-luring` + define `PGCH_USE_LIBURING` when `/usr/include/liburing.h` exists (no
    hard dependency).
  - `shm_offload.{h,c}`: new GUC `pg_clickhouse.tcp_send_method` (enum io_uring|blocking, default
    io_uring) so io_uring-TCP and bespoke-blocking-TCP A/B on one binary.
  - `shm_worker.{c}`: snapshot the GUC into the worker header (`tcp_send_method`), apply it to the
    producer; emit a `shm tcp-send` LOG (method + per-path send counts + bytes) under log_stream_stats.
  - `shm_producer.{c,h}`: per-worker io_uring ring (lazy init `tcp_iouring_ensure`, `IORING_OP_SEND`
    via `io_uring_prep_send` + `io_uring_submit` + `io_uring_wait_cqe_timeout` 100ms slices that still
    poll `CHECK_FOR_INTERRUPTS`/`origin_backend_dead`; partial-send + EAGAIN/EINTR resubmit); ring
    teardown in `producer_cleanup` (before the conn fd close, before tcp_scratch context free);
    transparent fallback to blocking on ring-init failure; send-path counters + `shm_producer_tcp_send_stats`.
- **How I did it (commands):** `make -j32 && sudo make install`; then the gates + proof below.
- **How verified (3 instrument classes for the correctness/on-path claim):**
  1. **Correctness oracle:** `CH_BIN=…/reldeb/programs/clickhouse PG_DB=shmdemo TRANSPORT=tcp bash
     test/shm/verify_offload.sh` (io_uring default) → **PASS=137 FAIL=0** (`/tmp/b0_verify_iouring.log`),
     clean teardown. Build clean under `-Wall -Werror`.
  2. **In-code counter (deterministic, the on-path proof):** TPC-H Q6 over tcp, 4 workers,
     `shm_log_stream_stats=on` → `method=io_uring iouring_sends≈461 blocking_sends=0 send_bytes≈390 MB`
     per worker; the blocking baseline (`SET tcp_send_method='blocking'`) → `iouring_sends=0
     blocking_sends≈461`. Raw: `evidence/b0-producer-iouring-onpath.txt`.
  3. **PMU/syscall (null/learning):** system-wide `perf stat -a -e
     syscalls:sys_enter_io_uring_enter,sendto` was INCONCLUSIVE (CH's own file-read io_uring swamps the
     producer's ~460 sends; not process-attributable). Logged as a killed instrument; the per-producer
     counter is the correct one.
- **Result (RAW):** see `evidence/b0-producer-iouring-onpath.txt`. io_uring: 4×~461 io_uring sends, 0
  blocking, ~1.56 GB total. blocking: 4×~461 blocking sends, 0 io_uring.
- **Interpretation:** io_uring is genuinely on the producer send path (not a silent fallback), the
  bespoke wire is byte-unchanged (137/137, no new DIFF), and the A/B selector works via `SET` (what the
  sweep harness emits). The copy count is unchanged by design (`IORING_OP_SEND`, kernel still copies
  from userspace). No perf claim here — the W=8 throughput null is measured in task #4.
- **Learnings:** (1) `PGOPTIONS -c` does not reliably set a LOAD-time custom enum GUC (placeholder not
  converted); use `SET` after LOAD (harness already does). (2) System-wide perf syscall counts are
  useless for attributing the producer here — CH's io_uring file reads dominate; the in-code counter is
  the right instrument and is reusable. (3) The producer is single-threaded per stream → io_uring is
  submit-one/wait-one (no batching win expected on loopback), matching the pre-registered ≈0 throughput
  prediction; its value is purely the SEND_ZC substrate + the async story on the consumer side.
- **Verdict:** DONE for the producer-send sub-step (correctness + on-path proven, green, committed).
  CONTINUE → async consumer (task #3), then the Branch-0 W=8 perf measurement (task #4).

---

### L0003 — Branch 0: async TcpStreamSource (+ executor-contract bug found & fixed)  [branch 0]  [iteration 2]  2026-06-26
- **Goal / hypothesis:** Convert `TcpStreamSource` from a blocking `ISource` to an **async** source
  (overlap recv with downstream processing), mirroring `PollableShmSource`'s contract. Pre-registered
  (00-PRE-REGISTRATION Branch 0): correctness preserved; the durable win is the single-stream regime
  (async no longer pins a thread inside recv); multi-stream W=8 ≈ 0. Bespoke wire UNCHANGED.
- **What I did (files):**
  - `TcpStreamSource.{h,cpp}`: rewrote as a dual-mode source. async=true (default): `prepare()` returns
    `Status::Async` while waiting; `schedule()` returns an owned readiness eventfd; a one-shot wake-bridge
    thread polls the socket fd (+ a stop eventfd) up to the remaining stall budget and writes the eventfd;
    a **resumable non-blocking recv state machine** (`tryRecvBlock`/`tryRecvInto`, O_NONBLOCK after the
    one-shot blocking handshake) reassembles a frame straddling schedule cycles. async=false: the Phase-1
    blocking leaf source, preserved for A/B. Shared `buildChunkFromPayload` (charge→retain→adopt→project→
    validate). Recv into the to-be-adopted buffer (so the Arrow adopter drops in for Branch A).
  - `Settings.cpp`/`SettingsChangesHistory.cpp`: new `shm_tcp_source_async` (default true).
  - `StorageShm.cpp`: pass the setting to the source ctor.
  - `shm_customscan.c`: re-derived the deadlock-safety comment for the async model (D-HC-0204): the
    blocking invariant `max_threads ≥ #blocking sources` RELAXES (async sources don't pin a recv thread);
    keeping `max_threads = Σ producers` stays safe for both modes and gives async overlap headroom.
  - gtest: 3 cases — `DrainsHandshakeAndBlocks` (async, fast), `DrainsHandshakeAndBlocksBlocking`
    (blocking), `AsyncResumesAcrossPartialFrames` (async, SLOW 17-byte-fragmented producer with 2ms
    delays → forces the resumable-recv + wake-bridge path).
- **How verified (3 instrument classes):**
  1. **gtests through a REAL PullingPipelineExecutor:** all 3 PASS. The slow async test completes in
     **1187 ms** ≈ the producer's fragment-rate send time (25 blocks × ~27 frags × 2 ms), i.e. the executor
     **blocks efficiently** between fragments — NO busy-spin (the timing is the proof of overlap-not-stall).
  2. **gdb thread-stack + env-gated stderr trace** (PGCH_TCP_TRACE, since removed): used to root-cause the
     hang (below). The fixed build shows the async path drives prepare→Async→schedule→wait→work cleanly.
  3. **verify_offload.sh TRANSPORT=tcp (async default):** [result recorded in L0004].
- **THE BUG (and the fix) — a real null/failure entry:** the first async build **HUNG** the slow test
  (>150 s). Root cause (gdb + trace): `IProcessor::onAsyncJobReady()` is invoked **only** by the
  multi-threaded `processAsyncTasks` monitor (`ExecutorTasks.cpp:325`); the **single-threaded**
  `PullingPipelineExecutor` (num_threads==1) runs `work()` directly on a ready async node and **never**
  calls `onAsyncJobReady`. My source had put the readiness-eventfd drain + `is_async_state=false` reset
  ONLY in `onAsyncJobReady`, so in single-thread mode the level-triggered fd stayed readable → the
  executor hot-spun (`prepare→Async` ~1.07M times, `onAsyncJobReady` 0). A second hang was a post-EOS
  infinite `prepare→Async` (stale `is_async_state` masking the base `Finished`). **Fixes:** (a) do the
  drain/stop/reset at the TOP of the async `tryGenerate` (runs in `work()`, so it covers both executor
  modes), placed BEFORE the `eos_observed` early-return; (b) guard `prepare()`'s Async return with
  `!finished && !eos_observed`. Both gated by the 3 gtests.
- **Interpretation:** the async source is correct in both executor modes; the slow-test timing proves it
  overlaps the recv wait without pinning a thread (the Branch-0 mechanism). The bug was a genuine,
  non-obvious executor-contract gotcha worth recording: **a single-threaded async ISource must not rely
  on `onAsyncJobReady`.**
- **Learnings:** (1) `onAsyncJobReady` ≠ guaranteed; single-thread executors skip it. (2) Diagnosing async
  hangs needs gdb thread stacks + a per-state trace; `timeout`-kill drops buffered stderr (run bg + tail).
  (3) A ninja "exit 0" can precede the `unit_tests_dbms` link — verify the binary before trusting results.
- **Verdict:** DONE for the async-source sub-step pending the in-query gate (L0004). CONTINUE → verify_offload + commit.

---

### L0004 — Branch 0 async consumer: in-query correctness gate (GREEN)  [branch 0]  [iteration 2]  2026-06-26
- **What I did:** removed the debug trace; rebuilt `clickhouse` + `unit_tests_dbms` clean; re-ran the
  gtests; ran the regression oracle in TCP/async-default mode.
- **How verified:**
  - gtests (fresh binary, trace removed): `TcpStreamSource.*` = **4/4 PASS** (async fast 0ms; blocking
    0ms; async-slow-resumable 1188ms; loopback microbench 7.57 GB/s 0.132 ns/byte).
  - `CH_BIN=…/reldeb/programs/clickhouse PG_DB=shmdemo TRANSPORT=tcp bash test/shm/verify_offload.sh`
    (async source is the default) → **PASS=137 FAIL=0** (`/tmp/b0_verify_async.log`), clean teardown,
    no new DIFF (bespoke wire unchanged). Live `:21002` server restarted onto the new binary (pid 2677975).
- **Interpretation:** the async TcpStreamSource is correct in real multi-source offload queries (scans,
  SEMI/ANTI joins one TCP conn per relation, decimal, NULLs, fail-closed, leak teardown) — same 137/137
  as the blocking baseline, so the async rewrite preserves correctness and clean teardown.
- **Verdict:** DONE (async consumer correct + green). Committing. CONTINUE → W=8 A/B measurement (task #4).

---

### L0005 — Branch 0: W=8 A/B measurement + adversarial review + hardening  [branch 0]  [iteration 2]  2026-06-26
- **Goal / hypothesis:** confirm the pre-registered multi-stream W=8 delta ≈ 0 (io_uring-async vs
  bespoke-blocking TCP); run the independent adversarial review; action its findings.
- **What I did:** added `EXTRA_SS`/`EXTRA_SET` hooks to `wsweep_split.sh` + `run_b0_sweep.sh` (A/B on the
  proven harness, identical cap/oracles). Ran a representative W=8 sweep (TPC-H {1,6,19} + ClickBench
  {2,24}), then a clean idle re-measure (N=7) of the win cell (Q1) + the regression cell (Q24).
- **How verified (≥3 instrument classes, converge):**
  1. **End-to-end W=8 timing (both modes fresh, same binary):** 4/5 cells parity within the noise band
     (Q1 +0.3%/clean +1.2%, Q6 −0.2%, Q19 −0.1%, CB Q2 −0.5%). Q24 (SELECT* ~8 GB): sweep-1 +7.3% (load
     ~2.9) → clean idle +4.9% (55 ms) = within `max(5%,1σ)=56 ms`. `evidence/b0-overhead-table.md`,
     `results/{b0-*,clean-*}/`.
  2. **gtests + loopback microbench:** 4/4 PASS; transport rate 7.5–7.9 GB/s (invariant across builds);
     async-resumable 1188 ms ≈ producer fragment-rate (overlap-not-spin).
  3. **Deterministic per-producer io_uring counter** (L0002) — io_uring on the path.
- **Result (RAW):** see `evidence/b0-overhead-table.md` (sweep 1 + clean re-measure tables).
- **Interpretation:** pre-registered null confirmed on 4/5 cells; Q24 is an honest small (~5%, statistically
  real: 2.6σ, t≈6.3) within-floor regression — the io_uring/async per-op overhead on the highest-throughput
  cell (the reviews' "io_uring targets a non-bottleneck on loopback" prediction). No floor breach. The
  single-stream overlap win is mechanism-proven (gtest) but query-level clamp-gated (CONTINUE).
- **Adversarial review (fresh subagent, `evidence/ADVERSARIAL-REVIEW.md`):** **PASS.** Re-ran gtests,
  re-derived every A/B delta from raw cells, confirmed io_uring linked + on path + GUC propagation,
  audited async concurrency vs the real executor. Non-blocking findings: #1 latent `sock_fd` non-atomic
  race (onCancel/dtor) → **FIXED** (atomic + exchange-on-close; rebuilt, 4/4 gtests still PASS); #2
  per-cycle wake-bridge thread spawn (source of the async sys-CPU + Q24 edge) → documented follow-up; #3
  Q24 headline honesty → REPORT tightened (real within-floor regression, not "noise").
- **Verdict:** DONE. Branch 0 GREEN — implementation + measurement + review complete, committed as small
  patches. CONTINUE → Branch A (Arrow wire) is designed/decided/pre-registered; implementation pending.
