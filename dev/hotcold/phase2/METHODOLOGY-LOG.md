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

---

### L0006 — Branch A step 1: vendor nanoarrow 0.8.0 (+ nanoarrow_ipc + flatcc), wire Makefile  [branch A]  [iteration 1]  2026-06-26
- **Goal / hypothesis:** Land the Arrow producer dependency (D-HC-0202) as a self-contained, buildable,
  behavior-NEUTRAL commit (no code references it yet). Predicted: build stays green; the bespoke-TCP
  regression oracle stays 137/137 (nothing on the data path changed).
- **What I did (files):**
  - Vendored the upstream **apache-arrow-nanoarrow-0.8.0** single-file *bundle* (CMake `bundle.py` /
    `-DNANOARROW_BUNDLE=ON -DNANOARROW_IPC=ON` output, staged at `/tmp/na-bundle`) into
    `src/nanoarrow/`: `src/{nanoarrow.c, nanoarrow_ipc.c, flatcc.c}`, `include/{nanoarrow,flatcc}/…`,
    `LICENSE.txt`, `NOTICE.txt`, `README.vendor.md` (provenance + how-regenerated). 44 files.
  - `Makefile`: guarded block (mirrors `PGCH_USE_LIBURING`) — when
    `src/nanoarrow/include/nanoarrow/nanoarrow_ipc.h` exists, add `-DPGCH_USE_NANOARROW
    -I./src/nanoarrow/include` and the 3 vendored TUs to `OBJS` (they live at `src/nanoarrow/src/*.c`,
    one level deeper than the `src/*/*.c` auto-glob, so they are NOT swept into the base extension and
    are added explicitly). After the PGXS include: `$(NANOARROW_OBJS): CFLAGS += -w` so the third-party
    generated code compiles without the base extension's `-Wall -Werror`; `EXTRA_CLEAN += $(NANOARROW_OBJS)`.
- **How I did it (commands):**
  - De-risk pre-vendor: `cd /tmp/na-bundle && gcc -c -Iinclude -O2 src/{nanoarrow,flatcc,nanoarrow_ipc}.c`
    → all 3 compile clean (bundle is self-contained; `flatbuffers_*_reader.h` etc. are inlined into
    `nanoarrow_ipc.c`). Confirmed `NANOARROW_VERSION "0.8.0"` and the IPC encoder API present
    (`ArrowIpcEncoderInit / EncodeSchema / EncodeSimpleRecordBatch / FinalizeBuffer`).
  - `make -j32 && sudo make install`.
- **How verified (correctness gate — no perf claim for a build-wiring step):**
  1. **Build:** clean `make -j32` → links `pg_clickhouse.so` with `src/nanoarrow/src/{nanoarrow,
     nanoarrow_ipc,flatcc}.o` (final link line shows all three); base extension TUs still `-Wall -Werror`.
  2. **Regression oracle:** `CH_BIN=…/reldeb/programs/clickhouse PG_DB=shmdemo TRANSPORT=tcp bash
     test/shm/verify_offload.sh` → **PASS=137 FAIL=0, EXIT=0** (`/tmp/bA_step1_verify.log`) on the
     live CH Branch-0 binary — i.e. the bespoke-TCP path is byte-unchanged by the vendoring.
- **Result (RAW):** `verify_offload TRANSPORT=tcp`: `PASS=137 FAIL=0 ALL CHECKS PASSED EXIT=0`. nanoarrow
  0.8.0 vendored, 44 files. `nm -D` shows ~106 `Arrow*` + flatcc symbols exported (the headers force
  `__attribute__((visibility("default")))` via `NANOARROW_DLL`, overriding `-fvisibility=hidden`).
- **Interpretation:** dependency landed, build green, no behavior change — exactly the predicted neutral
  outcome. The exported-symbol surface is harmless for a single dlopen'd extension here (no other
  Arrow/flatcc consumer in this PG backend) but is a productionization follow-up (namespace via
  `-DNANOARROW_NAMESPACE` for the nanoarrow symbols; flatcc would need separate handling).
- **Learnings:** (1) keep the vendored `.c` one level deeper than `src/*/*.c` so the base-extension glob
  never sweeps third-party code into `-Wall -Werror`. (2) The bundle's IPC TU inlines all flatcc-generated
  flatbuffer headers, so `-Iinclude` is the only include flag needed.
- **Verdict:** DONE (A1 green, committed). CONTINUE → A2 (`arrow:` transport-token plumbing) + A3
  (producer Arrow IPC serialize behind the `arrow:` token).

---

### L0007 — Branch A: producer Arrow IPC encoder module (TDD round-trip)  [branch A]  [iteration 1]  2026-06-26
- **Goal / hypothesis:** A PG-free, standalone-testable producer-side serializer that turns the
  already-deformed `ShmColumnPayload` buffers into standards-valid encapsulated Arrow IPC (Schema +
  RecordBatch) via nanoarrow, viewing the buffers zero-copy (D-HC-0201/0202/0203/0207). Predicted: a
  value-exact round trip across every Branch-A type family, decoded by nanoarrow's stock stream reader.
- **What I did (files):**
  - `src/include/shm_wire.h` (NEW, PG-free): relocated `ShmWireType` + made `shm_wire_fixed_width_size`
    a `static inline` (shared by the bespoke serializer and the Arrow one, no value drift). Rewired
    `src/include/shm_producer.h` (include it; drop the inline enum + extern) and `src/shm_producer.c`
    (drop the now-inline definition).
  - `src/include/shm_arrow.h` + `src/shm_arrow.c` (NEW, PG-free; guarded `#ifdef PGCH_USE_NANOARROW`):
    `shm_arrow_encoder_create/destroy`, `shm_arrow_encode_schema`, `shm_arrow_encode_record_batch`,
    `SHM_ARROW_EOS_MARKER`. Builds one `ArrowSchema` (struct of N children) once; per block builds a
    hand-rolled C-Data-Interface `ArrowArray` whose child buffers point at the deformed buffers (only
    extra work: prepend a `0` to the N END offsets to make Arrow's `N+1` LargeBinary offsets),
    `ArrowArrayViewSetArray`s it, and emits via `ArrowIpcEncoderEncodeSchema`/`EncodeSimpleRecordBatch`
    + `FinalizeBuffer(encapsulate=1)`. Type map per D-HC-0207 (raw uint16/uint32/int64 for
    Date/DateTime/DateTime64; raw int32/int64 for Decimal32/64; FixedSizeBinary(16) for Decimal128;
    LargeBinary for String).
  - `dev/hotcold/phase2/tests/arrow_roundtrip_test.c` (NEW): the TDD test.
  - `dev/hotcold/DECISIONS.md`: D-HC-0207 (wire = standard Arrow IPC stream; no bespoke handshake).
- **How I did it (TDD):** wrote `arrow_roundtrip_test.c` first (7 columns: UInt64, String, Int32,
  Float64, Date, DateTime, Decimal128) → built WITHOUT `shm_arrow.c` → **RED** (4 undefined refs, test
  + nanoarrow decode API compiled clean). Implemented `shm_arrow.c` → debugged 2 crashes with gdb
  (below) → **GREEN**.
- **How verified (3 instrument classes):**
  1. **Standalone round-trip gtest-equivalent:** `gcc -DPGCH_USE_NANOARROW … arrow_roundtrip_test.c
     shm_arrow.c nanoarrow*.c` → encode Schema+RecordBatch, reassemble the IPC stream, decode via
     nanoarrow's **stock** `ArrowIpcArrayStreamReader` → `ALL 3-row round trip across 7 columns PASSED`
     (exit 0). Values + the LargeBinary strings + the 16-byte Decimal128 bytes all match.
  2. **Extension build under -Wall -Werror:** `make -j32` compiles `src/shm_arrow.o` clean (a
     `#pragma GCC diagnostic` brackets only the nanoarrow includes; my code stays -Wall -Werror) and
     links it into `pg_clickhouse.so`.
  3. **Bespoke regression oracle (the floor):** `verify_offload.sh TRANSPORT=tcp` — caught a real
     LTO bug (below); after the fix, re-run = **PASS=137 FAIL=0** (`/tmp/bA_step2_verify2.log`).
- **THE LTO BUG (gate-caught):** the first cut made `shm_wire_fixed_width_size` a `static inline` in
  `shm_wire.h`. The local `.so` inlined it everywhere (no symbol), but `make install`'s rebuild under
  `-flto=auto` *non-deterministically* out-of-lined a call into an **undefined external** symbol →
  `dlopen` failed: "undefined symbol: shm_wire_fixed_width_size" (the two `.so`s had different md5s).
  Fix: give it ONE real external definition in its own PG-free TU `src/shm_wire.c` (declared `extern`
  in the header). Lesson: a cross-TU `static inline` in a header is an `-flto` footgun; prefer a single
  out-of-line definition.
- **THE BUGS (gdb-found, real entries):** (1) `ArrowBufferReset` calls `allocator.free()`
  *unconditionally*; a calloc'd-zero buffer has a NULL free ptr, so a `create` failure → `destroy`
  segfaulted at 0x0. Fix: `ArrowBufferInit(body/message)` BEFORE any goto-fail. (2)
  `ArrowSchemaAllocateChildren` leaves each child `release==NULL` (released); `ArrowSchemaSetType` then
  silently no-ops and `ArrowArrayViewInitFromSchema` rejects "released schema". Fix: `ArrowSchemaInit`
  each child before setting its type. Also a TEST-only bug: the encoder reuses one output buffer, so the
  schema bytes must be copied to the stream before encoding the record batch (documented contract).
- **Interpretation:** the producer emits well-formed, value-exact Arrow IPC for all Branch-A types,
  cross-decoded by an independent reader. The hand-built C-Data-Interface ArrowArray views the deformed
  buffers with no copy; nanoarrow's single body concatenation is the one userspace serialize copy
  (mirrors the bespoke scratch copy; Branch B fuses it).
- **Learnings:** (1) nanoarrow's encoder requires the view be backed by a real `ArrowArray`
  (`child->array->n_buffers`), so a hand-built C-ABI array + `ArrowArrayViewSetArray` is the zero-copy
  feed path (not a bare hand-populated view). (2) `ArrowArrayViewSetArray`→ValidateDefault derives the
  LargeBinary data-buffer size from the last offset, so the `N+1` offsets must be correct.
- **Verdict:** DONE for the producer encoder module (round-trip green, builds clean). CONTINUE →
  `arrow:` transport plumbing + wiring the encoder into `shm_producer.c` behind
  `PGCH_PRODUCER_TRANSPORT_ARROW`.

---

### L0008 — Branch A: wire the Arrow encoder into the producer + `arrow:` transport plumbing  [branch A]  [iteration 1]  2026-06-26
- **Goal / hypothesis:** Make `pg_clickhouse.shm_transport_mode='arrow'` end-to-end *selectable* on the
  producer: the deparser emits `arrow:127.0.0.1:<port>`, the worker maps it to a new producer transport,
  and the producer streams a standard Arrow IPC stream (Schema + RecordBatch* + EOS) over the same
  per-stream TCP socket the bespoke path uses (D-HC-0205/0207). Predicted: bespoke path byte-unchanged
  (137/137); arrow path not yet runnable end-to-end (the consumer doesn't parse `arrow:` until A4).
- **What I did (files):**
  - **Plumbing:** `shm_offload.{h,c}` — `PGCH_TRANSPORT_ARROW=3` + GUC value `arrow` + description.
    `shm_producer.h` — `PGCH_PRODUCER_TRANSPORT_ARROW=2`. `shm_worker.c` — GUC→producer-transport map
    gains the arrow case; the send-stats LOG gate widened from `==TCP` to `!=SHM`. `shm_customscan.c` —
    `shm_build_union_sql` emits `'<scheme>:127.0.0.1:<port>'` with `scheme ∈ {tcp,arrow}`.
  - **Producer integration:** `shm_producer.c` — `#include "shm_arrow.h"`; an `arrow_enc` field;
    extracted `tcp_accept_conn()` (shared accept+sockopts) out of `tcp_accept_and_handshake()`; new
    `arrow_accept_and_send_schema()` (accept → build encoder from `p->schema` → send Arrow Schema msg)
    and `arrow_publish_block()` (per block: encode RecordBatch, send encapsulated metadata + body; EOS:
    send `SHM_ARROW_EOS_MARKER`), both `#ifdef PGCH_USE_NANOARROW`. Dispatch in
    `shm_producer_create/publish/signal_eos/destroy` treats ARROW like a socket transport; the publish/eos
    dispatch errors cleanly if ARROW is selected in a non-nanoarrow build. `producer_cleanup` frees
    `arrow_enc` (its nanoarrow buffers are malloc'd, not palloc'd). The deformed payloads are passed to
    the encoder by reinterpret-cast (`ShmColumnPayload`≡`ShmArrowColBuffers`, two `StaticAssertDecl`s pin
    the layout) — no per-block adaptation copy.
- **How verified (correctness gate; end-to-end arrow is A5):**
  1. **Build:** `make -j32` clean; `pg_clickhouse.so` links `shm_arrow.o` + the integration.
  2. **Bespoke regression oracle (floor):** `verify_offload.sh TRANSPORT=tcp` → **PASS=137 FAIL=0**
     (`/tmp/bA_step3_verify.log`) — the bespoke wire + SHM paths are byte-unchanged by the arrow plumbing.
- **Interpretation:** the producer can now speak Arrow IPC on `arrow:` selection without disturbing any
  existing transport. Correctness of the arrow data path is proven end-to-end in A5 (needs the consumer).
- **Learnings:** the bespoke accept loop was cleanly shared (just the wire-specific handshake differs);
  reusing `tcp_handshake_sent` as the generic "stream started" flag kept the lazy-first-publish structure
  identical across wires.
- **Verdict:** DONE (producer arrow path selectable + green; bespoke unaffected). CONTINUE → A4: the CH
  consumer (`ShmTransportMode::ArrowTcp` + `TcpStreamSource` Arrow recv/decode + round-trip gtest vs the
  stock `ArrowColumnToCHColumn` oracle).

---

### L0009 — Branch A: Apache Arrow IPC consumer + the correctness gates (GREEN)  [branch A]  [iteration 1]  2026-06-26
- **Goal / hypothesis:** The CH consumer reads the standard Arrow IPC stream the producer now emits and
  reconstructs byte-identical Chunks (Branch-A COPYING decode), with no new `DIFF` vs native/other
  transports and no bespoke regression. Correctness is the Branch-A bar; perf parity is measured in A5.
- **What I did (ClickHouse repo, branch `streamed_table`, commit f2128e5a7fb):**
  - `TransportMode.h`: `ShmTransportMode::ArrowTcp` + `arrow:<host>:<port>` parse (shared
    `tryParseHostPort` with `tcp:`) + `toString`.
  - `StorageShm::read()`: dispatch `Tcp|ArrowTcp` → `TcpStreamSource` with a `WireFormat` flag.
  - `TcpStreamSource.{h,cpp}`: a `WireFormat{Bespoke,Arrow}` mode REUSING the whole Branch-0 async
    recv / wake-bridge / `onCancel` / `RetainToken` / charge machinery (wire-agnostic). Arrow path:
    `ensureConnected` → `readArrowSchema()` (read the leading Arrow IPC Schema message, validate the
    field count, build the projection map — replaces the bespoke handshake); `tryRecvArrowMessage()` =
    a resumable 3-phase recv (8-byte prefix / metadata / body) over the same `tryRecvInto`, with a
    0-length-metadata continuation as the stream EOS; `buildChunkFromArrow()` parses with Arrow C++
    (`Message::Open` + `ReadRecordBatch`) and COPIES each column into an owned CH column of the
    SQL-declared type (`copyArrowColumnToCH`: bulk memcpy of the contiguous LE data buffer for
    fixed-width incl. raw Date/DateTime/DateTime64/Decimal; `LargeBinary`→`ColumnString` chars + N END
    offsets). `ArrowRecvState` is a pimpl (arrow headers stay out of the .h). Charges `copied=true`
    (bumps `ShmCopiedBlocks`, the offload oracle). Guarded `#if USE_ARROW`.
  - `tests/gtest_arrow_stream_source.cpp`: loopback drain (async / blocking / slow-fragmented) with an
    **Arrow-C++ reference stream writer** as the producer, + a stock **ArrowColumnToCHColumn** decode
    oracle cross-check.
  - `test/shm/verify_offload.sh` (pg repo): `arrow` added to the `ShmCopiedBlocks` offload-oracle case.
- **How I did it (commands):** `ninja -C build/reldeb clickhouse unit_tests_dbms`; restart CH onto the
  new binary (live pid via `ss`); `TRANSPORT=arrow … verify_offload.sh`; `unit_tests_dbms --gtest_filter`.
- **How verified (≥3 INDEPENDENT converging instrument classes — all GREEN):**
  1. **End-to-end offload oracle** (the authoritative one): `CH_BIN=… PG_DB=shmdemo TRANSPORT=arrow bash
     test/shm/verify_offload.sh` → **PASS=137 FAIL=0** (`/tmp/bA_verify_arrow.log`). Producer nanoarrow →
     this consumer, results byte-identical to native + the other transports across every type, NULLs,
     SEMI/ANTI joins (one Arrow stream per relation), Decimal, projection, `count()`, EOS,
     producer-death/cancel, and leak teardown. The offload oracle asserted `ShmCopiedBlocks≥1` per heavy
     fragment — i.e. the arrow transport actually ran, not a base scan.
  2. **gtests** (`unit_tests_dbms`, fresh binary): `ArrowStreamSource.*` = **4/4 PASS** —
     `DrainsSchemaAndBatches`, `DrainsBlocking`, `AsyncResumesAcrossPartialMessages` (1022 ms ≈ the
     fragment-rate → the resumable 3-phase Arrow recv reassembles a message straddling schedule cycles
     without busy-spin), and `DecodeMatchesStockArrowReader` (the **independent stock
     `ArrowColumnToCHColumn` decoder agrees** cell-for-cell with the custom Branch-A decode AND the known
     values — uint16 'd' → Date via the header type hint).
  3. **Cross-implementation encode check:** the gtest producer uses Arrow C++'s OWN
     `RecordBatchStreamWriter` (not nanoarrow), so the consumer is proven to read *standard* Arrow IPC,
     not just nanoarrow's output. (End-to-end #1 independently proves the nanoarrow encode side.)
  4. **No bespoke regression:** `TRANSPORT=tcp` = **137/137** on the new binary; `TcpStreamSource.*` =
     **4/4** (loopback microbench 7.47 GB/s, invariant). The async machinery is shared, byte-unchanged.
- **Result (RAW):** verify_offload arrow `PASS=137 FAIL=0 EXIT=0`; tcp `PASS=137 FAIL=0`; gtests
  `[ PASSED ] 8 tests` (ArrowStreamSource 4 + TcpStreamSource 4).
- **Interpretation:** the Arrow wire is correct end to end and the bespoke `TcpFrame.h` is now retire-able
  on the `arrow:` transport (intention 1 substantially delivered). Four independent classes converge on
  correctness; no new `DIFF`. The decode is COPYING by design (Branch A) — Branch B drives the copies out.
- **Learnings:** (1) the chicken-and-egg of "need bodyLength to size the body recv, but it's inside the
  metadata flatbuffer" is solved cleanly by `Message::Open(metadata, /*body=*/nullptr)` to read
  `body_length()` before recv'ing the body — and this metadata-then-body split IS exactly Branch B's
  single-copy-recv shape. (2) Reusing the Branch-0 async source (vs a new class) meant the riskiest code
  (executor contract, cancel/teardown) was already hardened — the new surface is just framing + decode.
- **Verdict:** DONE. Branch A correctness GREEN, committed (CH f2128e5a7fb; pg A2/A3). CONTINUE → A5:
  W=8 sweep (arrow-tcp vs bespoke-tcp vs shm-adopt/copy) for the fixed-width-parity / String-bounded-
  regression gate, then the independent adversarial review + REPORT-branchA.md.

---

### L0010 — Branch A: W=8 perf gate + adversarial review + review-fix (GREEN)  [branch A]  [iteration 1]  2026-06-26
- **Goal / hypothesis:** the pre-registered Branch-A perf gate — fixed-width/numeric cells at PARITY with
  bespoke-TCP at W=8 (a fixed-width regression BLOCKS the branch), String-heavy a BOUNDED +10–30%
  regression (copying decode), recovered in Branch B. Plus the mandated independent adversarial review.
- **What I did:** wrote `run_bA_sweep.sh` (arrow/tcp/adopt, all fresh on the same binaries in one session,
  via `wsweep_split.sh` with `TRANSPORT=` + empty `EXTRA_SET/SS`). Ran W=8 N=5 on an idle host (load 0.12):
  TPC-H {1,6,19} + ClickBench {2,24}. Built `evidence/bA-overhead-table.md` + `REPORT-branchA.md`. Ran a
  fresh independent adversarial-review subagent. Actioned its finding #1.
- **How I did it (commands):** `N=5 K=3 bash dev/hotcold/phase2/run_bA_sweep.sh`; parsed
  `results/bA-*/{tpch,clickbench}/cells.tsv`; `unit_tests_dbms --gtest_filter='ArrowStreamSource.*:TcpStreamSource.*'`.
  (First sweep aborted to "no-customscan" because `wsweep_split.sh` uses `$EXTRA_SS` under `set -u`; passing
  `EXTRA_SET="" EXTRA_SS=""` fixed it — logged here as the null that explained an early no-offload run.)
- **How verified (≥3 INDEPENDENT converging classes):**
  1. **End-to-end W=8 wall (median+sd, fresh same-binary):** TPC-H Q1 **+1.3%**, Q6 **+0.6%**, Q19 **+2.4%**,
     CB Q2 **−0.5%** → all 4 fixed-width/numeric cells **PARITY** within `max(5%,1σ)` (no fixed-width
     regression → blocking condition satisfied). CB Q24 (`SELECT *` ~8 GB, String-heavy) **+15.9%** — inside
     the pre-registered **+10–30%** bound. `evidence/bA-overhead-table.md`, `results/bA-*/`.
  2. **Consumer CPU split (mechanism):** Q24 `cons_user` arrow−tcp = **+283.7 ms**, `cons_sys` **+199.3 ms**
     — the String copy-decode (arrow rebuilds `ColumnString` chars+offsets; bespoke adopts zero-copy);
     negligible on fixed-width (Q1 +11 ms, Q6 +60 ms user). The producer side is ≈ equal (`off_prod` ~4–7 ms).
  3. **Gap-to-adopt decomposition:** Q24 = adopt 914 + kernel-recv-copy(→tcp 1148, +26%) +
     consumer-copy-decode(→arrow 1331, +16%). The regression IS the copy-decode layer → Branch B removes it
     (→ bespoke parity); the kernel recv copy is the irreducible loopback residual (real-NIC north star).
- **Result (RAW):** see `evidence/bA-overhead-table.md`. Fidelity: every swept cell's `cmp.py` verdict is
  byte-identical across arrow/tcp/adopt (Q1 `approx(<=1.4e-16)` = same Decimal→Float64 bound in all modes;
  rest `exact`). No new DIFF.
- **Adversarial review (fresh subagent — `evidence/ADVERSARIAL-REVIEW.md` Branch-A section): VERDICT PASS.**
  Re-ran 8/8 gtests, re-derived all 5 deltas (≤0.1% match), confirmed the +283.7 ms mechanism, identical
  fidelity verdicts, memory safety (body freed after `batch.reset()`), no bespoke regression. 3 non-blocking
  findings: **#1 FIXED in-branch** — `readArrowSchema()` validated only field COUNT, not per-field layout
  (D-HC-0207 requires count + layout; the fixed-width memcpy trusted the SQL width → a latent width-mismatch
  heap over-read). Added per-field cross-validation: String ↔ variable-binary Arrow field; every fixed-width
  SQL type ↔ a fixed-width Arrow field of the SAME byte width (`FixedWidthType::bit_width()/8 ==
  getSizeOfValueInMemory()`). Rebuilt; **gtests 8/8 + verify_offload arrow 137/137 re-confirmed**. #2 (the
  stock-reader oracle is transitive via shared literals) DOCUMENTED. #3 (the `offsets[0]==0` sentinel
  "MUST validate") carried into Branch B as a binding requirement (the zero-copy adopter needs it).
- **Interpretation:** prediction matches observation exactly — fixed-width parity, String +15.9% inside the
  pre-registered band, mechanism = consumer copy-decode. Three perf classes + four correctness classes
  converge. The one regression is reported, bounded, and slated for Branch-B recovery — not hidden.
- **Verdict:** DONE. **Branch A GREEN** (correct, fixed-width parity, String bounded-regression, review
  PASS, finding #1 fixed). CONTINUE → Branch B (zero-copy Arrow adoption + ≥3 evidence-based optimization
  iterations + the copy-budget table + send-side measured-null + single-copy-recv).

---

### L0011 — Branch B kickoff: holistic end-to-end note + the adopt/alignment investigation  [branch B]  [iteration 0]  2026-06-26
- **Goal / hypothesis:** Before touching code (process rule 3), map the whole dataflow change Branch B
  makes and the binding constraint on zero-copy adoption of Arrow buffers. No perf claim yet.
- **Holistic end-to-end note (process rule 3).** Branch B changes exactly ONE stage of the path PG-deform
  → Arrow-serialize → io_uring-send → wire → recv-into-to-be-adopted-buffer → **DECODE** → CH-pipeline →
  drop: the consumer DECODE flips from Branch-A *copy* (`copyArrowColumnToCH` memcpy each Arrow buffer into
  an owned CH column) to *adopt* (the CH column ALIASES the Arrow buffer, a slice of the recv body, held
  by a RetainToken whose deleter frees the body on last-drop). Everything else is unchanged: the recv is
  already single-copy into the to-be-adopted buffer (Branch 0/A), the metadata-then-body framing is already
  the single-copy-recv shape, the producer already does one userspace serialize copy. So the predicted
  system effect is: the String/wide cells lose the consumer copy-decode layer (Branch-A Q24 +15.9% → ~tcp
  parity), fixed-width stays at parity (it was already parity, the copy was negligible), and the residual
  gap to SHM-adopt stays = the irreducible kernel recv copy (real-NIC north star, not closable on loopback).
  The body buffer is now retained (not freed in buildChunk) → in-flight memory = K recv buffers/stream;
  must prove prompt drop (RetainToken last-drop on chunk consume).
- **What I did:** dispatched an Explore agent to map `AdoptionLayer::adopt` + the `createAdopted` factories
  (`ColumnVector`/`ColumnString`/`ColumnDecimal`) + the alignment/SIMD-pad contract + `RetainToken`/
  `ChargeHandle` distribution + PODArray adopted mode. No code changed.
- **Key findings (the binding constraint):**
  1. `createAdopted` signatures: `ColumnVector<T>::createAdopted(T* data, n, retain, charge)`;
     `ColumnString::createAdopted(UInt8* chars, chars_size, UInt64* offsets, rows, retain, charge)`;
     `ColumnDecimal<T>::createAdopted(T* data, n, scale, retain, charge)`. Each takes a `RetainToken`
     (`std::shared_ptr<void>`) + a `ChargeHandle` (wrapped shared) shared across all columns of the block.
  2. **Alignment + SIMD pad (the crux):** each adopted buffer must be at its natural alignment and have
     `PADDING_FOR_SIMD == 64` bytes of safely-readable trailing slack. Arrow IPC bodies pad buffers to only
     **8 bytes** (nanoarrow `_ArrowRoundUpToMultipleOf8` in the body callback). **Resolution:** the 64-byte
     over-read is MEMORY-SAFE for every buffer because (a) `allocFrameBuffer` slacks the whole recv body by
     64 B at the end, and (b) a mid-body buffer's +64 over-read lands in the adjacent buffer (within the
     allocation) and SIMD masks the extra lanes → correctness-safe. 8-byte alignment satisfies every type
     ≤ 8 bytes + the 8-byte String offsets. The ONLY exception is **Decimal128** (needs 16-byte alignment;
     an 8-byte body offset may be 8 mod 16) → **copy fallback for Decimal128** (a documented residual), or
     a producer-64B-alignment follow-up. The swept datasets (TPC-H Decimal64, ClickBench no-decimal) have
     NO Decimal128, so adoption covers every exercised column; D128 fallback is correctness-preserving.
  3. **String adopt:** `chars = value_data()`, CH `offsets = &arrow_offsets[1]` (so CH `offsets[-1]` reads
     `arrow_offsets[0]`), MUST validate `array.offset()==0 && arrow_offsets[0]==0` (review finding #3 /
     D-HC-0201/0207); then `validateAdoptedOffsets()`.
  4. **RetainToken lifetime:** one token per block (deleter frees the recv body), shared across all adopted
     columns (the bespoke `buildChunkFromPayload` is the precedent); buildChunkFromArrow must STOP freeing
     `body_buf` and hand it to the token instead.
- **Pre-registered Branch-B iterations (00-PRE-REGISTRATION):** B-it1 producer 1-userspace-copy; **B-it2
  zero-copy adopt (the headline — recovers Q24)**; B-it3 send-side `IORING_OP_SEND_ZC` measured-null. Plus
  the copy-budget table, `convertToFullColumnIfAdopted` mutate-materialize + `ColumnNullable` recurse
  (D-HC-0206), single-copy-recv proof.
- **Verdict:** DONE (kickoff + constraint mapped). CONTINUE → implement B-it2 (the adopt-mode decoder +
  RetainToken retention + Decimal128 copy-fallback + String sentinel validation), gate, then measure the
  Q24 recovery.

---

### L0012 — Branch B iteration 2: zero-copy adoption — correct + measured; PARTIAL recovery (honest)  [branch B]  [iteration 2]  2026-06-26
- **Goal / hypothesis (pre-registered B-it2):** zero-copy adopt of the Arrow buffers ELIMINATES the
  consumer copy-decode → predicted to **recover the Branch-A String-heavy Q24 regression to bespoke-TCP
  parity**; fixed-width stays at parity; alloc/no-cloneResized prove the elimination.
- **What I did:** committed B-it2 (CH f30ed9d6efc): `shm_arrow_zero_copy` setting (default adopt; 0=copy
  for A/B), `adoptArrowColumnToCH` aliasing the Arrow buffers via `createAdopted` + one RetainToken/block
  (frees the recv body) + shared ChargeHandle; Decimal128/sliced/bad-sentinel → copy fallback. Ran the
  W=8 sweep `run_bB_sweep.sh` (arrow-adopt / arrow-copy / tcp / shm-adopt, all FRESH same-binary same-
  session, idle host load 0.12).
- **How verified (≥3 INDEPENDENT classes):**
  1. **Correctness:** verify_offload TRANSPORT=arrow (adopt default) = **137/137** (joins/aggregates over
     adopted columns, Decimal copy-fallback, EOS, leak teardown). gtests 10/10 (adopt + copy + stock oracle
     + zero-copy-across-fragmented-recv). No new DIFF (cmp.py verdicts identical to native/tcp/copy).
  2. **End-to-end W=8 timing (RAW, ms median(sd)):** TPC-H Q1 adopt 1581(14)/copy 1571/tcp 1547/shm 1424;
     Q6 1244/1238/1226/1158; Q19 2254/2260/2216/2042. CB Q2 407/408/404/402. **CB Q24 (headline) adopt
     1242(15) / copy 1292 / tcp 1156 / shm-adopt 916.** Fixed-width: adopt vs tcp +1.5…+2.2% → PARITY.
     Q24: adopt vs copy **−3.9%**; adopt vs tcp **+7.4%** (ABOVE the max(5%,1σ) band → NOT full parity).
  3. **Consumer CPU split (mechanism):** CB Q24 `cons_user` adopt **663** vs copy **962** = **−299 ms** —
     the copy-decode IS eliminated (matches the predicted +283 ms). adopt vs tcp `cons_user` +47, `cons_sys`
     +118.
- **Interpretation (prediction vs observation — a MISMATCH to investigate, NOT rationalize):** the
  zero-copy adoption is real and proven (−299 ms consumer user CPU; createAdopted aliases the buffer by
  construction; 137/137 correct). BUT the pre-registered "recover Q24 to bespoke parity" **did not fully
  hold**: adopt is **+7.4% vs bespoke-tcp**, above the noise band. Two converging reasons: (a) on this
  bandwidth-bound ~8 GB cell the eliminated consumer CPU was **overlapped** with the recv, so removing
  299 ms of CPU only moved the wall −50 ms (−3.9% vs copy); (b) the residual +7.4% vs bespoke is the
  **Arrow IPC framing/parse overhead** — `Message::Open` + `ReadRecordBatch` construct 105 `arrow::Array`
  objects/block (allocations + flatbuffer walk) that the bespoke descriptor wire does not pay. The wall
  decomposes: shm-adopt 916 + kernel-recv-copy(→tcp 1156, +240) + Arrow-parse(→arrow-adopt 1242, +86) [+
  copy-decode in arrow-copy 1292, +50]. So Branch B removes the copy-decode layer (intention 2 progress)
  but exposes the Arrow-parse layer as the new residual — the honest cost of a STANDARD format (intention 1).
- **Learnings / whole-system:** eliminating a consumer copy only helps the wall when the copy is ON the
  critical path; on a recv-bandwidth-bound cell it is overlapped, so the win is CPU/energy (real) more than
  wall. The next lever is the Arrow framing per-block parse (`ReadRecordBatch` array construction), not the
  data copy. Fixed-width cells were already parity (copy negligible) and stay parity.
- **Verdict:** CONTINUE. B-it2 is correct + the copy-decode is measurably eliminated, but the Q24 floor
  (parity vs bespoke) is NOT met (+7.4% residual = Arrow-parse). → B-it3: a LEAN Arrow buffer-extraction
  that skips `ReadRecordBatch`'s per-block `arrow::Array` construction (walk the RecordBatch flatbuffer's
  `Buffer{offset,length}` directly → adopt), pre-registered to attack the +7.4%. (Also B-it1 producer-1-copy
  confirm; send-zc measured-null.) D-HC-0206 ColumnNullable recurse + gtest pending a build.
