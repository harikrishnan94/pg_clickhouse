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
