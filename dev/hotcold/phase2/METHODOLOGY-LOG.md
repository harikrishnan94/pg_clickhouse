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
