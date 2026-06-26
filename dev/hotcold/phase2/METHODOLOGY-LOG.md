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
