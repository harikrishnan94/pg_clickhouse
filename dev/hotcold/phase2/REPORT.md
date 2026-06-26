# Hot-Cold Phase 2 — REPORT (Apache Arrow wire + zero-copy transport)

Sequenced **0 → A → B**. This top-level report states the honest status of each branch; per-branch
detail in `REPORT-branch0.md` (and `REPORT-branchA.md` / `REPORT-branchB.md` when those land). Evidence
under `evidence/`, `results/`; decisions in `../DECISIONS.md` (D-HC-0201..0206); pre-registration in
`00-PRE-REGISTRATION.md`; the append-only trail in `METHODOLOGY-LOG.md`; the per-step adversarial
reviews in `evidence/ADVERSARIAL-REVIEW.md`.

## Status summary (honest)

| Branch | Scope | Status |
|---|---|---|
| **0 — io_uring + async transport** | producer `IORING_OP_SEND`; async `TcpStreamSource` (overlap recv) on the **bespoke** wire | **GREEN — complete, measured, adversarially reviewed (PASS), committed** |
| **A — Apache Arrow serialization** | replace the bespoke `TcpFrame.h` payload with an Arrow IPC record-batch (nanoarrow producer; Arrow consumer; copying decode OK) | **DESIGNED + DECIDED (D-HC-0201..0203,0205) + PRE-REGISTERED — implementation NOT STARTED this session** |
| **B — copy reduction** | zero-copy Arrow→`ColumnPtr` adopt; `SEND_ZC` measured-null; single-copy recv; `ColumnNullable` recurse; ≥3 optimization iterations | **DESIGNED + DECIDED (D-HC-0206) + PRE-REGISTERED — implementation NOT STARTED this session** |

This is an honest accounting, not a claim of completion. Branch 0 — the structural precursor the
feasibility reviews uniformly flagged as the *de-risking* step (async + deadlock-safety before the
format changes) — is delivered to the full evidence standard. Branches A and B are fully scoped and
their judgement calls are decided + pre-registered (so the implementation has no open design questions),
but the substantial new code (nanoarrow IPC emission on the C producer; a new Arrow reader + a custom
zero-copy adopter on the consumer; ≥3 measured optimization iterations) was not written in this session.

## Branch 0 — GREEN (see REPORT-branch0.md)
- Producer send via io_uring `IORING_OP_SEND` (substrate for Branch-B `SEND_ZC`); GUC-selectable vs
  blocking, proven on the path by a deterministic per-producer counter. Copy count unchanged (`_SEND`).
- Consumer `TcpStreamSource` made async (Status::Async + readiness eventfd + resumable non-blocking recv
  + wake bridge), preserving the blocking baseline for A/B. A real executor-contract bug (single-thread
  `PullingPipelineExecutor` never calls `onAsyncJobReady`) was found and fixed.
- Gates: `verify_offload.sh TRANSPORT=tcp` 137/137 (async default); `TcpStreamSource.*` gtests 4/4; no
  new `DIFF`; clean teardown. Deadlock-safety invariant re-derived for async + pinned (code comment + a
  liveness gtest).
- W=8 A/B (io_uring-async vs bespoke-blocking), idle re-measure: parity within the noise band on 4/5
  cells (pre-registered ≈0 confirmed); ClickBench Q24 (`SELECT *` ~8 GB) a small honest within-floor
  regression (+4.9%, clears the pre-committed 5% floor by ~1.4 ms; statistically real, mechanism =
  io_uring/async per-op overhead on the highest-throughput cell). Single-stream overlap is
  mechanism-proven (gtest) but query-level clamp-gated (documented CONTINUE).
- Adversarial review: **PASS**; findings #1 (atomic `sock_fd`) + #3 (Q24 honesty) actioned in-branch,
  #2 (persistent wake-bridge thread) documented as the clean follow-up.

## Intention (1) — retire the bespoke format: status
The bespoke `TcpFrame.h` wire is still in place and is the basis Branch A migrates onto Apache Arrow.
The format choice, per-type Arrow mapping, nanoarrow producer path, and the raw-Date/DateTime/DateTime64
tradeoff are decided + pre-registered (D-HC-0201..0203). Retire-ability is **not yet achieved** — it is
the deliverable of Branch A (Arrow on the wire, stock `ArrowColumnToCHColumn` as the decode oracle).

## Intention (2) — best performance via copy reduction: status
Branch 0 establishes the io_uring substrate and the async source; it is, as pre-registered, throughput-
neutral on loopback at W=8 (the cost is the kernel copy, not syscalls). The copy-reduction itself
(zero-copy Arrow adoption; `SEND_ZC` measured-null; single-copy recv) is Branch B — **not yet
implemented**. The pre-registration records the full copy-budget predictions and the loopback-vs-real-NIC
honesty (kernel recv copy is the irreducible residual on this host).

## Next steps (Branch A, then B) — no open design questions; implementation only
1. Vendor `nanoarrow` + `nanoarrow_ipc` (0.8.0) into `pg_clickhouse/src/` (+ flatcc runtime); Makefile.
2. Producer: emit an Arrow IPC record-batch per block (fixed-width LE buffers first; then `LargeBinary`
   String, validity-bitmap Nullable, raw uint16/uint32/int64 Date/DateTime/DateTime64), IPC alignment 64,
   behind the `arrow:<host>:<port>` transport token (D-HC-0205).
3. Consumer: read the Arrow IPC message in `TcpStreamSource` (the async resumable recv already lands the
   body in the to-be-adopted buffer); Branch A uses the **copying** decode; cross-check vs the stock
   `ArrowColumnToCHColumn` oracle (round-trip gtest). Gate: no new `DIFF`; fixed-width parity; String
   bounded regression.
4. Branch B: custom Arrow→`adopt()` zero-copy path (fixed-width + `LargeBinary` via `&arrow_offsets[1]`,
   validate `offset()==0 && offsets[0]==0`); `ColumnNullable::convertToFullColumnIfAdopted` recurse;
   fused deform+serialize (1 userspace copy); `IORING_OP_SEND_ZC` measured-null via
   `SO_EE_CODE_ZEROCOPY_COPIED`; the measured copy-budget table; ≥3 optimization iterations.
