# Hot-Cold Phase 2 — Branch A REPORT (Apache Arrow IPC wire)

**Scope.** Replace the bespoke `TcpFrame.h` block payload with a **standard Apache Arrow IPC stream**
(Schema message + one RecordBatch message per block + EOS) on a new, separately-selectable transport
`arrow:<host>:<port>` (the bespoke `tcp:` stays selectable). Producer emits Arrow via vendored
**nanoarrow 0.8.0**; consumer reads it with **Arrow C++** and (Branch A) decodes by **copying**.
Correctness first, then per-cell-class performance: fixed-width **parity** with bespoke TCP, String-heavy
a **bounded** regression recovered in Branch B. Decisions D-HC-0201/0202/0203/0205/0207.

**Commits.** pg_clickhouse `46dfb93` (vendor nanoarrow), `34242a8` (producer `shm_arrow` encoder +
`shm_wire`), `0c9c732` (`arrow:` plumbing + producer integration), `719eeda` (methodology + verify_offload
arrow mode); ClickHouse `f2128e5a7fb` (Arrow IPC consumer + gtest).

## Verdict: GREEN.
Fixed-width/numeric cells at parity (no fixed-width regression — the blocking condition); the one
String-heavy cell regresses **+15.9%**, inside the **pre-registered +10–30%** bound, mechanism-explained
and slated for Branch-B recovery; correctness converges across 4 independent instrument classes; no new
`DIFF`; no bespoke regression.

## 1. Correctness (gated before timing) — 4 converging classes
- **End-to-end offload oracle (authoritative):** `verify_offload.sh TRANSPORT=arrow` = **137/137 PASS**
  (`/tmp/bA_verify_arrow.log`). Producer nanoarrow → Arrow C++ consumer, results **byte-identical to
  native and to SHM-adopt / SHM-copy / bespoke-TCP** within the same documented fidelity bounds (every
  fixed-width type, `LargeBinary` String, Decimal, raw Date/DateTime/DateTime64, empty strings, multi-
  source SEMI/ANTI joins each on its own Arrow stream, subset projection, `count()`, EOS, producer-death/
  cancel, leak teardown). The oracle asserted `ShmCopiedBlocks ≥ 1` per heavy fragment — the **arrow
  transport actually ran**, not a base scan.
- **gtests `ArrowStreamSource.*` = 4/4 PASS:** `DrainsSchemaAndBatches`, `DrainsBlocking`,
  `AsyncResumesAcrossPartialMessages` (~1.02 s ≈ fragment-rate — the resumable 3-phase prefix/metadata/body
  Arrow recv reassembles a message straddling schedule cycles, no busy-spin), and
  **`DecodeMatchesStockArrowReader`** — the stock **`ArrowColumnToCHColumn`** decoder agrees cell-for-cell
  with the custom Branch-A decode AND the known values (uint16 → Date via the header type hint).
- **Cross-implementation encode check:** the gtest producer uses Arrow C++'s OWN `RecordBatchStreamWriter`
  (not nanoarrow), proving the consumer reads *standard* Arrow IPC; the end-to-end oracle independently
  proves the nanoarrow encode side. Two different Arrow encoders, one consumer.
- **No bespoke regression:** `TRANSPORT=tcp` = **137/137** on the new binary; `TcpStreamSource.*` = 4/4
  (loopback microbench 7.47 GB/s, unchanged). The async recv/wake/teardown machinery is shared, byte-equal.

## 2. The Arrow wire (intention 1 — making the bespoke frame retire-able)
The `arrow:` transport carries a **valid, standalone Arrow IPC stream** (D-HC-0207): a Schema message,
then one RecordBatch message per block, then the IPC EOS marker — **no** bespoke `TcpHandshakeHeader` /
`TcpBlockHeader`. A third-party Arrow reader / Arrow Flight could consume it. Per-type mapping (D-HC-0203):
numerics → matching Arrow primitive; `String` → **`LargeBinary`** (int64 offsets; producer builds the
`N+1` offsets `[0,end0..endN-1]` from its `N` END offsets); Date/DateTime/DateTime64 → raw
`uint16`/`uint32`/`int64`; Decimal32/64 → raw `int32`/`int64`; Decimal128 → `FixedSizeBinary(16)`. The
exact CH type (Decimal scale, DateTime64 precision, Date vs UInt16) is recovered from the
`streamed_table()` SQL schema argument (authoritative), which the Arrow Schema message is validated
against. **Producer** = vendored nanoarrow IPC encoder over a hand-built C-Data-Interface `ArrowArray`
viewing the deformed buffers zero-copy. **Consumer** = Arrow C++ `Message::Open` + `ReadRecordBatch`, then
(Branch A) a COPYING decode into owned CH columns of the SQL type. The bespoke `TcpFrame.h` path is
**retire-able on the `arrow:` transport** — kept selectable only for the parity baseline during the
migration; flipping the default later retires it without deleting code mid-branch.

## 3. Performance: W=8, Arrow-TCP vs bespoke-TCP vs SHM-adopt (`evidence/bA-overhead-table.md`)
Pre-registered: fixed-width **parity**; String-heavy **+10–30%** (copying decode), recovered in Branch B.
N=5 warm, idle host (load 0.12), noise band `max(5%, 1σ)`.

| cell | arrow (sd) | tcp (sd) | adopt (sd) | **arrow vs tcp** | verdict |
|---|---|---|---|---|---|
| TPC-H Q1 (agg, decimals+dates) | 1580 (11) | 1559 (10) | 1430 (3) | **+1.3%** | parity |
| TPC-H Q6 (filter+sum)          | 1243 (8)  | 1235 (6)  | 1170 (4) | **+0.6%** | parity |
| TPC-H Q19 (join)               | 2275 (8)  | 2222 (6)  | 2035 (6) | **+2.4%** | parity |
| ClickBench Q2 (numeric agg)    | 404 (2)   | 406 (4)   | 403 (1)  | **−0.5%** | parity |
| ClickBench Q24 (`SELECT *` ~8 GB) | 1331 (16) | 1148 (10) | 914 (8) | **+15.9%** | bounded regression (within +10–30%) |

**Fixed-width/numeric: 4/4 parity** within the noise band → no fixed-width regression (the blocking
condition is satisfied). **ClickBench Q24** (the widest, transfer-dominated, String-heavy cell) regresses
**+15.9%**, squarely inside the pre-registered +10–30% band.

**Mechanism (converging classes 2 + 3).** The Q24 regression is **consumer-side copy-decode**: arrow's
decode rebuilds the ~8 GB `ColumnString` chars+offsets (and the other columns) from the Arrow buffers —
`cons_user` +283.7 ms, `cons_sys` +199.3 ms vs bespoke — where bespoke TCP **adopts** the recv buffer
zero-copy. On fixed-width cells the same copy is a negligible fraction of the agg/filter compute
(`cons_user` +11–60 ms) → parity. The wall decomposes additively into separately-attributable layers:
`adopt 914 + kernel-recv-copy (→tcp 1148, +26%) + consumer-copy-decode (→arrow 1331, +16%)`. **Branch B
(zero-copy adopt of the `LargeBinary` values+offsets) removes the copy-decode layer → bespoke-TCP
parity (~1148 ms)**; the residual gap to SHM-adopt is the kernel recv copy, NOT closable on this NIC-less
loopback host (feasibility review §3 — the real-NIC north star).

## 4. Prediction vs observation
- Fixed-width parity → **observed 4/4 parity (≤2.4%, within band).** ✓
- String-heavy +10–30% copying-decode regression → **observed Q24 +15.9%, inside the band.** ✓
- Mechanism = consumer copy-decode (not the producer, not the wire framing) → **confirmed by the
  consumer-CPU split + the gap-to-adopt decomposition.** ✓
- No new `DIFF` vs any mode → confirmed (`cmp.py` verdicts identical, all `exact`/the same `approx`). ✓

## 5. Evidence convergence (≥3 independent classes)
**Correctness:** (1) end-to-end `verify_offload` 137/137; (2) gtests + the stock `ArrowColumnToCHColumn`
decode oracle; (3) the Arrow-C++ reference-encoder cross-check; (4) no-bespoke-regression. **Performance:**
(1) end-to-end W=8 wall (median+sd); (2) consumer CPU split (`cons_user`/`cons_sys`); (3) the additive
gap-to-adopt cost decomposition. Direction + magnitude converge on: **Arrow wire correct, fixed-width at
parity, String-heavy a bounded consumer-copy-decode regression recovered in Branch B.**

## 6. Closing — intention (1) and the handoff to Branch B (intention 2)
**Intention 1 (retire the bespoke format):** delivered for the `arrow:` transport — a standard Arrow IPC
stream a third party can read, cross-checked against the stock Arrow C++ reader; the bespoke `TcpFrame.h`
is retire-able by flipping the default. **Intention 2 (best perf via copy reduction):** Branch A is the
COPYING decode by design — fixed-width already at parity; the String-heavy +15.9% is the single consumer
copy-decode layer that **Branch B drives out** via zero-copy adoption of the Arrow `LargeBinary`
values+offsets (`offsets = &arrow_offsets[1]`, validate `offset()==0 && offsets[0]==0`), with the
`convertToFullColumnIfAdopted` mutate-materialization + `ColumnNullable` recurse (D-HC-0206) and the
send-side measured-null + single-copy-recv accounting. **Branch A GREEN; proceed to Branch B.**
