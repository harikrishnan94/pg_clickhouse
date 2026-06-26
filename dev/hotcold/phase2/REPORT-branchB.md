# Hot-Cold Phase 2 — Branch B REPORT (copy reduction on the Arrow wire)

**Scope.** On the Branch-A Apache Arrow IPC wire + the Branch-0 io_uring/async substrate, drive the copy
count to the minimum and prove it end to end: **consumer zero-copy adoption** of the Arrow buffers into
`ColumnPtr`s (B-it2); a **lean direct-flatbuffer extraction** to attack the residual (B-it3);
**send-side zero-copy** (B-it4, a loopback measured-null by design); **producer 1-userspace-copy**
confirmation (B-it1); the **measured copy-budget table**; and `convertToFullColumnIfAdopted` keeping its
materialize-on-mutate + `ColumnNullable` recurse (D-HC-0206). Decisions D-HC-0206/0207/0208.

**Commits (this branch).** ClickHouse `streamed_table`: `f30ed9d6efc` (B-it2 zero-copy adopt),
`cf91028a9d7` (D-HC-0206), `b47ce3d1291` (B-it3 lean extraction), `bddb2d2646e` (D-HC-0208 default flip).
pg_clickhouse `streamed-table-shm-offload`: `854eacf` (L0014 + pre-reg + D-HC-0208), `81c1cb9` (B-it4
MSG_ZEROCOPY), `57e34b6` (B-it1 + copy-budget final), `63b96f2` (adversarial review + review-fix #1 +
this report), `1af9c95` (DoD checklist).

## Verdict: GREEN.
The copy-budget is **measured** and each ELIMINATED copy is **profile-proven**; the consumer pass-through
path adopts fixed-width + `LargeBinary` String **with zero per-column data copy and no realloc** (proven by
the −299 ms consumer-user-CPU drop on the String-heavy cell and parity on fixed-width); the **send-side**
zero-copy is honestly proven a **measured negative** on loopback via `SO_EE_CODE_ZEROCOPY_COPIED`; the
**recv-side** is single-copy (one kernel copy into the to-be-adopted buffer, no userspace recopy) with the
residual kernel recv copy reported as the dominant (~35%) cost — the real-NIC north star, not closable
here. **4 evidence-based iterations** logged — it2/it3/it4 (the ≥3 required optimization iterations) +
it1 (the 1-copy confirmation) + the D-HC-0206 capability; every claim backed by ≥3 converging
instruments; no new `DIFF`; the default path is unaffected. Independent adversarial review: **PASS, zero
blocking findings.**

## 1. Correctness (gated before every measurement)
- **End-to-end offload oracle:** `verify_offload.sh TRANSPORT=arrow` = **137/137 PASS** on every Branch-B
  binary state — B-it2 adopt-default, B-it3 lean-default, and the shipped it2-default (D-HC-0208) — and
  with `shm_arrow_lean_extract=1` forced (the adversarial reviewer re-ran both). Results byte-identical to
  native / SHM-adopt / SHM-copy / bespoke-TCP within the same fidelity bounds; joins/aggregates over
  adopted columns, EOS, producer-death/cancel, leak teardown. No new `DIFF`.
- **gtests = 15/15:** `ArrowStreamSource.*` 9/9 (lean default + it2 `ReadRecordBatch` adopt + both
  slow-fragmented variants + blocking ×2 + copying decode + the stock-`ArrowColumnToCHColumn` oracle),
  `TcpStreamSource.*` 4/4 (no bespoke regression), `AdoptedConvert.*` 2/2 (D-HC-0206 materialize-on-mutate +
  `ColumnNullable` recurse).

## 2. The measured copy-budget (the mandatory deliverable) — see `evidence/bB-copy-budget.md`
Per stage, end to end, labelled with the instrument that proves it:

| stage | copy? | how proven (measured) |
|---|---|---|
| PG heap → deform → column buffers + Arrow serialize | **1 (required)** | B-it1/L0016: the serialize is ONE cheap concat pass (`ArrowIpcEncoderEncodeRecordBatchImpl` 0.02% + `memcpy@plt` 0.46%, producer perf), NOT a scratch-then-copy; producer is DEFORM-bound (~20%), mirror of bespoke's one scratch copy |
| userspace → kernel (send) | loopback **1 (deferred)** / real-NIC **0** | B-it4/L0015: **`SO_EE_CODE_ZEROCOPY_COPIED` on every completion (zc_copied==zc_notifs 100%)** → loopback defers a copy; msg_zerocopy +7.3% slower = a measured NEGATIVE |
| kernel → userspace (recv) | loopback **1 (single-copy)** / real-NIC **0** | single-copy recv (one body buffer/msg, no userspace recopy); the kernel recv copy is the **DOMINANT ~35% of consumer CPU** (`__arch_copy_to_user`, perf) — irreducible (no NIC header/data split), the real-NIC north star |
| recv buffer → ColumnPtr — fixed-width | **0 (ELIMINATED)** | `createAdopted` aliases `buffers[1]` in place; fixed-width adopt vs bespoke at parity (≤2.2%) |
| recv buffer → ColumnPtr — String (`LargeBinary`) | **0 (ELIMINATED)** | B-it2: `cons_user` adopt 663 vs copy 962 = **−299 ms** on CB Q24; `offsets=&arrow_offsets[1]`, `offsets[0]==0` sentinel validated |
| recv buffer → ColumnPtr — Decimal128 / Nullable null_map | residual / n/a | Decimal128 16-byte-align copy-fallback (absent in swept data); Nullable null_map n/a (non-Nullable producer; D-HC-0206 recurse gtested) |
| ColumnPtr lifetime | — | one RetainToken/block, last-drop on chunk consume; leak oracle 137/137, in-flight bounded to ~K buffers/stream |

## 3. The optimization iterations (≥3 required; all pre-registered → measured ≥3 classes → verdict)
- **B-it2 — zero-copy adoption (L0012): PARTIAL WIN.** The consumer copy-decode is genuinely eliminated
  (CB Q24 `cons_user` −299 ms vs copy); fixed-width parity. Wall: arrow-adopt **+7.4% vs bespoke-tcp** on
  Q24 (the eliminated CPU was overlapped with the bandwidth-bound recv) — a logged prediction-miss → B-it3.
- **B-it3 — lean direct-flatbuffer extraction (L0014): MEASURED NULL.** Skips `ReadRecordBatch`'s per-block
  `arrow::Array` construction by walking the RecordBatch flatbuffer directly. Wall: lean **−0.5% vs the it2
  `ReadRecordBatch` adopt (within noise)**. perf: the decode path it eliminates is **only ~0.37% of consumer
  CPU** (present in it2, provably absent in lean — confirmed by the reviewer's standalone encoder test) → it
  cannot move the wall. **This FALSIFIES the L0012 hypothesis** that the +7% residual was the parse. The
  residual is NOT the decode, NOT wire size (arrow `send_bytes` +0.7%), and NOT the serialize concat (~0.5%,
  B-it1) — it is the **distributed cost of the standard Arrow framing** (per-block metadata message +
  `LargeBinary` offsets-prepend + recv-side bookkeeping), bounded to the widest String cell. Default
  `shm_arrow_lean_extract=0` (D-HC-0208): ship the standard-API path; lean is a tested alternative for the
  capable-NIC future. (Surfaced finding: `ColumnString::validateAdoptedOffsets()` is **14.4%** of consumer
  CPU — a safety scan common to all adopt transports, a future vectorization candidate.)
- **B-it4 — send-side `MSG_ZEROCOPY` (L0015): MEASURED NEGATIVE (the pre-registered loopback null).** Every
  `SO_EE_ORIGIN_ZEROCOPY` completion carries `SO_EE_CODE_ZEROCOPY_COPIED` (zc_copied==zc_notifs 100%) → the
  kernel defers a copy; msg_zerocopy is **+7.3% SLOWER** than io_uring-send on Q24. The authoritative
  send-side proof is the errqueue flag (NOT "absence of copy_from_user"); the real elimination is real-NIC-only.
- **B-it1 — producer 1-userspace-copy (L0016): CONFIRMED.** The serialize is one concat pass (profile +
  code-structure + wire bytes); the producer is deform-bound; the serialize copy == 1/block, the mirror of
  bespoke's scratch copy.

## 4. Performance: W=8, arrow-adopt vs bespoke-TCP vs SHM-adopt (N=5, idle host, noise band `max(5%,1σ)`)
| cell | arrow-adopt | bespoke-tcp | SHM-adopt | arrow vs tcp | verdict |
|---|---|---|---|---|---|
| TPC-H Q1 (agg) | 1581 | 1547 | 1424 | +2.2% | parity |
| TPC-H Q6 (filter+sum) | 1244 | 1226 | 1158 | +1.5% | parity |
| TPC-H Q19 (join) | 2254 | 2216 | 2042 | +1.7% | parity |
| ClickBench Q2 (numeric agg) | 404 | 404 | 402 | ~0% | parity |
| ClickBench Q24 (`SELECT *` ~8 GB, String-heavy) | 1241 | 1154 | ~916 | +7.5% | bounded — standard-framing cost |
(lean == it2 within noise — L0014; the it2 `ReadRecordBatch` adopt is the shipped default.)

**Fixed-width: parity** (the floor — Branch B's data path equals bespoke-TCP). **ClickBench Q24** is +7.5%
vs bespoke-TCP: mechanism-explained (B-it3/B-it1) as the distributed standard-Arrow-framing cost, NOT an
eliminable copy. The gap to SHM-adopt (~+325 ms) is the **kernel recv copy** (~240 ms, the irreducible
single-copy recv, ~35% of consumer CPU) + the framing (~85 ms) — NOT closable on this NIC-less loopback host.

## 5. Intention (1) — bespoke retire-ability
Unchanged from Branch A and preserved: the `arrow:` transport carries a standard, standalone Arrow IPC
stream a third party / Arrow Flight could read; the zero-copy adopter consumes it with no per-column copy.
The bespoke `TcpFrame.h` is retire-able by flipping the default transport. Branch B did not regress this:
the zero-copy adopt path is correct against the stock `ArrowColumnToCHColumn` oracle.

## 6. Intention (2) — best performance via copy reduction (measured best-vs-today delta + mechanism)
- **Today (bespoke-TCP) already did single-copy-recv + zero-copy-adopt**, so Branch B's data path **equals**
  it on fixed-width (parity, the pre-registered floor) and removes Branch A's consumer copy-decode (−299 ms
  consumer CPU on the String-heavy cell — the real CPU/energy win).
- **The widest String cell is +7.5% vs bespoke-TCP** — and this is the honest, fully-instrumented result:
  it is the distributed cost of speaking a **standard** format (intention 1), NOT a copy Branch B failed to
  remove. B-it3 proved it is not the decode (~0.4%); B-it1 proved it is not the serialize concat (~0.5%);
  L0014 proved it is not wire size (+0.7%). The DoD requirement "end-to-end ≥ today's TCP (mechanism
  explains any shortfall)" is met: fixed-width ≥ parity, and the String-cell shortfall is mechanism-explained.
- **The genuine remaining lever is the kernel recv copy (~35% of consumer CPU, +240 ms on Q24)** — the
  irreducible single-copy recv on this NIC-less host; eliminating it (`TCP_ZEROCOPY_RECEIVE` / io_uring
  `RECV_ZC`) needs NIC header/data split (review §3) and is the **real-NIC north star**, designed-for and
  reported, not claimed here. The send-side zero-copy is a proven loopback negative (B-it4).

## 7. Evidence convergence (≥3 independent classes) + adversarial review
**Per claim:** end-to-end W=8 wall (median+sd) · consumer CPU split (query_log `cons_user`/`cons_sys`) ·
perf profiles (the decode 0.37%, the recv-copy 35%, the producer serialize 0.5%, the validateAdoptedOffsets
14.4%) · the `SO_EE_CODE_ZEROCOPY_COPIED` errqueue flag · gtests + the stock-reader oracle. **Independent
adversarial review (evidence/ADVERSARIAL-REVIEW.md): PASS, zero blocking findings** — the reviewer re-ran
all gates (15/15 gtests, 137/137 verify on default AND forced-lean), wrote a standalone test against the
real producer encoder confirming the lean walk genuinely runs (the null is real, not a silent bail),
verified the msg_zerocopy buffer-reuse has no corruption window, and re-derived every wall delta; finding
#1 (a defensive ENOBUFS hot-spin) was fixed in-branch, #2 (the un-fused deform+serialize) documented.

**Branch B GREEN.**

## 8. Definition-of-Done checklist (PROMPT.md §"Definition of done") — Branch B
- [x] **Copy-budget table measured + each ELIMINATED copy profile-proven** — §2 + `evidence/bB-copy-budget.md`;
  perf shows the absence of per-column `memcpy`/`cloneResized` on the adopt path and the −299 ms consumer-CPU
  drop on the String-heavy cell.
- [x] **(a) No per-column userspace data copy on the pass-through path** — fixed-width + String adopted in
  place (`adoptFixedRaw`/`adoptStringRaw`); String `offsets = &arrow_offsets[1]` with `array.offset()==0 &&
  arrow_offsets[0]==0` validated (and `validateAdoptedOffsets()` on the emitted columns). Proven: parity on
  fixed-width + −299 ms on String + perf (no per-column data memcpy; `ReadRecordBatch`/`cloneResized` absent).
- [x] **(b) No per-column data allocation / zero realloc** — `createAdopted` is alias-only (structural); the
  only allocation per block is the one recv body buffer (`allocFrameBuffer`) + the small column shells; no
  `cloneResized`/realloc on the pass-through path (perf-confirmed absent). Nullable null_map: n/a (non-Nullable
  producer); the capability + recurse are gtested.
- [x] **(c) `convertToFullColumnIfAdopted` materialize-on-mutate + `ColumnNullable` recurse (D-HC-0206)** —
  NOT a blanket no-op; gtested `AdoptedConvert.FixedWidthMaterializesAndIsMutable` +
  `.NullableRecursesIntoAdoptedNested` (2/2), end-to-end-neutral (137/137).
- [x] **(d) Prompt drop / bounded memory** — one `RetainToken`/block, freed on the chunk's last-drop when
  consumed downstream; in-flight bounded to ~K recv buffers/stream (structural, shared with Branch 0/A's
  validated lifetime machinery); the leak oracle (`verify_offload` teardown) = 137/137 (no leaked
  workers/sockets/fds) on every binary state. (A dedicated adopt-path RSS-peak curve is inherited from
  Branch 0/A — the buffer-lifetime machinery is unchanged; Branch B only changes WHO holds the buffer.)
- [x] **Send-side zero-copy honestly proven (loopback measured null via `SO_EE_CODE_ZEROCOPY_COPIED`)** —
  B-it4/L0015: zc_copied==zc_notifs 100% (deferred copy); +7.3% wall (negative); NOT claimed as an elimination.
- [x] **Recv-side single-copy recv + residual kernel copy reported + capable-NIC design recorded** — one
  kernel copy into the to-be-adopted buffer, no userspace recopy; the residual kernel recv copy is the
  measured dominant ~35% consumer cost (no NIC header/data split — review §3); `TCP_ZEROCOPY_RECEIVE`/io_uring
  `RECV_ZC` recorded as the real-NIC north star, not claimed.
- [x] **End-to-end ≥ today's TCP at W=8 (mechanism explains any shortfall)** — fixed-width parity (≥); the
  widest-String-cell +7.5% shortfall is mechanism-explained (distributed standard-Arrow-framing cost: NOT the
  decode [B-it3 ~0.4%], NOT wire size [+0.7%], NOT the serialize concat [~0.5%]).
- [x] **≥3 evidence-based optimization iterations logged** — L0012 (it2), L0014 (it3), L0015 (it4) + L0016
  (it1 confirm) + D-HC-0206; 4 iterations, each pre-registered → gated → measured ≥3 classes → verdict.
- [x] **Every claim ≥3 converging instruments; every deviation logged; reproduction recorded** — §7;
  10-REPRODUCTION.md; the methodology log is the append-only audit trail.
- [x] **Small reviewable patches, each green before commit** — 8 Branch-B commits across the two repos,
  history is the audit trail.
- [x] **Independent adversarial review passed** — `evidence/ADVERSARIAL-REVIEW.md` Branch B = PASS, zero
  blocking; finding #1 fixed in-branch, #2 documented.

**Branch B: Definition of Done — MET. GREEN.**
