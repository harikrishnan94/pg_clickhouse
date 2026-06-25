# Full SHM-offload of TPC-H — Decision log

Running log of: (a) decisions taken without a human where the spec left an
ambiguity (with alternatives + rationale), (b) every **intentional fidelity
deviation** accepted to unlock coverage (bounded + quantified), and (c) every
query concluded **cannot** be offloaded, with the root-caused blocking mechanism.

This is the artifact a human reviews later. It is append-only in spirit; entries
are dated. "Offloaded" always means proven from ClickHouse `system.query_log`
(new `streamed_table()` `QueryFinish`, `ShmAdoptedBlocks >= 1`, heavy fragment in
the dispatched SQL), never from the PostgreSQL plan alone.

---

## Environment of record

- DB `tpch_sf10`, schema `pg`, SF10 (lineitem ~60M rows). PG 18 @ 127.0.0.1:5432.
- ClickHouse patched `streamed_table` build, RUN_ID=tpchcb, HTTP 127.0.0.1:21002.
  **The manifest `CH_PID` is stale** — the live pid is resolved from the listening
  port (`ss -ltnp | grep :21002`), per the spec.
- `ubuntu` OS login has no PG role; all psql runs as the `postgres` OS user.
- Offload engaged via the `shm_set_block` SET block in `dev/bench/bench-common.sh`
  (`enable_shm_offload=on`, `shm_min_rows=0`, `session_settings` incl.
  `allow_experimental_streamed_table_function 1`, `join_use_nulls 1`,
  `group_by_use_nulls 1`, `final 1`, and a unique `log_comment` tag).

---

## D0001 — 2026-06-24 — Phase-0 oracle: "fully offloaded" classification rule

**Decision.** A query is **fully offloaded** only if BOTH independent sources agree:
1. ClickHouse `system.query_log` (correlated by unique `log_comment` tag) shows a
   `streamed_table()` `QueryFinish` with `ShmAdoptedBlocks >= 1`, and the dispatched
   CH SQL contains the query's *heavy* operator (the GROUP BY / JOIN / aggregate,
   not merely `SELECT cols FROM streamed_table(...)`), AND
2. the PostgreSQL `EXPLAIN` plan shows the heavy operator is **not** re-done above
   the topmost `Custom Scan (ClickHouseShmScan)` (no residual `Aggregate` /
   `HashAggregate` / `*Join` doing the query's heavy lifting on top).

`scan-only` = a `ClickHouseShmScan` fires (oracle 1 partial) but PG still performs
the join/aggregate on top (oracle 2 fails). `not-offloaded` = no ClickHouseShmScan.

**Alternative considered.** Trust the PG plan's node label alone. **Rejected** —
the spec's oracle explicitly forbids concluding offload from the PG plan; the
query_log is the authority, and we additionally require the PG plan to confirm the
heavy fragment is not duplicated in PG.

---

## D0002 — 2026-06-24 — Phase-0 measured state: join offload broken at SF10

**Finding.** Running all 22 through the oracle (`evidence/phase0/`) shows offload
today gives a **correct answer for only 7/22** queries (Q1 native; Q4 fully; Q6,
Q10, Q13, Q15, Q18 scan_only-exact). The other 15 return **wrong/empty/NULL** (9)
or **error** (5). The dominant cause is a **ClickHouse consumer-side bug**: the
join pipeline's `SimpleSquashingTransform` calls a non-const accessor
(`reserve()`) on a zero-copy *adopted* SHM column without COW-materialising
(`IColumn::mutate()`) — guard at `/home/ubuntu/ClickHouse/src/Columns/IColumn.cpp:64`.
With filters the exception is swallowed → the join side yields 0 rows (silent
wrong answer); without filters it surfaces as `Code: 164 ... (READONLY)`. The
dispatched join SQL is verified correct, so this is **not** a deparse bug. CH
aggregation over adopted columns is unaffected (Q4 + single-table GROUP BY
count(*) over 60M both correct).

**Decision (phasing).** Sequence by risk/value:
1. **Phase 1 — single-table Decimal/numeric aggregate output unlock** (Q1, Q6).
   No join, so it cannot hit the READONLY bug; proven safe because single-table
   agg pushdown already works at scale. Establishes the code-change → build →
   correctness-gate → W-sweep → review → commit machinery and gives a guaranteed
   correct fully-offloaded win.
2. **Phase 2 — fix the ClickHouse adoption-layer READONLY bug** (join squashing).
   The foundational unlock for ~10 join queries. Requires a ClickHouse rebuild +
   keeping its `streamed_table` tests green (ASan/TSan).
3. **Phase 3 — Decimal aggregate output over (now-correct) joins** (Q3, Q5, Q7,
   Q8, Q9, Q10, Q14, Q19).
4. **Phase 4+ — subqueries** (Q2, Q11, Q17, Q18, Q20, Q22), **anti-joins**
   (Q16, Q21, Q22), **count(distinct)** (Q16), **rescan** (Q16).

**Alternative considered.** Do the CH join fix first (more queries). **Rejected
as Phase 1** because it is the highest-risk change (CH rebuild, complex
adoption-layer semantics); doing the clean single-table win first de-risks the
pipeline and yields an immediately committable, evidence-backed result. The CH
fix is Phase 2 — next, not deferred.

## D0006 — 2026-06-25 — Phase 3: numeric aggregate output over joins unlocked

Removed the `!ifpinfo->is_heap_offload` gate in `shm_create_upper_paths` so a
numeric `sum`/`avg` output pushes over a (now-correct) SHM-offload join, not just
a single table. **9 queries now fully offload** (heavy fragment in ClickHouse, no
residual PG aggregate): Q1, Q3, Q4, Q5, Q6, Q7, Q10, Q12, Q19. Values match native
within the bounded Decimal display-scale / avg→Float64 deviations (e.g. Q5 exact
`INDIA|536862587.9995…`; Q3/Q10 differ only by Decimal trailing zeros such as
`439855.3250` vs `439855.325`, numerically identical). verify_offload.sh 137/0.

**Residual deadlock (#1 remaining blocker) — Q8, Q9, Q11, Q14.** When the
aggregate is pushed over *these* joins the ClickHouse query freezes: `read_rows`
stuck at exactly one ring block (1048576) on the join **build** side's producer,
which trips `SHM_PRODUCER_STALL` at 30s. Root-caused as a genuine multi-ring
`streamed_table` JOIN-build consumption deadlock — **independently of** the
READONLY fix, `max_threads` (tested `total_producers+16`; no change), `final`, and
`group_by_use_nulls` (all ruled out by experiment). It is a deeper ClickHouse
pipeline/`PollableShmSource` scheduling issue: the hash-join **build** side adopts
one block then waits on a source whose producer is blocked on a ring the consumer
never drains. **Additionally ruled out: it is NOT the UNION-ALL multi-ring** — Q14
with `parallel_workers=1` per table (a bare
`streamed_table(lineitem) JOIN streamed_table(part)`, one ring each, no UNION ALL)
still freezes (read_rows stuck at 262144 on the part build side). So it is a
fundamental 2-source `streamed_table` hash-join-build deadlock for this shape,
while 3–6-way joins (Q3, Q5, Q7, Q10) work — i.e. shape-specific, not arity. A fix
needs CH-side pipeline work (the build-side `PollableShmSource` consumption /
hash-join build scheduling) and is deferred. Evidence: `dev/tpch/evidence/phase0/q{9,14}.on.err`, live probes showing
frozen read_rows.

**Trade-off decision (logged).** Q8 and Q11 were scan_only-CORRECT after Phase 2
(bare join streamed back, aggregate in PG); enabling agg-over-join (Phase 3) makes
them attempt the full push and hit the deadlock → they now error. This is a
deliberate trade-off favoring the spec's PRIMARY goal ("maximize offload coverage
aggressively / fully push down" for a perf-measurement exercise): +5 fully-offloaded
revenue joins (Q3,Q5,Q7,Q10,Q19 — the best showcase of offload's join performance)
at the cost of Q8,Q11 regressing from scan_only-correct to deadlock-error. Both
states are "not a correct full offload"; at the ORIGINAL baseline Q8/Q11 were wrong
(bpchar). Alternatives: (i) revert Phase 3 → 13 correct but only 4 fully-offloaded,
no regression; (ii) per-shape gate → cannot predict the deadlock statically. The
deadlock fix (when done) unlocks Q8/Q9/Q11/Q14 fully and erases the trade-off.

## D0007 — 2026-06-25 — Phase 2/3 adversarial review: PASS + literal-trim fix

Independent reviewer (fresh context) attacked join correctness, bpchar safety, the
READONLY fix, the deadlock diagnosis, and regressions. **No blocking findings for
TPC-H.** Confirmed: Q3/Q5/Q6/Q7/Q19 exact (maxrel 0), Q1 avg 1.57e-16, Q10
set-exact (only an unordered tie reorder), oracle not fooled (full join+GROUP BY+sum
in dispatched CH SQL, no residual PG aggregate), READONLY fix sound (67/67 adoption
unit tests; a no-filter 3-table join returns 100000 == native), Q8/Q9/Q11/Q14
deadlock genuine (read_rows frozen at 1 block, clean teardown), no Q1/Q6 regression.

**One non-blocking finding addressed:** the bpchar fix trimmed the COLUMN but not
the comparison LITERAL, so `col = 'literal '` (literal with a trailing blank)
under-matched under offload (e.g. `n_name = 'GERMANY '` → 0 vs native 1). No TPC-H
query carries a trailing-blank literal (verified all 22), so it was not a TPC-H
correctness issue, but it is a real general-SQL gap. **Fixed:** `deparseConst` now
also strips trailing blanks from bpchar (BPCHAROID) constants, so both sides are
trimmed consistently. Verified `n_name = 'GERMANY '` → 1 == native; verify_offload
137/0. (Closes the robustness follow-up noted in D0005.)

## D0008 — 2026-06-25 — Phase 4: hash-join-BUILD deadlock ROOT-CAUSED & FIXED

The Q8/Q9/Q11/Q14 deadlock from D0006 is fixed. **One CH-side change**, in
`/home/ubuntu/ClickHouse/src/Interpreters/HashJoin/HashJoin.cpp` free function
`materializeColumnsFromRightBlock` (the single chokepoint for `HashJoin::addBlockToJoin`
AND `ConcurrentHashJoin::addBlockToJoin`): materialise build-side adopted columns with
`actual_column = actual_column->convertToFullColumnIfAdopted();` (no-op for non-adopted).

**Root cause (3 converging evidence classes; raw logs `dev/tpch/evidence/phase-deadlock/`).**
The hash-join BUILD (right) side stores its blocks for the whole query. Those columns
arrive from `streamed_table()` as zero-copy *adopted* columns aliasing a slot in the
producer's bounded SHM ring (K=4 slots/ring, `PGCH_SHM_RING_DEPTH_K`). The hash table
retained them → the slot's `RetainToken` never released → slot never returned to EMPTY.
With only 4 slots/ring, once the build table outgrew the ring the PG producer blocked
forever in `publish_block` (`shm_producer.c:622`, the `while state!=EMPTY` wait) → the
build never completed → the probe never started → the CH consumer parked in
`Epoll::getManyReady(timeout=-1)`. Evidence at the freeze (Q14, lineitem×part):
- Liveness: `system.processes.read_rows` frozen at 1,048,576 (=16×65536) while elapsed
  climbed 26.9s→174s (`dl_q14_diag.liveness.log`).
- CH stacks: executor master in `ExecutorTasks::processAsyncTasks`/`PollingQueue`
  epoll-wait; all 11 workers parked in `tryGetTask` (`dl_q14_diag.gdb.txt`).
- PG stacks: all 11 producers blocked in `publish_block` ring-full wait
  (`dl_q14_diag.producers.txt`).
- Ring states (`dl_q14_diag.ringstates.txt`): the 16 `part` (build) slots all
  `PUBLISHED/seq1/refcount=1` (drained AND retained); the 28 `lineitem` (probe) slots
  `PUBLISHED/refcount=0` (never drained). 16×65536 = read_rows exactly.
Canonical symptom reproduced with default 30s stall: `Code 781 SHM_PRODUCER_STALL
'/pgch_..._2_2_0'` on the **part build** producer (`dl_q14_default.err`).
**Why 3–6-way joins (Q3/Q5/Q7/Q10) never deadlocked:** their build (right) sides are
small/filtered dimension tables that fit inside the retained ring capacity; only build
sides that *exceed* the ring (unfiltered part 2M; partsupp 8M) trip it. Shape-specific,
not arity. (Q19 is also lineitem×part but its selective part filter is pushed, so its
build fits — it worked pre-fix.)

**The fix is correct & necessary, not a workaround.** A hash table that retains its
build blocks for the whole query cannot safely alias a *bounded streaming ring*; it must
OWN the data. Materialising on the build side bounds SHM residency to the in-flight
block, so slots recycle as the build advances and the build completes regardless of size.
The probe/left side stays zero-copy (the real win — lineitem is the huge probe). NOT a
producer-side change and does NOT raise `shm_source_stall_timeout_ms`.

**Results (oracle = CH query_log; native compared per query). The deadlock is gone for
ALL FOUR (read_rows advances past 1,048,576; no Code 781; clean teardown each run). But
"fully offload CORRECTLY" holds only for Q9 and Q14; Q8 and Q11 reveal SEPARATE residual
bugs (below).**
- **Q14** ✅ fully offload + correct: read_rows 61,986,052, ShmAdoptedBlocks 961; full
  JOIN+sum in CH; native `16.6475949416150953` vs offload `16.647594941615093` (rel
  ~1.4e-16, F3-class).
- **Q9** ✅ fully offload + correct: read_rows 85,086,077, blocks 1335, 175 rows; the
  full 6-way JOIN+GROUP BY+sum is pushed; diff vs native is ONLY Decimal display-scale
  trailing zeros (F1, normalized diff = 0).
- **Q8** ⚠ pushes the full JOIN+aggregate to CH (oracle fires: read_rows 78,586,107,
  blocks 1238, ch_join+ch_grpby, pg_agg=0) but the RESULT is DEGRADED by a *separate*
  CH Decimal-division issue (below) — NOT "fully offload correctly".
- **Q11** ⚠ deadlock GONE — now dispatches the full JOIN+GROUP BY+HAVING — but ERRORS
  (Code 456, returns no rows) on a *separate* HAVING-subquery param-binding bug (below).

**Regression evidence.** Adoption unit tests 84/84 (incl. new
`Ac3AdoptionProof.BuildSideRetainedAdoptedColumnsAreMaterializedAndDrainTheRing`, which
streams 120 blocks through a K=4 ring while a build-like consumer retains every
materialised block and proves all ring slots release). `verify_offload.sh` 137/0, no
leaks. The 9 prior queries (Q1,Q3,Q4,Q5,Q6,Q7,Q10,Q12,Q19) still fully offload with the
same documented Decimal/Float/ordering fidelity. Single-table queries (Q1,Q6) never reach
the changed (join-only) function. W-sweep: see RESULTS (build-side copy is of the small
build tables only; probe path unchanged).

**Two residual blockers UNMASKED by the fix (SEPARATE from the deadlock; logged, deferred).**
- **Q8 — CH Decimal/Decimal division scale.** mkt_share = `sum(case…)/sum(volume)` is
  Decimal/Decimal; CH gives the quotient the dividend's scale (4) → `0.0388` vs native
  `0.03882014251433219622` (~5e-4 rel). This is a deparse fidelity gap (CH Decimal-division
  semantics), NOT the deadlock. (Q14's ratio is Float64 because of its `100.00 *` literal,
  so Q14 is exact.) Fix would deparse Decimal `/` via `toFloat64` (matching the accepted
  avg→Float64 policy); deferred to avoid touching the deparse hot path in this minimal
  deadlock fix.
- **Q11 — HAVING scalar-subquery param unbound.** The non-correlated HAVING subquery
  becomes a PG `PARAM_EXEC` (InitPlan); the deparser emits `{p1:Decimal}` but the SHM
  one-shot HTTP dispatch never binds it → `Code 456 Substitution 'p1' is not set`. This is
  the Phase-4 subquery-param-binding gap (the SHM dispatch must evaluate the InitPlan in PG
  and bind/inline the value), NOT the deadlock. Deferred.

**Known limitation of the fix (tracked).** The fix bounds SHM retention at the hash-join
BUILD chokepoint only; the probe/left side stays zero-copy adopted (intentional — it is
the large fact table and is consumed-and-released block by block). A future plan that
*fully buffers a streamed probe side* before a blocking operator (sort-merge join,
window/ORDER BY over a streamed source feeding a blocking node, certain self-joins) could
re-exhaust the K=4 ring the same way. It is correct for all current hash-join shapes; a
general "bounded SHM retention" guarantee would require materialising at every operator
that can retain > ring-depth blocks. Logged as a known limitation, not a regression.

**Evidence note (variance control).** The first back-to-back `eligibility-scan.sh` runs
(`evidence/phase-deadlock/elig/`, `elig9/`) were taken under background-worker
registration contention (multi-source queries needing many producers raced
`max_worker_processes=32`; the worker pool was also shared with a concurrent adversarial
reviewer), so their `SUMMARY.md` shows transient `scan_only?`/`ON_ERR` and raw unsorted
`DIFF(...)` that DO NOT reflect steady-state behaviour. The authoritative results above
were taken in ISOLATION (one query at a time, idle host) and re-confirmed by an
independent reviewer: Q9/Q14 fully offload + correct; Q10's raw `DIFF(200846)` collapses
to 0 after sort + CHAR-pad/trailing-zero normalisation. Clean isolated re-runs are in
`evidence/phase-deadlock/` (the `dl_q*_chk` / `dl_q14_fixed` artifacts and the clean
W-sweep `wsweep-clean/`).

## Queries provisionally NOT-YET-offloadable (root-caused, pending phase work)

- **CH join READONLY/empty (Phase 2):** Q2, Q3, Q5, Q7, Q8, Q11, Q12, Q17, Q19, Q20.
- **CH hash-join-BUILD deadlock (Phase 4 — FIXED in D0008):** ~~Q9, Q14~~ now fully
  offload; Q8 fully offloads (residual ratio-precision, D0008); Q11 deadlock gone
  (residual HAVING-subquery param, D0008). Q21 join-build no longer deadlocks either,
  but Q21 remains blocked by its anti-join + correlated EXISTS subqueries (Phase 4).
- **CH Decimal/Decimal division scale (deparse fidelity, Phase 4):** Q8 mkt_share (D0008).
- **HAVING/scalar-subquery PARAM_EXEC not bound by SHM dispatch (Phase 4):** Q11 (D0008),
  and the related correlated-subquery family below.
- **rescan unsupported (Phase 4):** Q16.
- **correlated-subquery param unbound (Phase 4):** Q22.
- **anti-join rejected `fdw.c:1865` (Phase 4):** Q16 (NOT IN), Q21, Q22 (NOT EXISTS).

## D0003 — 2026-06-24 — Phase 1: single-table Decimal/numeric aggregate output unlocked

**Change.** `src/shm_customscan.c` `shm_create_upper_paths`: the NUMERICOID-output
decline is now gated to **join inputs only** (`!ifpinfo->is_heap_offload`). A
single-table aggregate over a heap-offload base relation now pushes its numeric
`sum`/`avg`/`min`/`max` output to ClickHouse. Unlocks **Q1, Q6** to fully offload
(oracle-proven: Q1 CH SQL carries `sum×4, avg×3, count(*) GROUP BY`,
read_rows=59986052, ShmAdoptedBlocks=927, PG plan `Sort → CustomScan` with no
residual aggregate; Q6 likewise). Aggregate-over-join numeric output stays
declined until the Phase-2 ClickHouse join fix.

**Regression test.** `test/shm/verify_offload.sh` previously asserted byte
identity; the pushed Decimal aggregates legitimately differ in display scale and
`avg`→Float64. Added `check_result_equiv`/`rows_equiv`: exact match else
numeric value-equivalence within a 1e-9 relative tolerance, **printing the
observed max deviation** so it is never hidden. Suite is green: 137 PASS / 0 FAIL
(was 131/6).

**No code change needed for read-back**: `char_to_datum`→`numeric_in` is driven by
the PG output tuple descriptor, so a numeric output column always parses CH's
text (Decimal or Float64) correctly; finite values never fail.

## D0004 — 2026-06-24 — Phase 1 independent adversarial review: PASS

A fresh reviewer (separate context, did not write the code) attacked Phase 1 on
correctness, fidelity, performance, and test integrity, running its own queries.
Verdict: **no blocking findings.**
- Correctness: Q1/Q6 fully offload, oracle fires, no residual PG aggregate; empty
  result, empty-group, 2526-group, and HAVING edge cases all match native.
- Fidelity: SUM columns (incl. scale-growing `sum_charge`) bit-exact (maxrel 0);
  avg independently re-derived at max rel **1.48e-16** (≤ machine epsilon). Noted
  CH Decimal `sum` wraps silently on overflow — ~20 orders of magnitude of
  headroom at SF10, irrelevant here, recorded as a latent caveat.
- Performance: native baseline confirmed parallel (16 workers) + JIT + no spill;
  offload not cached (reads 60M fresh each run); cgroup cap applies to both trees;
  cores=CPU-s/wall correct. The 3.2–4.3× win is real.
- Test integrity (non-blocking, **addressed**): the `rows_equiv` relative
  tolerance was 1e-9, which could mask a sub-cent error in a billion-scale SF10
  sum. Tightened to **1e-14** (still ≥45× above Float64 epsilon; catches a
  one-cent-at-billion error rel ~1.8e-14). sum/min/max compare Decimal-exact up
  front so the tolerance only ever guards the avg→Float64 round-off. Suite still
  137 PASS / 0 FAIL. Phase 1 marked GREEN.

## D0005 — 2026-06-25 — Phase 2: two join blockers root-caused & fixed

The Phase-0 "join offload broken at SF10" had TWO distinct causes, both now fixed:

**(a) ClickHouse READONLY crash on adopted columns** (CH-side; ClickHouse repo
commit 498959fa5ee). The join pipeline's `SimpleSquashingTransform` reused the
first input chunk's columns as a mutable accumulator via `IColumn::mutate()`, which
is a no-op at refcount 1, leaving a read-only adopted (zero-copy SHM) column in
place → `prepareForSquashing()->reserve()` threw `Code 164 READONLY`. Fixed by
adding `IColumn::convertToFullColumnIfAdopted()` (no-op default; overridden in
ColumnVector/Decimal/String to materialize an owned copy when adopted) and calling
it at the squash accumulator. +2 unit tests (refcount-1 materialization); 83/83
adoption tests pass. Verified: a no-filter 3-table join that threw Code 164 now
returns correct rows.

**(b) bpchar (CHAR(n)) equality semantics under pushdown** (producer-side;
`src/shm_offload.c`). TPC-H string columns are `CHAR(n)` (bpchar), stored
blank-padded ("BUILDING  "). PostgreSQL's `bpchareq` ignores trailing blanks; the
pushed-down ClickHouse `String =` is byte-exact, so `c_mktsegment = 'BUILDING'`
matched 0 rows (the dominant cause of the "empty join" symptom: Q3, Q5, Q7, Q8,
Q11, Q12, Q17, Q19, Q20). Diagnosed by an adversarial investigation that
overturned the initial "CH string-comparison bug" hypothesis (LIKE and GROUP BY
worked; only `=`/`IN`/range on padded values failed). Fixed by stripping trailing
blanks from bpchar values at columnization (both the per-row and vectorized
string-fill paths), in `pgch_bpchar_trim_len`. **No ClickHouse rebuild needed.**
Verified end-to-end: single-table `c_mktsegment='BUILDING'` → 300276 (was 0); Q3
join+filter → 10 rows matching native; Q12 (sum(case)→bigint over join) fully
offloads, 2 rows matching native. verify_offload.sh stays 137/0.

**Decision (alternatives).** For (b), chose producer-side bpchar trim over
(i) padding the comparison literal to the column width in deparse — rejected:
deparseConst lacks the column typmod, needs OpExpr+ScalarArrayOpExpr plumbing,
more surface; (ii) declining bpchar-comparison pushdown — rejected: would keep the
filter (and hence the aggregate) in PostgreSQL, losing full-pushdown coverage. The
producer trim is uniform across `=`/`<>`/`IN`/ordering/`length()`/grouping and
matches PG bpchar semantics exactly except for display padding (F5). **Limitation:**
a query literal carrying *significant* trailing blanks on a bpchar comparison
(`col = 'X  '`) is not yet trimmed on the literal side; TPC-H literals are clean,
so this is a logged robustness follow-up (also trim bpchar constants in
deparseConst), not a TPC-H correctness issue.

## Intentional fidelity deviations (bounded, quantified)

| # | query/col | engine diff | max abs err | max rel err | bound / cause |
|---|-----------|-------------|-------------|-------------|---------------|
| F1 | Q1 `sum_qty,sum_base_price,sum_disc_price,sum_charge` (SF10) | CH Decimal sum vs PG numeric sum | 0 | 0 | exact; only display-scale trailing zeros (`377518399` vs `377518399.00`), equal as numeric |
| F2 | Q1 `count_order` (SF10) | count → Int | 0 | 0 | exact |
| F3 | Q1 `avg_qty,avg_price,avg_disc` (SF10) | CH `avg(Decimal)`→Float64 vs PG exact numeric | avg_price 6e-12 | **1.57e-16** | Float64 round-off; ≤ machine epsilon (~2.2e-16). Per fidelity policy: bounded, intentional. Measured `dev/tpch/evidence/phase1/`. |
| F4 | Q6 `revenue` (SF10) | CH Decimal sum | 0 | 0 | exact (`1230113636.0101` both) |
| F5 | any projected/grouped `CHAR(n)` column (e.g. Q2/Q9/Q10 `n_name`) | offload strips bpchar trailing blanks ("GERMANY" vs "GERMANY⎵⎵…") | 0 (display only) | 0 | semantically identical — bpchar trailing blanks are insignificant in PG too (`length()`/comparison/grouping all ignore them). Display-padding only. Producer rtrim (D0005b). Detected in the eligibility scan as `exact(bpchar)`. |

**Root cause of F3.** ClickHouse `avg()` over a Decimal returns Float64 (it does
not keep Decimal accumulation), so the mean carries ~15–16 significant digits vs
PostgreSQL's exact numeric. This is the explicitly-allowed Decimal→Float64
deviation. It could be removed by deparsing `avg(x)` over a Decimal as
`sum(x)/count(x)` (both exact Decimal) — deferred unless a consumer needs exact
`avg`; recorded so the option is on the table.

---

## Queries concluded NOT offloadable (root-caused)

_(none yet — populated as blockers are proven irreducible)_
