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

## Queries provisionally NOT-YET-offloadable (root-caused, pending phase work)

- **CH join READONLY/empty (Phase 2):** Q2, Q3, Q5, Q7, Q8, Q11, Q12, Q17, Q19, Q20.
- **CH join deadlock (Phase 2):** Q9, Q14, Q21.
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

## Intentional fidelity deviations (bounded, quantified)

| # | query/col | engine diff | max abs err | max rel err | bound / cause |
|---|-----------|-------------|-------------|-------------|---------------|
| F1 | Q1 `sum_qty,sum_base_price,sum_disc_price,sum_charge` (SF10) | CH Decimal sum vs PG numeric sum | 0 | 0 | exact; only display-scale trailing zeros (`377518399` vs `377518399.00`), equal as numeric |
| F2 | Q1 `count_order` (SF10) | count → Int | 0 | 0 | exact |
| F3 | Q1 `avg_qty,avg_price,avg_disc` (SF10) | CH `avg(Decimal)`→Float64 vs PG exact numeric | avg_price 6e-12 | **1.57e-16** | Float64 round-off; ≤ machine epsilon (~2.2e-16). Per fidelity policy: bounded, intentional. Measured `dev/tpch/evidence/phase1/`. |
| F4 | Q6 `revenue` (SF10) | CH Decimal sum | 0 | 0 | exact (`1230113636.0101` both) |

**Root cause of F3.** ClickHouse `avg()` over a Decimal returns Float64 (it does
not keep Decimal accumulation), so the mean carries ~15–16 significant digits vs
PostgreSQL's exact numeric. This is the explicitly-allowed Decimal→Float64
deviation. It could be removed by deparsing `avg(x)` over a Decimal as
`sum(x)/count(x)` (both exact Decimal) — deferred unless a consumer needs exact
`avg`; recorded so the option is on the table.

---

## Queries concluded NOT offloadable (root-caused)

_(none yet — populated as blockers are proven irreducible)_
