# Phase 0 — TPC-H SHM-offload coverage map & fidelity baseline

**Date:** 2026-06-24 · **Branch:** streamed-table-shm-offload · **HEAD:** bbd2d00
**DB:** tpch_sf10 (SF10, schema `pg`, lineitem ~60M) · **CH:** patched
`streamed_table` build, RUN_ID=tpchcb, HTTP 127.0.0.1:21002.

This is the starting coverage map and fidelity baseline for the "all-22 fully
offloaded" effort. It is **measured**, not assumed: every row is backed by the
ClickHouse `system.query_log` oracle (a `streamed_table()` `QueryFinish` with
`ShmAdoptedBlocks >= 1` correlated by a unique `log_comment` tag) **and** the
PostgreSQL `EXPLAIN` plan, per the classification rule in
`FULL-OFFLOAD-DECISIONS.md` (D0001).

Reproduce: `PGDB=tpch_sf10 dev/tpch/eligibility-scan.sh`. Raw artifacts (one set
per query: `*.off.out`, `*.on.out`, `*.plan.txt`, `*.chsql.txt`, `*.on.err`) are
under `dev/tpch/evidence/phase0/`.

## Verdict legend

- **fully** — heavy fragment (join/aggregate) runs in ClickHouse; PG plan has no
  residual heavy op above the topmost `Custom Scan (ClickHouseShmScan)`; result
  correct.
- **scan_only** — a `ClickHouseShmScan` fires (base scan or a sub-fragment), but
  PG still performs the query's heavy join/aggregate. Result correct (the heavy
  math stays in PG, exact).
- **none** — no `ClickHouseShmScan` in the chosen plan (offload declined / lost
  on cost).
- **BUG** — offload engaged but produced a wrong answer or hung/errored
  (root-caused below).

## Coverage table (Phase-0 baseline, measured — clean re-scan)

Result column = offload-ON output vs native (offload-OFF). **CORRECT** answers
today: only **7 of 22** (Q1 stays native; Q4 fully offloads; Q6/Q10/Q13/Q15/Q18
are scan_only-exact). The other 15 produce **wrong/empty/error** answers under
offload — dominated by the ClickHouse join-squashing READONLY bug (see below).

| Q | result | offload behaviour today | blocker to correct "fully" |
|---|--------|-------------------------|----------------------------|
| 1 | correct (native) | nothing offloads (parallel seq scan wins) | numeric agg declined → only a non-parallel base CustomScan, loses on cost |
| 2 | **WRONG (empty, want 100)** | join offload → 0 rows | CH join READONLY/empty; correlated `min` subquery; numeric |
| 3 | **WRONG (empty, want 10)** | 3-way join reads 76M but join→0 rows | CH join READONLY bug; numeric agg |
| 4 | **fully ✓** | orders SEMI lineitem + count(*) GROUP BY pushed whole (75M, exact 5) | — already fully offloaded |
| 5 | **WRONG (empty, want 5)** | join reads only 5 rows (big tables never streamed) | CH join bug (one-side-empty); numeric agg |
| 6 | scan_only ✓ exact | filtered lineitem scan offloads; sum in PG | numeric agg declined |
| 7 | **WRONG (empty, want 4)** | derived-table join → 0 rows | CH join bug; derived table; numeric agg |
| 8 | **WRONG (empty, want 2)** | derived-table join → 0 rows | CH join bug; numeric agg; division |
| 9 | **ERROR (producer stall)** | 6-way join, agg-not-pushed → deadlock | CH join deadlock; numeric agg |
| 10 | scan_only ✓ exact (381105) | 4-way join offloads, agg in PG | numeric agg declined |
| 11 | **WRONG (empty, want 8685)** | join → 0 rows | CH join bug; scalar HAVING subquery; numeric |
| 12 | **WRONG (empty, want 2)** | orders⋈lineitem + sum(case)→bigint pushed but 0 rows (orders side not streamed) | CH join bug (one-side-empty) |
| 13 | scan_only ✓ exact (46) | inner count(→bigint) pushed, outer GROUP BY c_count in PG | two-level aggregation; outer agg in PG |
| 14 | **ERROR (producer stall)** | lineitem⋈part, agg-not-pushed → deadlock (CH froze at 1 block 200s) | CH join deadlock; numeric agg |
| 15 | scan_only ✓ exact | view: 3 base scans offload, agg+max in PG | view + scalar subquery; numeric |
| 16 | **ERROR (rescan)** | "rescan of a SHM-offload scan not supported" | rescan; NOT IN anti-join; count(distinct) |
| 17 | **WRONG (NULL, want 3295493.5)** | lineitem⋈part join → 0 rows → sum=NULL | CH join bug; correlated avg subquery; numeric |
| 18 | scan_only ✓ exact (624) | IN-subquery lineitem scan offloads; joins+agg in PG | IN-subquery-with-agg; joins in PG; numeric |
| 19 | **WRONG (NULL, want 30104438)** | lineitem⋈part join → 0 rows → sum=NULL | CH join bug; numeric agg; OR predicate |
| 20 | **WRONG (empty, want 1804)** | join → 0 rows | CH join bug; nested correlated subqueries |
| 21 | **ERROR (producer stall)** | 4-way join+EXISTS+NOT EXISTS → deadlock | CH join deadlock; NOT EXISTS anti-join |
| 22 | **ERROR (`{p1}` unbound)** | correlated subquery param not bound | correlated subquery param; NOT EXISTS anti-join |

Per-query raw signals in `evidence/phase0/SUMMARY.md`.

### Verdict tally (baseline)
- **fully offloaded & correct: 1** (Q4)
- **scan_only & correct (heavy op in PG): 5** (Q6, Q10, Q13, Q15, Q18)
- **not offloaded (correct, native): 1** (Q1)
- **WRONG answer under offload: 9** (Q2, Q3, Q5, Q7, Q8, Q11, Q12, Q17, Q19, Q20 → empty/NULL)
- **ERROR under offload: 5** (Q9, Q14, Q16, Q21, Q22)

## Root-caused blocking mechanisms

**Decimal/numeric aggregate output decline** (`src/shm_customscan.c:537`). The
grouped-aggregate CustomScan path bails whenever any output column is
`NUMERICOID`. This is the single biggest coverage gate — it keeps the
`sum`/`avg` revenue aggregate in PostgreSQL for Q1,Q3,Q5,Q6,Q9,Q10,Q14,Q15,
Q17,Q19 (and the agg over derived-table joins Q7,Q8). For single-table
aggregates (Q1) it has a second-order effect: with only a *non-parallel* base
CustomScan available, PG's parallel-aggregate plan wins on cost and the query
offloads **nothing**.

**Bug #1 — ClickHouse join-squashing READONLY on adopted columns (CORRECTNESS,
the dominant join blocker).** Bisecting Q3 to a 3-table join with no filters
surfaces the underlying ClickHouse exception:

```
Code: 164. Non-const accessor ColumnVector::reserve called on an adopted
(zero-copy SHM) column; such columns are read-only. Call IColumn::mutate() first
to COW-materialize. See adoption-layer spec I3: While executing
SimpleSquashingTransform. (READONLY)
```

The join pipeline coalesces input blocks with `SimpleSquashingTransform`, which
calls a non-const accessor (`reserve()`/`insertRangeFrom`) on a zero-copy
*adopted* SHM column without first COW-materialising it (`IColumn::mutate()`).
The guard is in `/home/ubuntu/ClickHouse/src/Columns/IColumn.cpp:64` and
`ColumnString.cpp:42`. With filters present the exception is swallowed inside the
join lane and the side silently yields **0 rows** (so the INNER join is empty →
Q2,Q3,Q5,Q7,Q8,Q11,Q12,Q17,Q19,Q20 return empty/NULL); without filters it
surfaces as the Code-164 error. This is a **ClickHouse consumer-side** bug, not a
deparse bug — the dispatched join SQL is verified correct (join keys, columns,
filters all right; e.g. `evidence/phase0/q3.chsql.txt`). It does **not** affect
single-table aggregate pushdown: Q4 (count(*) GROUP BY over a SEMI join) and a
single-table `GROUP BY count(*)` over 60M rows both produce correct results, so
CH aggregation over adopted columns is fine — the bug is specific to the join
squashing path. Fix lives in `/home/ubuntu/ClickHouse` (Phase 2).

**Bug #2 — join deadlock when the large join result is streamed back
(LIVENESS).** When the aggregate is *not* pushed (numeric decline) the join
result is materialised and streamed back to PG. Q14 (lineitem INNER part) freezes
with CH `read_rows` stuck at exactly one ring block (1048576 rows) for >200s — a
true deadlock, not slowness (a 300s stall budget does not help; reproduces with
one producer per table; independent of `final`/`group_by_use_nulls`). Affects
Q9, Q14, Q21. Likely the same adoption-layer interaction under output
backpressure; revisit with Bug #1.

**Bug #3 — SIGTERM leak (ROBUSTNESS, harness-relevant).** Killing the psql
client mid-offload (SIGTERM, e.g. a shell timeout) leaks the in-flight CH query
and its producer workers (observed hung 200s). PostgreSQL `statement_timeout`
cancellation, by contrast, reaps workers and rings cleanly. **All harnesses must
bound runtime with `statement_timeout`, never by killing psql.**

**rescan unsupported** (Q16): a `NOT IN (subquery)` plans the CustomScan inside a
rescanned nested loop → "rescan of a SHM-offload scan is not supported in phase
1".

**Correlated subquery parameter** (Q22): the offloaded fragment references an
outer parameter `{p1:Decimal}` that is never bound → `UNKNOWN_QUERY_PARAMETER`.

**Anti-join rejected** (`src/fdw.c:1865`): `JOIN_ANTI` (from `NOT EXISTS`/`NOT
IN`) is declined by `foreign_join_ok`. Blocks Q16, Q21, Q22.

## Fidelity ledger (Phase-0 baseline)

Wherever the offload **produces an answer** at baseline it is **bit-exact** to
native (the heavy numeric math is either kept in PG — scan_only — or integer-exact
— Q4 count). So there are **zero fidelity deviations at baseline**; the failures
are correctness BUGS (empty/NULL/error), not bounded deviations. Intentional
Decimal→Float64 deviations begin in Phase 1+ and are recorded here and in
`FULL-OFFLOAD-DECISIONS.md`.

| Q | offload result vs native | max abs err | max rel err | class |
|---|--------------------------|-------------|-------------|-------|
| 4 | identical (5 rows, count) | 0 | 0 | exact |
| 6,10,13,15,18 | identical (scan_only, agg in PG) | 0 | 0 | exact |
| 2,3,5,7,8,11,12,17,19,20 | offload returns empty/NULL | — | — | **BUG (not a deviation)** |
| 9,14,16,21,22 | offload errors | — | — | **BUG (error)** |
| 1 | native (no offload) | 0 | 0 | exact |
