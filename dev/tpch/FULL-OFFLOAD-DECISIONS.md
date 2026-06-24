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

## Intentional fidelity deviations (bounded, quantified)

_(none yet — populated as coverage is unlocked; each entry: query, column, max abs
error, max rel error, root cause, why bounded)_

---

## Queries concluded NOT offloadable (root-caused)

_(none yet — populated as blockers are proven irreducible)_
