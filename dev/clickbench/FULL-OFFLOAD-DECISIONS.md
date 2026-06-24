# Full SHM-offload of ClickBench — Decision log

Running log of: (a) decisions taken without a human where the spec left an
ambiguity (with alternatives + rationale), (b) every **intentional fidelity
deviation** accepted to unlock coverage (bounded + quantified), and (c) every
query concluded **cannot** be offloaded, with the root-caused blocking mechanism.

This is the artifact a human reviews later. It is append-only in spirit; entries
are dated. "Offloaded" always means proven from ClickHouse `system.query_log`
(a `streamed_table()` `QueryFinish` — or `ExceptionWhileProcessing` for a
client-cancelled stream — with `ShmAdoptedBlocks >= 1`, the heavy fragment in the
dispatched SQL), never from the PostgreSQL plan alone.

---

## Environment of record

- DB `clickbench`, schema `pg`, table `hits` (10M-row subset `hits_0..9`). PG 18.4
  @ 127.0.0.1:5432. `pg_clickhouse` 0.3.
- ClickHouse patched `streamed_table` build (v26.6.1.1), RUN_ID=tpchcb, HTTP
  127.0.0.1:21002 / TCP 21003. The live pid (`2394956` at scan time) is resolved
  from the listening port, **not** the manifest `CH_PID` (`274738`, stale).
- `ubuntu` OS login has no PG role; all psql runs as the `postgres` OS user.
- Offload engaged via the `shm_set_block` SET block in `dev/bench/bench-common.sh`
  (`enable_shm_offload=on`, `shm_min_rows=0`, `session_settings` incl.
  `allow_experimental_streamed_table_function 1`, `final 1`, `group_by_use_nulls 1`,
  and a unique `log_comment` tag).
- Host: 32 cores, 61 GiB. Idle/dedicated.

---

## D0001 — 2026-06-25 — Environment: stale FDW server port

**Finding.** At session start the FDW server `ch_bench` (which `local_ch_server`
points at for the offload) had `port '21000'`, but the live ClickHouse HTTP port
is `21002` (matches the manifest). Offload failed with `communication error:
Failed to connect to 127.0.0.1 port 21000`.

**Decision.** Fix in place: `ALTER SERVER ch_bench OPTIONS (SET port '21002')`
(minimal; the 10M `pg.hits` heap + `ch` FDW import are intact, only the server
port option was stale). Verified: offload now fires (`Q2 → 202134`, matches
native; `last_query_used_clickhouse = on`). Did **not** re-run `make pg` (would
re-COPY 10M rows for no benefit).

---

## D0002 — 2026-06-25 — Oracle: poll-retry the query_log (async-flush race)

**Finding.** ClickHouse enqueues the `QueryFinish` row into the *asynchronous*
`query_log` buffer slightly **after** the query pipeline completes and the PG
backend returns. A single `SYSTEM FLUSH LOGS` + read immediately after the PG
query can therefore **miss** the just-finished `streamed_table` entry. This
produced **false negatives** (`strmd=0`) on the large-grouped-result queries
Q18/Q37/Q40 in the first scan, even though re-reading seconds later found the
entry (`read_rows=10000000, ShmAdoptedBlocks=164`, tag attached).

**Decision.** The oracle must **poll-retry**: `FLUSH LOGS` + read by tag, retry up
to 12×0.5 s until the entry appears (else conclude genuine non-offload). Applied
to `dev/clickbench/eligibility-scan.sh` and `dev/clickbench/wsweep.sh`
(`check_eligible`). After the fix Q37/Q40 correctly classify **fully**. A single
necessary-not-sufficient `SHOW last_query_used_clickhouse` is explicitly **not**
trusted alone — it reports `on` even when the tag/recency query_log read races.

---

## D0003 — 2026-06-25 — "Fully offloaded" classification rule (ClickBench shape)

**Decision.** ClickBench is single-table (no joins); the **heavy fragment** is the
aggregate / `GROUP BY` / `HAVING` (+ top-N) for aggregate queries, or the filtered
scan (+ `ORDER BY`/`LIMIT`) for the non-aggregate projections (Q20, Q24–27). A
query is **fully offloaded** iff BOTH independent sources agree:
1. CH `system.query_log` (by unique `log_comment` tag) shows a `streamed_table()`
   `QueryFinish` with `ShmAdoptedBlocks >= 1`, and the dispatched CH SQL contains
   the query's heavy operator (the `GROUP BY`/aggregate, or the filtered scan), AND
2. the PG `EXPLAIN` plan has **no residual `Aggregate`/`HashAggregate`/`GroupAggregate`**
   doing the query's reduction above the topmost `Custom Scan (ClickHouseShmScan)`.

`fully(cxl)` = the heavy fragment ran in CH (`ShmAdoptedBlocks >= 1`) but the
stream was **client-cancelled** (logged `ExceptionWhileProcessing`, Code 210
broken pipe) because PG's `LIMIT`-without-`ORDER BY` closed the socket after k
rows (Q18). The heavy op still executed in CH; counted as offloaded.

`scan_only` = a `ClickHouseShmScan` fires but PG still does the aggregate.
`none` = no `ClickHouseShmScan` / no `streamed_table` query in the log.

A residual PG `Sort`/`Limit` above the CustomScan does **not** demote `fully` (the
GROUP BY+aggregate is the heavy fragment); it is tracked separately as the
**top-N pushdown** perf lever (D0007).

---

## D0004 — 2026-06-25 — count(DISTINCT) deparses to ClickHouse uniqExact (EXACT)

**Finding.** `count(DISTINCT col)` deparses verbatim to ClickHouse `count(DISTINCT
col)`. ClickHouse's `count_distinct_implementation` setting **defaults to
`uniqExact`** (exact), so the offloaded result is **bit-exact** to PostgreSQL — NOT
approximate HyperLogLog. Measured exact for **every** ClickBench count(DISTINCT)
query: Q5, Q6, Q9, Q10, Q11, Q12, Q14, Q23 all `fidelity=exact` vs native (incl.
the tiebroken top-N variants). Independent re-derivation (confirmed by the
adversarial review): Q5 `count(DISTINCT UserID)` native == offload == CH
`uniqExact` == CH `count(DISTINCT)` all `1530334` (0 abs/rel error); Q6
`count(DISTINCT SearchPhrase)` all `835093`.

**Decision.** **Accept as-is.** No deparse change, no fidelity deviation. This is
the single biggest fidelity trap called out in the spec, and it is a non-issue
here. Should a future CH config set `count_distinct_implementation=uniq`
(approximate), this would regress — noted as a risk; not changing the default.

---

## D0005 — 2026-06-25 — avg() over a full-range Int64 column overflows in ClickHouse (Q4)

**Finding.** Q4 `SELECT AVG(UserID) FROM hits`. `UserID` is BIGINT spanning the
full signed-int64 range (`min=-9.22e18, max=9.22e18`). PostgreSQL `avg(bigint)`
accumulates the sum in arbitrary-precision `numeric` → exact `2513100748938099884`.
ClickHouse `avg(Int64)` accumulates the numerator in a fixed-width integer that
**overflows** over 10M large values → `-653315757734.87` (wrong sign + magnitude;
**rel err ≈ 1.0** — a wrong answer, not bounded round-off). Independent check:
`avg(toFloat64(UserID))` on CH = `2513100748938100700` ≈ native (rel ≈ 3e-16). So
the fix is to accumulate in Float64.

**Decision.** This is a **correctness bug**, not an acceptable deviation (the spec
forbids undocumented wrong answers). Fix in Phase 2 by deparsing `avg(<wide int>)`
as `avg(toFloat64(<col>))` — which both eliminates the overflow AND aligns with
the spec's accepted **avg()→Float64** policy (bounded ≤ ~1e-15 rel). Scope: only
Q4 hits this at ClickBench scale (every other `avg()` is over `ResolutionWidth`
SMALLINT or `length(...)`, whose sums stay well within range). Until fixed, Q4 is
logged as **offloaded-but-WRONG**, not accepted.

---

## D0006 — 2026-06-25 — Top-N tie-boundary reshuffle is a benign, documented deviation

**Finding.** Q22, Q32, Q33, Q39, Q40, Q41 showed `DIFF`/`approx` at baseline. Root
cause: these are `... ORDER BY <agg> DESC LIMIT k [OFFSET m]` where PG performs the
`Sort`+`Limit` over the streamed grouped result (top-N not pushed, D0007). The
`GROUP BY` counts/aggregates are computed **exactly** in CH (identical to native);
the only difference is **which equal-ranked tied rows** land in the top-k window —
non-deterministic in *both* engines (PG's native plan reshuffles ties run-to-run
too). Re-running each with a **deterministic tiebreak** appended (`ORDER BY <agg>
DESC, <group keys>`) yields **`exact|k|k|0|0`** for all six.

**Decision.** **Accept** as a benign tie-order deviation (explicitly allowed by the
spec). The fidelity oracle compares top-N results **tie-robustly** (deterministic
tiebreak or set-equivalence), never a brittle byte diff. Recorded per-query in the
fidelity ledger (`PHASE0-COVERAGE.md`). Q18 (`LIMIT 10` with **no** `ORDER BY`) is
the degenerate case — nondeterministic *by design*; comparing exact rows is
meaningless, so it is excluded from the byte oracle and judged on offload mechanism
alone (its GROUP BY offloads, D0003 `fully(cxl)`).

---

## D0007 — 2026-06-25 — Top-N / ORDER BY / LIMIT / OFFSET is NOT pushed (perf lever)

**Finding.** For **every** aggregate top-N query the GROUP BY+aggregate offloads
(`pg_agg=0`) but the `ORDER BY`+`LIMIT`(+`OFFSET`) runs in **PostgreSQL**: the plan
is `Limit → Sort → Custom Scan (ClickHouseShmScan)`, the dispatched CH SQL has
**no** `ORDER BY`/`LIMIT` (`ch_ord=no, ch_lim=no` for all), and CH streams the
**entire** grouped result back (`read_rows=10000000` feeding a grouped relation PG
then sorts). The deparser *has* `appendOrderByClause`/`appendLimitClause`
(`src/deparse.c:4857-4936`) but they do not engage for the streamed_table upper
path.

**Decision.** Pursue top-N pushdown as a **performance** phase (P3), not a coverage
blocker (these are already `fully` per D0003). Highest value where the grouped
result is large: Q16 (`GROUP BY UserID`), Q34/Q35 (`GROUP BY URL`), Q31–33
(`GROUP BY ClientIP`/`WatchID`) — streaming millions of grouped rows to take a
top-10 is wasteful. Quantify the win in `wsweep.sh` before/after.

---

## D0008 — 2026-06-25 — Timestamp columns cannot stream (coverage blocker, Q19/24/25/27/43)

**Finding.** The SHM wire type map (`src/shm_offload.c:112-121`,
`pgch_pg_type_to_ch_wire`) has **no `TIMESTAMP`/`TIMESTAMPTZ`** case — only
bool/int{2,4,8}/float{4,8}/date/text/varchar/bpchar/numeric. Any query that must
**stream** a `timestamp` column (project it, filter/sort on it, or feed it to a
CH-side function) is declined entirely. Confirmed: Q25 `... ORDER BY EventTime
LIMIT 10` plans `Limit → Sort → Parallel Seq Scan` (no CustomScan). Affects Q19
(`extract(minute FROM EventTime)`), Q24 (`SELECT *`, 3 timestamp cols), Q25/Q27
(`ORDER BY EventTime`), Q43 (`DATE_TRUNC('minute', EventTime)`).

**Decision.** Add `timestamp` wire support in Phase 1 (highest coverage: +5
queries). The patched ClickHouse consumer **already** adopts `DateTime` (UInt32)
and `DateTime64` (ColumnDecimal) — `AdoptionLayer.cpp:338,345` — so this is a
**PG-producer-side-only** change (map `TIMESTAMPOID` → a DateTime/DateTime64 wire
tag + converter), no ABI/consumer change. Fidelity to verify: PG `timestamp`
(µs since 2000-01-01) vs CH `DateTime` (s since 1970) — ClickBench `EventTime`
values are whole seconds, so second-resolution should be exact; will measure and
pick DateTime vs DateTime64(6) accordingly.

---

## D0009 — 2026-06-25 — REGEXP_REPLACE semantics differ slightly PG vs CH (Q29)

**Finding.** Q29 `GROUP BY REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$',
'\1')` offloads fully, but the per-group `COUNT(*)` deviates by **rel ≈ 9.9e-5**
(`off=223329 on=223307` on the largest group) even with a deterministic tiebreak —
so it is **not** a tie reshuffle. Root cause (hypothesis): PostgreSQL
`regexp_replace` (POSIX/PCRE) and ClickHouse `replaceRegexpOne` (RE2) extract a
slightly different host substring for a small fraction of `Referer` values (edge
cases in the optional `(?:www\.)?` group / anchoring / non-matching rows that
`\1`-collapse differently), shifting a few rows between groups.

**Decision.** Root-cause + quantify exactly in the regex phase (P4): enumerate the
divergent `Referer` rows, bound the max per-group rel error, and decide **accept
(bounded, documented)** vs **deparse to an RE2-equivalent that matches PG**. Until
then Q29 is logged as offloaded with a measured ~1e-4 count deviation (group-set
identical; counts shift). Not hidden.

---

## D0010 — 2026-06-25 — Q1 (`SELECT COUNT(*) FROM hits`) declines — expected

**Finding/Decision.** Q1 references **no column**, so there is nothing to stream;
the planner adds no `ClickHouseShmScan` and a parallel seq-scan `count(*)` wins.
Per the spec this is **expected, not a failure**. Logged as `none (expected)`.
Optional later experiment (P5): force a 1-column stream to offload the count;
low value, deferred.
