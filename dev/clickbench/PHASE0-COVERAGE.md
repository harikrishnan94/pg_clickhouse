# Phase 0 — ClickBench SHM-offload coverage map & fidelity baseline

**Date:** 2026-06-25 · **Branch:** streamed-table-shm-offload
**DB:** clickbench (10M-row subset `hits_0..9`, schema `pg`, `pg.hits` = 10,000,000
rows) · **CH:** patched `streamed_table` build v26.6.1.1, RUN_ID=tpchcb, HTTP
127.0.0.1:21002 (live pid resolved from the port, not the stale manifest CH_PID).

Starting coverage map + fidelity baseline for the "all-43 fully offloaded" effort.
**Measured, not assumed:** every row is backed by the ClickHouse `system.query_log`
oracle (a `streamed_table()` `QueryFinish` — or `ExceptionWhileProcessing` for a
client-cancelled stream — with `ShmAdoptedBlocks >= 1`, correlated by a unique
`log_comment` tag, with the heavy fragment in the dispatched SQL) **and** the PG
`EXPLAIN` plan (residual ops above the topmost `Custom Scan (ClickHouseShmScan)`),
per the classification rule in `FULL-OFFLOAD-DECISIONS.md` (D0003).

Reproduce: `RUN_ID=tpchcb PGDB=clickbench dev/clickbench/eligibility-scan.sh`. Raw
artifacts per query (`q{N}.off.out`, `q{N}.on.out`, `q{N}.plan.txt`,
`q{N}.chsql.txt`, `q{N}.fid.txt`, `q{N}.{off,on}.err`) under
`dev/clickbench/evidence/phase0/`. Fidelity comparator: `cmp_results.py`
(numeric-aware: pairs rows canonically, quantifies avg→Float64 / count(DISTINCT)
integer error / structural diffs, tie-robust).

## Headline

| | count | queries |
|--|------:|---------|
| **fully offloaded** (heavy fragment in CH, oracle QueryFinish) | **36** | Q2–17, 20–23, 26, 28–42 |
| **fully(cxl)** (heavy GROUP BY ran in CH, stream client-cancelled by LIMIT) | **1** | Q18 |
| **none** (declined) | **6** | Q1 (expected, no column); Q19/24/25/27/43 (timestamp) |
| **TOTAL offloading the heavy fragment** | **37 / 43** | |

At baseline **37 of 43** already push the heavy analytic fragment to ClickHouse
over SHM — ClickBench's single-table scan+filter+aggregate+GROUP BY shape is the
offload's sweet spot. The 6 non-offloaded split into **1 expected** (Q1) and **5
blocked by one root cause** (timestamp wire-type gap, D0008) — the entire Phase-1
target. Contrast TPC-H Phase-0 (1/22 fully) — ClickBench has no joins, so the
join-squashing READONLY/deadlock bugs that dominated TPC-H do not apply here.

## Verdict legend

- **fully** — heavy fragment (aggregate / GROUP BY / HAVING, or filtered scan for
  non-agg projections) runs in CH; PG plan has no residual `Aggregate` above the
  topmost `ClickHouseShmScan`; result correct (modulo documented deviations).
- **fully(cxl)** — heavy fragment ran in CH (`ShmAdoptedBlocks >= 1`) but PG
  cancelled the stream early (`LIMIT` w/o `ORDER BY` → Code 210 broken pipe).
- **none** — no `ClickHouseShmScan` / no `streamed_table` query (declined).

## Coverage table (Phase-0 baseline, measured — canonical re-scan, fixed oracle)

Columns: `pg_agg`/`pg_sort`/`pg_lim` = residual PG nodes above the CustomScan;
`ch_grpby`/`ch_ord`/`ch_lim` = ops present in the dispatched CH SQL; `ch_distinct`
= how count(DISTINCT) deparsed. **`pg_sort=1, ch_ord=no, ch_lim=no`** on every
top-N query = top-N NOT pushed (D0007, the perf lever).

| Q | verdict | fidelity | ShmBlk | read_rows | pg_agg | pg_sort | ch_grpby | ch_distinct | shape |
|--:|---------|----------|-------:|----------:|:------:|:------:|:--------:|:-----------:|-------|
| 1 | none (expected) | exact | 0 | 0 | 2 | 0 | no | - | `COUNT(*)` no column |
| 2 | fully | exact | 163 | 10M | 0 | 0 | no | - | filtered count |
| 3 | fully | exact | 164 | 10M | 0 | 0 | no | - | sum+count+avg |
| 4 | **fully (WRONG)** | **float!** | 162 | 10M | 0 | 0 | no | - | `AVG(UserID)` Int64-overflow (D0005) |
| 5 | fully | exact | 162 | 10M | 0 | 0 | no | count(DISTINCT) | `COUNT(DISTINCT UserID)` exact (D0004) |
| 6 | fully | exact | 164 | 10M | 0 | 0 | no | count(DISTINCT) | `COUNT(DISTINCT SearchPhrase)` exact |
| 7 | fully | exact | 164 | 10M | 0 | 0 | no | - | min/max date |
| 8 | fully | exact | 164 | 10M | 0 | 1 | yes | - | GROUP BY + ORDER BY (no LIMIT) |
| 9 | fully | exact | 164 | 10M | 0 | 1 | yes | count(DISTINCT) | top-N + count(DISTINCT) |
| 10 | fully | exact | 163 | 10M | 0 | 1 | yes | count(DISTINCT) | top-N, sum+avg+count(DISTINCT) |
| 11 | fully | exact | 162 | 10M | 0 | 1 | yes | count(DISTINCT) | top-N + count(DISTINCT) |
| 12 | fully | exact | 163 | 10M | 0 | 1 | yes | count(DISTINCT) | top-N + count(DISTINCT) |
| 13 | fully | exact | 163 | 10M | 0 | 1 | yes | - | top-N GROUP BY str |
| 14 | fully | exact | 163 | 10M | 0 | 1 | yes | count(DISTINCT) | top-N + count(DISTINCT) |
| 15 | fully | exact | 163 | 10M | 0 | 1 | yes | - | top-N 2-key GROUP BY |
| 16 | fully | exact | 162 | 10M | 0 | 1 | yes | - | `GROUP BY UserID` (large result) |
| 17 | fully | exact | 163 | 10M | 0 | 1 | yes | - | `GROUP BY UserID,SearchPhrase` |
| 18 | fully(cxl) | DIFF→nondet | 163* | 10M* | 0 | 0 | yes* | - | `LIMIT 10` no ORDER BY (D0006) |
| 19 | **none** | exact | 0 | 0 | 1 | 1 | no | - | `extract(minute FROM EventTime)` (D0008) |
| 20 | fully | exact | 163 | 10M | 0 | 0 | no | - | point filter (empty in subset) |
| 21 | fully | exact | 162 | 10M | 0 | 0 | no | - | `URL LIKE '%google%'` count |
| 22 | fully | DIFF→tie | 161 | 10M | 0 | 1 | yes | - | top-N + LIKE (tie, D0006) |
| 23 | fully | exact | 162 | 10M | 0 | 1 | yes | count(DISTINCT) | top-N + LIKE/NOT LIKE + count(DISTINCT) |
| 24 | **none** | exact | 0 | 0 | 0 | 1 | no | - | `SELECT *` (3 timestamp cols) (D0008) |
| 25 | **none** | exact | 0 | 0 | 0 | 1 | no | - | `ORDER BY EventTime` (D0008) |
| 26 | fully | exact | 162 | 10M | 0 | 1 | no | - | proj + `ORDER BY SearchPhrase` |
| 27 | **none** | exact | 0 | 0 | 0 | 1 | no | - | `ORDER BY EventTime,SearchPhrase` (D0008) |
| 28 | fully | exact | 162 | 10M | 0 | 1 | yes | - | `AVG(length(URL))` HAVING top-N |
| 29 | **fully** | **approx** | 166 | 10M | 0 | 1 | yes | - | `REGEXP_REPLACE` group; ~1e-4 count dev (D0009) |
| 30 | fully | exact | 163 | 10M | 0 | 0 | no | - | 90× `SUM(ResolutionWidth+n)` |
| 31 | fully | exact | 162 | 10M | 0 | 1 | yes | - | top-N sum+avg |
| 32 | fully | approx→tie | 162 | 10M | 0 | 1 | yes | - | top-N (tie, D0006) |
| 33 | fully | approx→tie | 163 | 10M | 0 | 1 | yes | - | top-N (tie, D0006) |
| 34 | fully | exact | 163 | 10M | 0 | 1 | yes | - | `GROUP BY URL` (large result) |
| 35 | fully | exact | 163 | 10M | 0 | 1 | yes | - | `GROUP BY 1, URL` |
| 36 | fully | exact | 164 | 10M | 0 | 1 | yes | - | `GROUP BY ClientIP, ClientIP-1..3` |
| 37 | fully | exact | 162 | 10M | 0 | 1 | yes | - | top-N PageViews (date filter) |
| 38 | fully | exact | 162 | 10M | 0 | 1 | yes | - | top-N Title |
| 39 | fully | DIFF→tie | 163 | 10M | 0 | 1 | yes | - | top-N `OFFSET 1000` (tie, D0006) |
| 40 | fully | DIFF→tie | 163 | 10M | 0 | 1 | yes | - | `CASE WHEN` group + `OFFSET 1000` (tie) |
| 41 | fully | approx→tie | 163 | 10M | 0 | 1 | yes | - | top-N `OFFSET 100` (tie, D0006) |
| 42 | fully | exact | 163 | 10M | 0 | 1 | yes | - | top-N `OFFSET 10000` (empty in subset) |
| 43 | **none** | exact | 0 | 0 | 1 | 1 | no | - | `DATE_TRUNC('minute', EventTime)` (D0008) |

\* Q18: from the `ExceptionWhileProcessing` log row (Code 210 broken pipe) — the
GROUP BY read all 10M rows and adopted 163 blocks before PG's `LIMIT 10` closed
the socket.

## Fidelity ledger (Phase-0 baseline)

Comparison is offload-ON vs native (offload-OFF), numeric-aware and tie-robust.

| Q | column | class | max abs err | max rel err | root cause | disposition |
|---|--------|-------|------------:|------------:|------------|-------------|
| 5,6,9,10,11,12,14,23 | `COUNT(DISTINCT)` | **exact** | 0 | 0 | CH `count(DISTINCT)`→`uniqExact` (exact), not HLL | **accept** (D0004) — no deviation |
| 3,10,28,29,31,32,33 | `AVG(small int / length)` | **exact** | 0 | 0 | sum stays in int64 range; read-back numeric matches | accept |
| **4** | `AVG(UserID)` | **WRONG** | 3.2e18 | **~1.0** | CH `avg(Int64)` numerator overflow on full-range UserID | **BUG → fix P2** (Float64 cast, D0005) |
| **29** | per-group `COUNT(*)` | **approx** | 22 | **9.9e-5** | PG vs CH regexp_replace extract differ on edge Referers | **root-cause P4** (D0009); not a tie |
| 22,32,33,39,40,41 | top-k membership | tie | 0† | 0† | top-N tie reshuffle (PG sort over streamed result) | **accept** (D0006); †exact w/ deterministic tiebreak |
| 18 | top-k membership | nondet | — | — | `LIMIT 10` no `ORDER BY` — nondeterministic by design | accept (D0006) |
| all others | — | exact | 0 | 0 | integer aggregates / pushed exactly | accept |

**Independent re-derivations** (≥2 sources, per the evidence standard):
- **count(DISTINCT) exact:** (1) `cmp_results.py` → `exact|0|0` for all 8; (2)
  manual `count(DISTINCT UserID)` native `1530334` == offload `1530334` == CH
  `uniqExact`/`count(DISTINCT)` `1530334`; `count(DISTINCT SearchPhrase)` all
  `835093` (independently confirmed by adversarial review).
- **Q4 avg overflow:** (1) native `2513100748938099884` vs offload
  `-653315757734.87`; (2) CH-direct `avg(UserID)` = `-653315757734.87` (same wrong
  value — a CH `avg(Int64)` property, reproduced off the offload path), while
  `avg(toFloat64(UserID))` = `2513100748938100700` ≈ native.
- **Tie reshuffle benign:** (1) baseline `DIFF`/`approx`; (2) re-run with
  deterministic tiebreak → `exact|k|k|0|0` for Q22/32/33/39/40/41.

## Root-caused blocking / opportunity mechanisms

1. **Timestamp wire-type gap (D0008)** — the only true coverage blocker (Q19, 24,
   25, 27, 43). `src/shm_offload.c:112-121` maps no `TIMESTAMP`; the consumer
   already adopts `DateTime`/`DateTime64`. → **Phase 1**.
2. **avg(wide Int64) overflow in CH (D0005)** — Q4 wrong answer. → **Phase 2**
   (deparse `avg(int)`→`avg(toFloat64(..))`, the spec's avg→Float64 policy).
3. **Top-N / ORDER BY / LIMIT / OFFSET not pushed (D0007)** — every top-N query
   streams the full grouped result to PG for the sort+limit. Coverage-neutral
   (still `fully`), but the dominant **perf** lever. → **Phase 3**.
4. **REGEXP_REPLACE semantics (D0009)** — Q29 ~1e-4 count deviation. → **Phase 4**.
5. **Q1 no-column decline (D0010)** — expected, logged, not a failure.
