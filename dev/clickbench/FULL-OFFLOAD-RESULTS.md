# ClickBench full SHM-offload — results

**Date:** 2026-06-25 · **Branch:** streamed-table-shm-offload · **Scale:** 10M-row
subset (`hits_0..9`, `pg.hits` = 10,000,000 rows) · **PG** 18.4 · **ClickHouse**
patched `streamed_table` build v26.6.1.1 (RUN_ID=tpchcb). Host: 32 cores, 61 GiB,
idle/dedicated. Live CH pid + ports resolved from the listening socket (the
manifest `CH_PID` is stale — D0013).

Goal: push each ClickBench query's heavy analytic fragment (scan+filter+aggregate+
GROUP BY[+top-N]) down to the co-located ClickHouse over the SHM offload
(`pg_clickhouse.enable_shm_offload`), and measure offload vs best-tuned native PG.
"Offloaded" is proven from ClickHouse `system.query_log` (a `streamed_table()`
`QueryFinish` — or `ExceptionWhileProcessing` for a client-cancelled stream — with
`ShmAdoptedBlocks >= 1`, the heavy fragment in the dispatched CH SQL), the PG
`EXPLAIN` plan (no residual aggregate above the topmost `ClickHouseShmScan`), and a
numeric-aware, tie-robust fidelity comparison vs native (D0003).

## Headline

| | count | queries |
|--|------:|---------|
| **fully offloaded** | **41** | Q2–17, 19–43 (all except Q18, Q1) |
| **fully(cxl)** (heavy GROUP BY ran in CH; stream client-cancelled by `LIMIT`-no-`ORDER BY`) | **1** | Q18 |
| **declined (intentional, no column referenced)** | **1** | Q1 |
| **TOTAL offloading the heavy fragment** | **42 / 43** | |

**42 of 43 ClickBench queries push their heavy fragment to ClickHouse over SHM.**
The single non-offload, Q1 (`SELECT COUNT(*) FROM hits`), references no column, so
there is nothing to stream and a parallel seq-scan `count(*)` wins — expected, not a
failure (D0010). Starting baseline was 37/43; this effort added **+5** (timestamp
support Q19/24/25/27/43, of which Q24 also needed the column-cap raise) and fixed
two correctness/fidelity bugs (Q4 avg overflow, Q29 regex).

## Coverage + fidelity table (final, measured)

`fragment`: heavy op pushed to CH. `pg_resid`: residual PG op above the CustomScan
(top-N is not pushed — D0007/D0015 — so a `Sort`/`Limit` remains; it does **not**
demote `fully`). `fidelity`: offload vs native (tie-robust).

| Q | offloaded | fragment in CH | pg_resid | fidelity | notes |
|--:|:---------:|----------------|----------|----------|-------|
| 1 | **no** | — | Aggregate (native) | exact | no column → declined (expected, D0010) |
| 2 | yes | filtered count | — | exact | |
| 3 | yes | sum+count+avg | — | exact | |
| 4 | yes | `AVG(UserID)` | — | avg→Float64 | rel ~6e-16 (D0005 fix; was wrong) |
| 5 | yes | `COUNT(DISTINCT UserID)` | — | **exact** | uniqExact (D0004) |
| 6 | yes | `COUNT(DISTINCT SearchPhrase)` | — | **exact** | uniqExact |
| 7 | yes | min/max date | — | exact | |
| 8 | yes | GROUP BY + agg | Sort | exact | |
| 9 | yes | top-N + count(DISTINCT) | Sort+Limit | exact | |
| 10 | yes | top-N sum+avg+count(DISTINCT) | Sort+Limit | exact | |
| 11 | yes | top-N + count(DISTINCT) | Sort+Limit | exact | |
| 12 | yes | top-N + count(DISTINCT) | Sort+Limit | exact | |
| 13 | yes | top-N GROUP BY str | Sort+Limit | exact | |
| 14 | yes | top-N + count(DISTINCT) | Sort+Limit | exact | |
| 15 | yes | top-N 2-key GROUP BY | Sort+Limit | exact | |
| 16 | yes | `GROUP BY UserID` | Sort+Limit | exact | large grouped result |
| 17 | yes | `GROUP BY UserID,SearchPhrase` | Sort+Limit | exact | |
| 18 | yes(cxl) | `GROUP BY UserID,SearchPhrase` | Limit | nondeterministic | `LIMIT 10` no `ORDER BY` (D0006) |
| 19 | yes | `extract(minute FROM EventTime)` group | Sort+Limit | exact | timestamp (D0011) |
| 20 | yes | point filter | — | exact | empty in subset |
| 21 | yes | `URL LIKE '%google%'` count | — | exact | |
| 22 | yes | top-N + LIKE | Sort+Limit | tie (exact tiebroken) | D0006 |
| 23 | yes | top-N + LIKE/NOT LIKE + count(DISTINCT) | Sort+Limit | exact | |
| 24 | yes | `SELECT *` (105 cols) filter | Sort+Limit | exact | col-cap raise (D0012) |
| 25 | yes | `ORDER BY EventTime` filtered proj | Sort+Limit | exact | timestamp |
| 26 | yes | `ORDER BY SearchPhrase` filtered proj | Sort+Limit | exact | |
| 27 | yes | `ORDER BY EventTime,SearchPhrase` proj | Sort+Limit | exact | timestamp |
| 28 | yes | `AVG(length(URL))` HAVING top-N | Sort+Limit | exact | |
| 29 | yes | `REGEXP_REPLACE` group HAVING top-N | Sort+Limit | **exact** | regex dotall (D0009 fix) |
| 30 | yes | 90× `SUM(ResolutionWidth+n)` | — | exact | |
| 31 | yes | top-N sum+avg | Sort+Limit | exact | |
| 32 | yes | top-N (WatchID,ClientIP) | Sort+Limit | tie (exact tiebroken) | D0006 |
| 33 | yes | top-N (WatchID,ClientIP) | Sort+Limit | tie (exact tiebroken) | D0006 |
| 34 | yes | `GROUP BY URL` | Sort+Limit | exact | large grouped result |
| 35 | yes | `GROUP BY 1, URL` | Sort+Limit | exact | grouped expr |
| 36 | yes | `GROUP BY ClientIP, ClientIP-1..3` | Sort+Limit | exact | grouped exprs |
| 37 | yes | top-N PageViews (date filter) | Sort+Limit | exact | |
| 38 | yes | top-N Title | Sort+Limit | exact | |
| 39 | yes | top-N `OFFSET 1000` | Sort+Limit | tie (exact tiebroken) | D0006 |
| 40 | yes | `CASE WHEN` group + `OFFSET 1000` | Sort+Limit | tie (exact tiebroken) | D0006 |
| 41 | yes | top-N `OFFSET 100` | Sort+Limit | tie (exact tiebroken) | D0006 |
| 42 | yes | top-N `OFFSET 10000` | Sort+Limit | exact | empty in subset |
| 43 | yes | `DATE_TRUNC('minute',EventTime)` group | Sort+Limit | exact | timestamp |

## Fidelity ledger (every deviation detected, quantified, classified)

| class | queries | max rel err | cause | disposition |
|-------|---------|------------:|-------|-------------|
| exact | 33 of 42 offloaded | 0 | integer aggregates / count(DISTINCT)=uniqExact / pushed exactly | — |
| **avg→Float64** | Q4 | ~6e-16 | CH `avg(Int64)` overflowed full-range UserID; fixed to `avg(toFloat64())` | accept (bounded, D0005) |
| **top-N tie** | Q22,32,33,39,40,41 | 0 (tiebroken) | PG Sort tiebreak among equal-ranked rows differs (non-deterministic in both engines) | accept (D0006) — exact with a deterministic tiebreak |
| **nondeterministic** | Q18 | — | `LIMIT 10` with no `ORDER BY` (undefined order by design) | accept (D0006) |

**Zero unexplained deviations.** The two spec-flagged fidelity traps are both
resolved cleanly: **count(DISTINCT) is exact** (CH `count_distinct_implementation`
defaults to `uniqExact` — measured 0 error on all 8 count(DISTINCT) queries, D0004),
and **avg()→Float64** is bounded at Float64 epsilon and only one query (Q4) is even
affected (every other `avg()` is over a small SMALLINT/`length()` column and stays
bit-exact). avg(length) columns (Q28/29) match within the 1e-6 tolerance.

## Code changes (this effort)

| phase | change | file(s) | effect |
|-------|--------|---------|--------|
| 1 | `timestamp` → CH `DateTime64(6, 'UTC')` wire support (the `'UTC'` pin is required — CH server tz is Asia/Kolkata) | `src/shm_offload.c`, `src/shm_deform.cpp`, `src/include/shm_deform.h` | +Q19/25/27/43 (timestamp columns stream) |
| 1b | SHM column cap 64→128 (PG `SHM_IMPL_MAX_COLS` + consumer `IMPL_MAX_COLUMNS`) | `src/shm_producer.c`, `ClickHouse .../Wire/Layout.h` | +Q24 (`SELECT *`, 105 cols) |
| 2 | `avg(bigint)` → `avg(toFloat64())` | `src/deparse.c` | Q4 wrong→bounded avg→Float64 |
| 3 | top-N (ORDER BY/LIMIT/OFFSET) pushdown — **attempted, reverted** (correctness bug, D0015) | — | top-N stays in PG (correct) |
| 4 | `regexp_replace` pattern → `concat('(?s)', …)` (RE2 dotall = PG default) | `src/deparse.c` | Q29 1e-4 count dev → exact |

The only consumer/ABI-side change in the whole effort is the column-cap raise
(1b); the patched-ClickHouse `streamed_table` consumer was rebuilt and its
SharedMemory/Adoption/Wire unit tests stay green (49 passed).

## Performance — native (best-tuned) vs offload, W-sweep under shared cgroup cap

Method (`dev/clickbench/wsweep.sh`): both the PostgreSQL postmaster tree AND the
co-located ClickHouse server are placed in ONE cgroup v2 capped to W cores via
`cpu.max` (quota = W×period), so native and offload compete for the SAME core
budget. The measuring shell stays outside the cap. Per (query, engine, W): median
of N=5 warm runs (min/max + stdev), cores = CPU-seconds/wall from whole-host
`/proc/stat` split into ClickHouse-server (consumer) vs PostgreSQL (host−CH =
producer). Native is tuned for best performance (parallel workers = W, `work_mem`
2 GB so HashAggregate/Sort do not spill, JIT on). speedup = nat_med / off_med.

Full W={8,16} matrix for all 42 offload-eligible queries:
`dev/clickbench/evidence/final/wsweep/RESULTS.md` (N=5 warm/cell). Summary:

**Distribution (speedup = native_median / offload_median):**

| W | queries | offload faster (≥1×) | offload slower (<1×) | ≥3× | mean | max | min |
|--:|--------:|---------------------:|---------------------:|----:|-----:|----:|----:|
| 8 | 42 | **38** | 4 | 7 | 1.97× | 5.23× (Q30) | 0.55× (Q25) |
| 16 | 42 | **37** | 5 | 7 | 2.18× | 6.98× (Q10) | 0.40× (Q25) |

(W=16 central tendency over all 42: arithmetic mean 2.18×, geometric mean 1.79×,
median 1.80× — reported transparently; all three computed over the full set
including the 5 losses, not just the wins.)

**Where offload wins big — CPU-heavy aggregation that native PG can't parallelize.**
The largest wins are `COUNT(DISTINCT)`, many-aggregate, multi-key `GROUP BY`, and
regex queries, where native PG's aggregate is effectively serial (its measured
cores stay ~1–5 even with W=16 available) while the offload's producer scan
parallelizes to W cores and ClickHouse's vectorized aggregate (e.g. `uniqExact`)
finishes fast:

| Q | shape | W=8 speedup | W=16 speedup | nat cores@16 | mechanism |
|--:|-------|------------:|-------------:|-------------:|-----------|
| Q10 | top-N sum+avg+count(DISTINCT) | 5.18× | **6.98×** | 5.13 | native count-distinct serial; CH uniqExact scales |
| Q9 | top-N + count(DISTINCT) | 4.75× | **6.42×** | 5.23 | same |
| Q6 | `COUNT(DISTINCT SearchPhrase)` | 4.36× | **6.21×** | 5.19 | same |
| Q30 | 90× `SUM(ResolutionWidth+n)` | 5.23× | 5.58× | 15.70 | both scale; CH SIMD aggregate faster |
| Q5 | `COUNT(DISTINCT UserID)` | 3.40× | 4.68× | 5.48 | native count-distinct serial |
| Q29 | `REGEXP_REPLACE` group + avg | 3.95× | 4.05× | 13.69 | CH regex+agg vectorized |
| Q17 | `GROUP BY UserID,SearchPhrase` | 3.30× | 3.77× | 1.12 | native hash-agg serial (1 core!) |

(Q17/Q33 native pin at ~1.1 cores even at W=16 — the high-cardinality 2-key
hash-aggregate does not parallelize in PG — so offload wins despite a large grouped
result streamed back.)

**Where offload loses (5 of 42, all at W=16) — cheap projections, nothing to
amortize.** These have no heavy aggregate; native PG's parallel scan + top-N is
already sub-500 ms, and the offload pays a stream round-trip (and, for Q24, a
105-column read-back), with the `ORDER BY`/`LIMIT` still done in PG (top-N pushdown
reverted, D0015):

| Q | shape | W=16 | why |
|--:|-------|-----:|-----|
| Q25 | `SELECT SearchPhrase … ORDER BY EventTime LIMIT 10` | 0.40× | cheap filter; top-N in PG; stream round-trip dominates |
| Q27 | `… ORDER BY EventTime,SearchPhrase LIMIT 10` | 0.40× | same |
| Q24 | `SELECT *` (105 cols) `… LIMIT 10` | 0.70× | wide-row read-back of all 105 columns |
| Q26 | `… ORDER BY SearchPhrase LIMIT 10` | 0.84× | cheap projection |
| Q40 | `CASE WHEN` group + `OFFSET 1000` | 0.87× | modest grouped result; sort+offset in PG |

These five are exactly the queries top-N/ORDER BY pushdown (D0007/D0015) would help
most — pushing the `ORDER BY … LIMIT` into ClickHouse would return k rows instead
of streaming the filtered/grouped relation back. That pushdown was attempted and
reverted for correctness (D0015); it remains the clear next perf lever.

**Net:** on ClickBench's analytic core — scan+filter+aggregate+GROUP BY — the SHM
offload is **2.18× faster on average at an equal 16-core cap (37/42 queries
faster, up to 7×)**, confirming the TPC-H finding (CPU-heavy single-table
aggregation favors the offload) holds across the whole suite. It loses only on the
handful of cheap non-aggregate projections, by a documented and addressable margin.

## Evidence (≥2 independent converging sources per claim)

- **Coverage** (per query): (1) CH `system.query_log` oracle — `streamed_table`
  `QueryFinish`/cxl with `ShmAdoptedBlocks ≥ 1` correlated by a unique
  `log_comment` (poll-retried for the async-flush race, D0002); (2) PG `EXPLAIN`
  plan — `ClickHouseShmScan` present, no residual Aggregate; (3) the dispatched CH
  SQL inspected for the heavy op. Raw artifacts: `dev/clickbench/evidence/final/`.
- **Fidelity**: `cmp_results.py` (numeric-aware, tie-robust) + independent manual
  re-derivations (count(DISTINCT) values, Q4 native-vs-CH-direct, regex divergent
  rows). Confirmed by two independent adversarial reviews (Phase 0; Phases 1/1b/2).
- **Performance**: end-to-end median-of-5 wall time + measured cores split
  (producer vs consumer) under the shared cap; W-sweep shows scaling.
- **Regression**: `dev/clickbench/sanity.sh` green (PASS=17, FAIL=0) after every
  phase; zero `/dev/shm/pgch_*` leaks; consumer unit tests green.

## Reproduction

```sh
RUN_ID=tpchcb dev/bench/ch-bench-server.sh start     # then re-point FDW if port changed (D0013)
RUN_ID=tpchcb make -C dev/clickbench ch && RUN_ID=tpchcb make -C dev/clickbench pg
RUN_ID=tpchcb PGDB=clickbench dev/clickbench/eligibility-scan.sh   # coverage + fidelity (all 43)
RUN_ID=tpchcb PGDB=clickbench QUERIES="$(seq 2 43)" W_LIST="8 16" N=5 \
  OUT=dev/clickbench/evidence/final/wsweep dev/clickbench/wsweep.sh   # perf
```
