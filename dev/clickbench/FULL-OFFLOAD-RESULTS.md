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
| **fully offloaded** | **42** | Q2–43 (all except Q1) |
| **declined (intentional, no column referenced)** | **1** | Q1 |
| **TOTAL offloading the heavy fragment** | **42 / 43** | |

**42 of 43 ClickBench queries push their heavy fragment to ClickHouse over SHM,**
**and the final top-N (ORDER BY/LIMIT/OFFSET) is now pushed into ClickHouse too**
(D0016): for every top-N query the dispatched SQL carries `ORDER BY` +
`LIMIT`/`OFFSET` and the PG plan has no residual `Sort`/`Limit` — so ClickHouse
returns only the top-k window instead of streaming the whole grouped/filtered
relation back. The single non-offload, Q1 (`SELECT COUNT(*) FROM hits`),
references no column, so there is nothing to stream and a parallel seq-scan
`count(*)` wins — expected, not a failure (D0010). Q18 (`LIMIT` with no
`ORDER BY`) now pushes the `LIMIT` and finishes as a clean `QueryFinish` (no more
Code-210 broken pipe from PG cancelling the stream). Starting baseline was 37/43;
this effort added **+5** coverage (timestamp support Q19/24/25/27/43), fixed two
correctness/fidelity bugs (Q4 avg overflow, Q29 regex), and **landed top-N
pushdown** (the dominant remaining structural perf gap).

## Coverage + fidelity table (final, measured)

`fragment`: heavy op pushed to CH. `pg_resid`: residual PG op above the CustomScan.
With top-N pushdown **landed** (D0016), the final `ORDER BY`/`LIMIT`/`OFFSET` is
now pushed into the dispatched ClickHouse SQL, so there is **no** residual
`Sort`/`Limit` above any `Custom Scan` (`pg_sort=0, pg_lim=0` for all 42 — proven
by `eligibility-scan.sh`, raw under `evidence/phase-topn/scan/`). `fidelity`:
offload vs native (tie-robust).

| Q | offloaded | fragment in CH | pg_resid | fidelity | notes |
|--:|:---------:|----------------|----------|----------|-------|
| 1 | **no** | — | Aggregate (native) | exact | no column → declined (expected, D0010) |
| 2 | yes | filtered count | — | exact | |
| 3 | yes | sum+count+avg | — | exact | |
| 4 | yes | `AVG(UserID)` | — | avg→Float64 | rel ~6e-16 (D0005 fix; was wrong) |
| 5 | yes | `COUNT(DISTINCT UserID)` | — | **exact** | uniqExact (D0004) |
| 6 | yes | `COUNT(DISTINCT SearchPhrase)` | — | **exact** | uniqExact |
| 7 | yes | min/max date | — | exact | |
| 8 | yes | GROUP BY + agg | — | exact | ORDER BY pushed (no LIMIT) |
| 9 | yes | top-N + count(DISTINCT) | — | exact | |
| 10 | yes | top-N sum+avg+count(DISTINCT) | — | exact | |
| 11 | yes | top-N + count(DISTINCT) | — | exact | |
| 12 | yes | top-N + count(DISTINCT) | — | exact | |
| 13 | yes | top-N GROUP BY str | — | exact | |
| 14 | yes | top-N + count(DISTINCT) | — | exact | |
| 15 | yes | top-N 2-key GROUP BY | — | exact | |
| 16 | yes | `GROUP BY UserID` | — | exact | large grouped result |
| 17 | yes | `GROUP BY UserID,SearchPhrase` | — | exact | |
| 18 | yes | `GROUP BY UserID,SearchPhrase` | — | nondeterministic | `LIMIT 10` no `ORDER BY` (D0006); LIMIT now pushed, QueryFinish (no cxl) |
| 19 | yes | `extract(minute FROM EventTime)` group | — | exact | timestamp (D0011) |
| 20 | yes | point filter | — | exact | empty in subset |
| 21 | yes | `URL LIKE '%google%'` count | — | exact | |
| 22 | yes | top-N + LIKE | — | tie (exact tiebroken) | D0006 |
| 23 | yes | top-N + LIKE/NOT LIKE + count(DISTINCT) | — | exact | |
| 24 | yes | `SELECT *` (105 cols) filter | — | exact | col-cap raise (D0012) |
| 25 | yes | `ORDER BY EventTime` filtered proj | — | exact | timestamp |
| 26 | yes | `ORDER BY SearchPhrase` filtered proj | — | exact | |
| 27 | yes | `ORDER BY EventTime,SearchPhrase` proj | — | exact | timestamp |
| 28 | yes | `AVG(length(URL))` HAVING top-N | — | exact | |
| 29 | yes | `REGEXP_REPLACE` group HAVING top-N | — | **exact** | regex dotall (D0009 fix) |
| 30 | yes | 90× `SUM(ResolutionWidth+n)` | — | exact | |
| 31 | yes | top-N sum+avg | — | exact | |
| 32 | yes | top-N (WatchID,ClientIP) | — | tie (exact tiebroken) | D0006 |
| 33 | yes | top-N (WatchID,ClientIP) | — | tie (exact tiebroken) | D0006 |
| 34 | yes | `GROUP BY URL` | — | exact | large grouped result |
| 35 | yes | `GROUP BY 1, URL` | — | exact | grouped expr |
| 36 | yes | `GROUP BY ClientIP, ClientIP-1..3` | — | exact | grouped exprs |
| 37 | yes | top-N PageViews (date filter) | — | exact | |
| 38 | yes | top-N Title | — | exact | |
| 39 | yes | top-N `OFFSET 1000` | — | tie (exact tiebroken) | D0006 |
| 40 | yes | `CASE WHEN` group + `OFFSET 1000` | — | tie (exact tiebroken) | D0006 |
| 41 | yes | top-N `OFFSET 100` | — | tie (exact tiebroken) | D0006 |
| 42 | yes | top-N `OFFSET 10000` | — | exact | empty in subset |
| 43 | yes | `DATE_TRUNC('minute',EventTime)` group | — | exact | timestamp |

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
| 3 | top-N (ORDER BY/LIMIT/OFFSET) pushdown — **LANDED** (D0016; resolves the D0015 0-rows bug) | `src/shm_customscan.c` | top-N pushed into CH; PG Sort/Limit removed |
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

Full W={8,16} matrix for all 42 offload-eligible queries, **with top-N pushdown
landed**: `dev/clickbench/evidence/phase-topn/wsweep-full/RESULTS.md` (N=5
warm/cell). The pre-top-N baseline (top-N in PG) is preserved at
`dev/clickbench/evidence/final/wsweep/RESULTS.md`. Summary:

**Distribution (speedup = native_median / offload_median), over ALL 42 (incl. the
one remaining loss), before vs after top-N pushdown:**

| W | set | faster (≥1×) | slower (<1×) | ≥3× | arith mean | geomean | median | max | min |
|--:|-----|-------------:|-------------:|----:|-----------:|--------:|-------:|----:|----:|
| 16 | **before** (top-N in PG) | 37 | 5 | 7 | 2.18× | 1.79× | 1.80× | 6.98× (Q10) | 0.40× (Q25) |
| 16 | **after** (top-N in CH) | **41** | **1** | **18** | **4.07×** | **2.87×** | **2.01×** | **25.8× (Q33)** | 0.71× (Q24) |
| 8 | **after** | 41 | 1 | 16 | 3.01× | 2.41× | 1.89× | 15.5× (Q33) | 0.74× (Q24) |

All central-tendency numbers are over the full 42 (including the single remaining
loss, Q24), not just the wins. Top-N pushdown lifts the W=16 geomean **1.79× →
2.87×** and flips 4 of the 5 prior losers (Q25/26/27/40) to faster; only Q24
remains <1× (root-caused in D0017).

**Biggest movers — large grouped/filtered result no longer streamed back.** These
are precisely the queries that previously streamed a huge grouped/filtered
relation to PG for it to Sort+Limit; now CH returns k=10 rows. Mechanism is
proven by CH `result_rows` (rows returned to PG) collapsing to 10 — see
`evidence/phase-topn/MECHANISM.md`:

| Q | shape | W=16 before | W=16 after | off_med ms before→after | rows CH returned (before→after) |
|--:|-------|------------:|-----------:|-------------------------|---------------------------------|
| Q33 | `GROUP BY WatchID,ClientIP` (near-unique) | 2.01× | **25.8×** | 4619→358 | 10,000,000 → 10 |
| Q17 | `GROUP BY UserID,SearchPhrase` | 3.77× | **14.7×** | (native pinned ~1 core) | high-card → 10 |
| Q19 | `GROUP BY UserID,minute,SearchPhrase` | — | **12.7×** | — | high-card → 10 |
| Q35 | `GROUP BY 1,URL` | 1.28× | **7.26×** | 1982→318 | 2,620,109 → 10 |
| Q34 | `GROUP BY URL` | 1.50× | **7.86×** | 1703→319 | 2,620,109 → 10 |
| Q16 | `GROUP BY UserID` | 2.05× | **4.54×** | 543→240 | 1,530,334 → 10 |
| Q32 | `GROUP BY WatchID,ClientIP` (filtered) | 1.54× | **4.81×** | 820→262 | — → 10 |
| Q31 | `GROUP BY SearchEngineID,ClientIP` | 1.88× | **3.59×** | 503→261 | — → 10 |
| Q36 | `GROUP BY ClientIP, ClientIP-1..3` | — | **4.54×** | — | — → 10 |

**Prior losers — flipped to wins** (CH now sorts+limits instead of streaming the
filtered relation back; `result_rows` 1.37M → 10 for Q25):

| Q | shape | W=16 before | W=16 after |
|--:|-------|------------:|-----------:|
| Q25 | `SELECT SearchPhrase … ORDER BY EventTime LIMIT 10` | 0.40× | **1.70×** |
| Q27 | `… ORDER BY EventTime,SearchPhrase LIMIT 10` | 0.40× | **1.71×** |
| Q26 | `… ORDER BY SearchPhrase LIMIT 10` | 0.84× | **1.67×** |
| Q40 | `CASE WHEN` group + `OFFSET 1000` | 0.87× | **1.77×** |

**The one remaining loss — Q24 `SELECT *` (105 cols), 0.70× → 0.72× (unchanged).**
This was pre-registered to improve and did NOT — recorded honestly (D0017). Root
cause (measured, not asserted): Q24 is **producer-bound** — streaming all 10M rows
× 105 columns into shared memory dominates (W-sweep cores: off_prod≈13.6 vs
off_cons≈1.6). Top-N pushdown shrinks only the already-tiny read-back, so it cannot
move Q24's wall time; native's parallel seq-scan + bounded top-N (430 ms) wins.
Every other top-N query (1–10 narrow columns) improved sharply — isolating column
width / producer cost as the cause, exactly as the cores split predicts.

**Net:** with top-N pushed into ClickHouse, on ClickBench's analytic core the SHM
offload is **4.07× faster on average (geomean 2.87×) at an equal 16-core cap,
41/42 queries faster (18 of them ≥3×, up to 25.8×)**, up from 2.18× mean / 37-of-42
before. The dominant remaining structural gap (D0007) is closed: only one query
(Q24, the very wide `SELECT *`) is slower, and for a measured, producer-side reason
unrelated to top-N.

## Evidence (≥2 independent converging sources per claim)

- **Coverage** (per query): (1) CH `system.query_log` oracle — `streamed_table`
  `QueryFinish` with `ShmAdoptedBlocks ≥ 1` correlated by a unique `log_comment`
  (poll-retried for the async-flush race, D0002); (2) PG `EXPLAIN` plan —
  `ClickHouseShmScan` present, no residual Aggregate **and (for top-N) no residual
  `Sort`/`Limit`**; (3) the dispatched CH SQL inspected for the heavy op **plus
  `ORDER BY` + `LIMIT`/`OFFSET`**. Raw: `dev/clickbench/evidence/phase-topn/scan/`
  (top-N), `dev/clickbench/evidence/final/` (prior phases).
- **Fidelity**: `cmp_results.py` (numeric-aware, tie-robust) + a deterministic
  tiebreak for the top-N tie cases (`dev/clickbench/tiebreak_check.sh`,
  `evidence/phase-topn/tiebreak/`): Q18/22/24/25/26/27/32/33/39/40/41 all exact
  under a total order → pure tie reshuffle; OFFSET windows match native (no
  double-apply). Plus independent re-derivations (count(DISTINCT), Q4, regex).
- **Mechanism** (top-N): CH `result_rows` returned to PG collapses to k=10 while
  the full grouped/filtered relation was 1.4M–10M rows
  (`evidence/phase-topn/MECHANISM.md`); converges with the wall-time drop and the
  plan change.
- **Performance**: end-to-end median-of-5 wall time + measured cores split
  (producer vs consumer) under the shared cap; W-sweep before/after at
  `evidence/phase-topn/wsweep-full/` (pre-registration in
  `evidence/phase-topn/PRE-REGISTRATION.md`).
- **Regression**: `dev/clickbench/sanity.sh` green (PASS=17, FAIL=0) and
  `test/shm/verify_offload.sh` green (PASS=137, FAIL=0) after the top-N change;
  zero `/dev/shm/pgch_*`/socket/worker leaks; consumer unchanged (pure PG-side
  planner change — no ClickHouse rebuild).

## Reproduction

```sh
RUN_ID=tpchcb dev/bench/ch-bench-server.sh start     # then re-point FDW if port changed (D0013)
RUN_ID=tpchcb make -C dev/clickbench ch && RUN_ID=tpchcb make -C dev/clickbench pg
RUN_ID=tpchcb PGDB=clickbench dev/clickbench/eligibility-scan.sh   # coverage + fidelity (all 43)
RUN_ID=tpchcb PGDB=clickbench dev/clickbench/tiebreak_check.sh     # tie-robust fidelity (top-N)
RUN_ID=tpchcb PGDB=clickbench QUERIES="$(seq 2 43)" W_LIST="8 16" N=5 \
  OUT=dev/clickbench/evidence/phase-topn/wsweep-full dev/clickbench/wsweep.sh   # perf (top-N landed)
```
