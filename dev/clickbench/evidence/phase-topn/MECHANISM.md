# Top-N pushdown — mechanism evidence (independent of wall time)

The perf win must be explained by a mechanism, not asserted from timing alone.
Three independent instrument classes converge on the same story: ClickHouse now
returns only the top-k window to PostgreSQL instead of the whole grouped/filtered
relation, and the PG Sort+Limit disappears.

## 1. CH `system.query_log.result_rows` (rows CH returned to PG) collapses to k

Captured by running the offloaded query and reading `result_rows` of the
`streamed_table()` QueryFinish (correlated by log_comment):

| Q | rows CH returned to PG (after) | full grouped/filtered relation (before) | reduction |
|--:|-------------------------------:|----------------------------------------:|-----------|
| Q33 `GROUP BY WatchID,ClientIP LIMIT 10` | **10** | 10,000,000 distinct groups | ~1,000,000x |
| Q16 `GROUP BY UserID LIMIT 10`           | **10** | 1,530,334 distinct UserID | ~153,000x |
| Q34 `GROUP BY URL LIMIT 10`              | **10** | 2,620,109 distinct URL | ~262,000x |
| Q25 `... ORDER BY EventTime LIMIT 10`    | **10** | 1,374,133 rows (SearchPhrase<>'') | ~137,000x |

(`read_rows` stays 10M — CH still scans the whole streamed table to compute the
GROUP BY/sort; only the *returned* result collapses to k. That is exactly the
intended lever.)

## 2. PG EXPLAIN plan: no residual Sort/Limit above the CustomScan

eligibility-scan (all 43): every top-N query now `pg_sort=0, pg_lim=0` (was
`pg_sort=1, pg_lim=1` in D0007). The dispatched CH SQL carries `ORDER BY` +
`LIMIT`/`OFFSET` (`ch_ord=yes, ch_lim=yes`). For the OFFSET queries there is no
PG `Limit` node at all -> OFFSET applied once, in CH (no double-apply).

## 3. Wall time (wsweep, shared cgroup cap, N=5 warm): offload_median collapses, native unchanged

| Q | native_med@16 (unchanged) | offload_med@16 before | offload_med@16 after |
|--:|--------------------------:|----------------------:|---------------------:|
| Q33 | 9248 ms | 4619 ms | **358 ms** |
| Q16 | 1078 ms | 543 ms | **240 ms** |
| Q34 | 2544 ms | 1703 ms | **319 ms** |
| Q25 | 439 ms | 1088 ms | **258 ms** |

The native side does not change (it already did Sort+Limit); the entire delta is
on the offload side, and it tracks the result_rows collapse — i.e. the win is the
top-N pushdown, not a baseline shift.

## Convergence
For each query the three classes agree in direction and roughly in magnitude:
result_rows -> 10 (class 1), Sort/Limit gone (class 2), offload wall time drops
to the floor while native is flat (class 3). No single source is relied on alone.
