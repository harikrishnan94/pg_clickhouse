# Phase 1 — timestamp wire-type support (Q19, 25, 27, 43)

**Date:** 2026-06-25 · Mechanism: map PG `timestamp` → ClickHouse `DateTime64(6,
'UTC')` over the SHM wire so timestamp columns (EventTime/ClientEventTime/
LocalEventTime) can stream. Pure PG-producer-side change — the patched ClickHouse
consumer already adopts `DateTime64` (Wire/Layout.h tag 18); **no consumer/ABI
change**, so the `streamed_table` consumer tests are unaffected.

## Pre-registration (before measuring)
- Hypothesis: `TIMESTAMPOID` unmapped in `pgch_pg_type_to_ch_wire` → 5 queries
  (Q19/24/25/27/43) decline. Wiring `TIMESTAMPOID → SHM_WIRE_DATETIME64` +
  converter unlocks them.
- Predicted coverage: +5. Predicted fidelity: exact (EventTime is whole-second).
- Predicted perf: producer-scan-bound, comparable to other offloaded queries.

## Observed
- Coverage: **+4** (Q19, 25, 27, 43). **Q24 NOT unlocked by timestamp alone** —
  the correctness gate surfaced a *second*, independent blocker: `SELECT *` projects
  105 columns > the SHM 64-column cap (`SHM_IMPL_MAX_COLS`). Deferred to Phase 1b
  (D0012). The prediction of +5 was wrong by one because of this second blocker —
  recorded, not hidden.
- Fidelity: **exact** for all 4 (tie-robust for the top-N Q19/Q43).
- A real **correctness bug** was caught by the gate and fixed before commit: the
  timezone issue below (D0011).

## The timezone fix (D0011) — caught by the correctness gate
First implementation used bare `DateTime64(6)`. Q19 (`extract(minute FROM
EventTime)`) came back with **every minute shifted +30** (`offload_m =
(native_m + 30) mod 60`): native `5,29,30,8,...` vs offload `35,59,0,38,...`. Root
cause: the CH server time zone is **`Asia/Kolkata` (+5:30)**; a bare `DateTime64`
inherits it, and `toMinute()` (the deparse of `extract(minute …)`) is
time-zone-dependent. PG `extract` is tz-naive. Fix: declare `DateTime64(6,
'UTC')`. Q25/27 (`ORDER BY EventTime`) and Q43 (`date_trunc`→`toStartOfMinute`)
were already exact because ordering and whole-minute truncation are tz-invariant;
only minute-of-hour *extraction* shifts — which is exactly why only Q19 failed.
After the UTC pin, Q19 is exact (tiebroken `exact|10|10|0|0`).

## Evidence (≥3 independent classes converge)

### 1. query_log oracle (authority)
| Q | strmd | ShmAdoptedBlocks | read_rows | dispatched CH carries |
|---|------:|-----------------:|----------:|-----------------------|
| 19 | 1 | 162 | 10,000,000 | `toMinute(eventtime)` (UTC), GROUP BY, `DateTime64(6, 'UTC')` |
| 25 | 1 | 166 | 10,000,000 | filter `searchphrase != ''`, ORDER BY in PG |
| 27 | 1 | 166 | 10,000,000 | filtered projection |
| 43 | 1 | 162 | 10,000,000 | `toStartOfMinute(eventtime)`, GROUP BY, date filter |

### 2. fidelity vs native (cmp_results.py, tie-robust)
- Q19 tiebroken: `exact|10|10|0|0|0|0`
- Q25: `exact` · Q27: `exact` · Q43 tiebroken: `exact|10|10|0|0|0|0`

### 3. PG plan
All four: `pg_agg=0` above a `Custom Scan (ClickHouseShmScan)` (GROUP BY/aggregate
offloaded; for Q25/27 the filtered scan is offloaded). A residual `Sort`/`Limit`
remains in PG (top-N not pushed — D0007, Phase 3).

### 4. perf (wsweep.sh, W=8, N=5 warm, shared cgroup cpu cap)
| Q | nat_med_ms | nat_cores | off_med_ms | off_prod | off_cons | speedup |
|---|-----------:|----------:|-----------:|---------:|---------:|--------:|
| 19 | 3791 | 3.07 | 2504 | 2.16 | 0.67 | **1.51×** |
| 25 | 631 | 7.04 | 1112 | 3.65 | 0.22 | **0.57×** (offload slower) |
| 43 | 678 | 6.80 | 414 | 7.41 | 0.22 | **1.64×** |

**Q25 is slower offloaded** — and honestly so: its heavy fragment is only a cheap
`SearchPhrase <> ''` filter; the `ORDER BY EventTime LIMIT 10` runs in PG, so the
offload pays a stream round-trip with no aggregation to amortize it, while native
PG's parallel top-N is already fast. This is the clearest motivation for Phase 3
(top-N pushdown): pushing `ORDER BY EventTime LIMIT 10` into CH would return 10
rows instead of streaming the whole filtered relation back.

## Regression
`dev/clickbench/sanity.sh` green (PASS=17, FAIL=0). No consumer change → patched-CH
`streamed_table` tests unaffected.
