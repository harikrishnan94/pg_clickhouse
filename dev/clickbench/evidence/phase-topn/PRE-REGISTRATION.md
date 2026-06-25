# Top-N pushdown — pre-registration (written BEFORE measuring perf)

Branch: streamed-table-shm-offload. Change: push the final ORDER BY + LIMIT
[OFFSET] of an offloaded single-table aggregate/projection query into the
dispatched ClickHouse `streamed_table()` SQL (UPPERREL_ORDERED + UPPERREL_FINAL
CustomPaths whose parent reuses the underlying grouping/base rel's fpinfo).

## Mechanism (what should change, and why)
Before: `Limit -> Sort -> Custom Scan (ClickHouseShmScan)`; CH streamed the WHOLE
grouped/filtered relation back; PG did the Sort+Limit. After: a single
`Custom Scan` whose dispatched SQL ends in `... ORDER BY ... LIMIT k [OFFSET m]`,
so CH returns only the top-k window. The PG `Sort` and `Limit` vanish, and the
rows transferred PG<-CH collapse from the full relation to k.

Independent mechanism signals to confirm (not wall time alone):
- CH query_log: dispatched SQL contains ORDER BY + LIMIT/OFFSET; ShmAdoptedBlocks>=1.
- PG EXPLAIN: no Sort, no Limit above the topmost ClickHouseShmScan.
- Wall time: median of N>=5 warm runs under a shared cgroup cpu.max cap.

## Correctness gate (already GREEN before any perf claim)
- eligibility-scan (all 43): every top-N query now ch_ord=yes, ch_lim=yes,
  pg_sort=0, pg_lim=0. Coverage 42/43 (Q1 declined, expected). No regressions.
- tie-robust fidelity (deterministic tiebreak) exact for Q18,22,24,25,26,27,32,33,
  39,40,41 (the DIFF/approx-at-plain-ORDER-BY cases collapse to exact -> pure tie
  reshuffle, D0006). OFFSET windows (Q39/40/41) match native exactly -> no
  double-apply. D0015 trivial query stable x3 (no 0-rows intermittency).
- sanity.sh 17/0, verify_offload.sh 137/0, zero leaks. Consumer unchanged.

## Predicted magnitudes (speedup = native_median / offload_median; shared W-core cap)
Prior (top-N in PG) -> predicted (top-N in CH). Predictions are directional with a
predicted band; a miss is a finding to investigate, not to hand-wave.

### Projection top-N (the prior losers; round-trip of the full filtered relation dominated)
| Q | shape | prior off_ms@16 | prior sp@16 | predicted sp@16 |
|--:|-------|----------------:|------------:|-----------------|
| Q25 | SELECT SearchPhrase ORDER BY EventTime LIMIT 10 | 1088 | 0.40x | >=1.0x (CH returns 10, not the filtered relation) |
| Q27 | + ORDER BY EventTime,SearchPhrase | 1088 | 0.40x | >=1.0x |
| Q26 | ORDER BY SearchPhrase LIMIT 10 | 521 | 0.84x | >=1.0x |
| Q24 | SELECT * (105 cols) ORDER BY EventTime LIMIT 10 | 609 | 0.70x | >=0.9x (10 wide rows back, not the filtered relation) |

### Aggregate top-N with a LARGE grouped result streamed back today
| Q | shape | prior off_ms@16 | prior sp@16 | predicted sp@16 |
|--:|-------|----------------:|------------:|-----------------|
| Q33 | GROUP BY WatchID,ClientIP (no filter; ~near-unique -> huge grouped result) | 4619 | 2.01x | >=3x (biggest grouped-result collapse) |
| Q16 | GROUP BY UserID (~1.5M groups) | 543 | 2.05x | >=2.3x |
| Q34 | GROUP BY URL (very high card) | 1703 | 1.50x | >=1.8x |
| Q35 | GROUP BY 1,URL | 1982 | 1.28x | >=1.6x |
| Q32 | GROUP BY WatchID,ClientIP (SearchPhrase<>'') | 820 | 1.54x | >=1.7x |
| Q31 | GROUP BY SearchEngineID,ClientIP (filtered) | 503 | 1.88x | >=1.9x |
| Q40 | CASE-group + OFFSET 1000 | 678 | 0.87x | >=1.0x |

## What would falsify / surprise
- If a projection loser stays <1x: the round-trip was NOT the bottleneck (e.g. the
  CH-side full scan + sort dominates regardless) -> investigate read_rows, cores.
- If a large-grouped query does NOT improve: the grouped result was not actually
  the cost (transfer was cheap; the GROUP BY itself dominates) -> investigate.
- Effect must exceed run-to-run noise (report min/max + stdev). If effect <= noise,
  say so.
