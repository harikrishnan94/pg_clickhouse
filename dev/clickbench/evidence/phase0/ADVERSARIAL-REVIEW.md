# Phase 0 — Independent adversarial review verdict

**Date:** 2026-06-25 · Reviewer: independent agent (did not produce the Phase-0
work), instructed to attack along correctness / fidelity / methodology and assume
the implementer was over-optimistic. All numbers below are the reviewer's own
re-derivations against the live DB (CH 26.6.1.1 pid resolved from port :21002,
`pg.hits` = 10,000,000).

## Verdict: **SOUND — no blocking findings.**

All 9 attack points PASS:

| # | angle | result | reviewer's independent evidence |
|---|-------|--------|---------------------------------|
| 1 | heavy fragment really offloaded (Q35/Q36/Q40 incl.) | PASS | dispatched CH SQL carries the full GROUP BY expr + aggregate; swept all 41 plans → every fully/fully(cxl) has `Aggregate-nodes=0` AND `ClickHouseShmScan>=1` |
| 2 | Q18 fully(cxl) benign cancel | PASS | reproduced `ExceptionWhileProcessing` code 210 broken pipe, `ShmAdoptedBlocks=162`, read_rows=10M; psql returned 10 rows clean |
| 3 | 6 `none` decline reasons | PASS | no CustomScan in any; Q25 with `ORDER BY SearchPhrase` (timestamp removed) DOES get a ClickHouseShmScan → EventTime proven sole blocker; Q1 native count wins |
| 4 | count(DISTINCT) exact | PASS | `COUNT(DISTINCT UserID)`=1530334 and `COUNT(DISTINCT SearchPhrase)`=835093 identical across native/offload/CH-uniqExact/CH-count(DISTINCT); deparses to `count(DISTINCT ...)` not `uniq(` |
| 5 | Q4 avg(UserID) overflow is CH property | PASS | native 2513100748938099884 vs offload −653315757734.87; CH-direct `avg(UserID)` reproduces the wrong value off-path; `avg(toFloat64(UserID))`≈native; other avgs (small ints) genuinely exact |
| 6 | tie reshuffle benign | PASS | Q32 tie pool = 1,374,133 groups all c=1; Q39 OFFSET-1000 window all PageViews=2; both exact with deterministic tiebreak |
| 7 | Q29 regex bounded, no rows dropped | PASS | population COUNT(*) WHERE Referer<>'' = 8,682,923 on BOTH; same 15 groups; worst max_abs=32, max_rel=9.85e-5 |
| 8 | comparator cannot false-"exact" | PASS | 14 adversarial inputs; canonical all-column sort means "exact" ⟺ true row-multiset equality; NULL handled conservatively |
| 9 | wsweep methodology fair | PASS | cgroup cpu controller delegated; both trees in one cap; measuring shell outside; live CH pid from port; N≥5 warm, median+spread; PG producer cores counted as off_prod |

## Two non-blocking nits (FIXED)

1. **Stale constant** — D0004 / PHASE0-COVERAGE.md cited `COUNT(DISTINCT UserID) =
   2037258` (a value accidentally carried from the comparator's synthetic test
   data). Live value is **1530334**. The *exactness* conclusion is unaffected
   (offload == native == 1530334). **Fixed** in both docs (now also cites the
   reviewer-confirmed `SearchPhrase` = 835093).
2. **Q18 SUMMARY under-report** — the machine table showed `ShmBlk=0` for Q18
   because the metric columns filtered `type='QueryFinish'` only; the 162 adopted
   blocks live in the `ExceptionWhileProcessing` row. **Fixed**: the scan now reads
   the cancelled-stream metrics/SQL from the Exception row (Q18 row now shows
   `ShmBlk=162, read_rows=10M, ch_grpby=yes`).

Both fixes are in commit following this review; the coverage/fidelity conclusions
are unchanged.
