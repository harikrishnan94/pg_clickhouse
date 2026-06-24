# Adversarial review — Phases 1 (timestamp) + 1b (wide row) + 2 (avg)

Independent reviewer, own measurements against live server (CH port 21000).
VERDICT: ALL THREE SOUND — no blocking findings.

Timestamp: extract(hour/minute/second)+date_part all exact native==offload (tz
complete; +5:30 pin correct). min/max of all 3 timestamp cols exact; ClientEventTime
range 1971-2035 exercises epoch rebase near 1970 + far future (UInt32 DateTime would
fail; signed Int64 DateTime64(6) handles it). Whole-second data: 0 fractional-us rows.
Wide row: Q24 byte-identical all 105 cols x 10 rows; oracle QueryFinish blk=499;
boundary N=64/65/67/105 all offload+exact; SELECT * ORDER BY WatchID LIMIT 5 exact.
Narrow regression: Q2=202134 both; sanity.sh PASS=17 FAIL=0; no leaks.
avg: Q4 abs=884 rel=3.5e-16 (was rel~1.0); avg(int2/int4)/length-avg stay plain+exact;
wrap is avg-only (sum/min/max/count(int8) dispatch plain); applies to int8 exprs too.

OUT-OF-SCOPE CONCERN (non-blocking, logged D0014): SUM(int8) (e.g. SUM(UserID))
overflows in offload (native 2.51e25 vs offload -6.5e18, wrong sign) — same Int64
accumulation class as the old avg bug. NO ClickBench query sums a wide int8 (Q3's
SUM is over int2 AdvEngineID), so it does not affect any of the 43 queries.
