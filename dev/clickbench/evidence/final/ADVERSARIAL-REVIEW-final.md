# Final adversarial review — whole ClickBench SHM-offload effort

Independent reviewer, own measurements (CH port 21000, hits 10M). VERDICT: SOUND —
all 10 attack points PASS, no blocking findings.

COVERAGE: 42/43 re-derived (Q5/6/19/24/34/43 oracle QueryFinish + ShmAdoptedBlocks>=162
+ heavy op in CH SQL + no residual HashAggregate). Q1 genuinely declines (no column,
no ClickHouseShmScan, used_ch=off). Q18 fully(cxl) honest (blk=161, Code-210 cancel).
FIDELITY: Q29 exact tiebroken (dotall in dispatched SQL; no-newline case unaffected);
Q4 avg rel 3.5e-16 (dispatched avg(toFloat64)), small-int avg bit-exact; count(DISTINCT)
Q5/6 + all 8 exact, no counterexample; top-N ties honest (Q32 all c=1, Q39 all PageViews=2;
exact with tiebreak).
PERFORMANCE: own median-of-5 W=16 — Q6 native 4036ms vs offload 447ms (9.02x uncapped;
direction+magnitude hold), Q25 619 vs 1085ms (0.57x loss real, LOAD overhead ~0).
Mechanism real: Q6/Q17 native plans have NO Gather/Workers (serial count-distinct /
hash-agg). No cherry-picking: recomputed W=16 mean over ALL 42 = 2.1838x (matches 2.18x);
geomean 1.79x, median 1.80x; 5 losses are the documented ones.
REVERT: post-revert Q34 + Q43(OFFSET) return 10 correct rows (exact tiebroken) — revert
left nothing broken; top-N in PG is correct.
No /dev/shm/pgch_* leaks, no stray workers.

"I found no exaggeration, no fabricated mechanism, and no hidden wrong answers...
if anything the W=16 win magnitudes are conservative versus my uncapped re-measurements."
