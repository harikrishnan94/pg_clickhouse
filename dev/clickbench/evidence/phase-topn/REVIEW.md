# Top-N pushdown — independent adversarial review verdict

A fresh reviewer (separate context, did NOT write the code) was instructed to
attack the work along correctness / fidelity / performance, re-deriving every
claim with its own psql/ClickHouse runs (not trusting the implementer's evidence
files). It was told not to modify any source/docs.

## VERDICT: SOUND — no blocking findings

### A. Correctness (independently re-derived)
- D0015 0-rows / intermittency does **not** reproduce: 12/12 sequential + 5/5
  oracle runs return the correct non-empty rows == native; ShmAdoptedBlocks
  162–164; **zero** `ExceptionWhileProcessing` (no Code-210 broken pipe).
- OFFSET applied exactly **once**: Q39/40/41/43 exact vs native (tiebroken),
  plans collapse to a single `Custom Scan` with no PG `Limit`. Independent
  double-apply check: Q39 offload window is byte-identical to ClickHouse-native
  `LIMIT 10 OFFSET 1000` and DIFFERENT from `OFFSET 2000` (a double-apply would
  have matched 2000).
- HAVING (Q28/29), grouped exprs (Q35/Q36), DATE_TRUNC ordering (Q43): all exact.
- Read-back genuinely from CH: `result_rows` to PG = 10 for Q16/Q33/Q34 over
  1,530,334 / 10,000,000 / 2,620,109 distinct groups.

### B. Fidelity
- Tie cases (Q22/Q32/Q33/Q39/Q40/Q41): pure reshuffle — tied rows genuinely share
  the ORDER BY key value; exact under a deterministic tiebreak (D0006), not value
  bugs.
- avg→Float64 (Q4, rel ≈ 7e-17) and count(DISTINCT) (Q5/Q9/Q10, exact uniqExact)
  unregressed. Q18 returns 10 rows, no error (nondeterministic by design).

### C. Performance / evidence standard
- Full-42 W=16 aggregate recomputed from the raw table matches exactly: arithmetic
  mean 2.18→4.07×, geomean 1.79→2.87×, 41/42 faster (was 37), only Q24 slower
  (0.71×). Not cherry-picked.
- Mechanism shown (not just wall time): result_rows→10 AND plan loses Sort+Limit;
  off_med collapses while native is flat.
- Q24 (the one loss) honestly reported and producer-bound: top-N IS pushed
  (dispatched SQL has ORDER BY+LIMIT, no PG Sort/Limit) yet ~0.72× because the
  105-column stream dominates.

### Non-blocking observation (pre-existing, NOT this change)
Under a 6-way concurrency storm, offload queries can fail with "could not register
SHM streaming background worker" (max_worker_processes=32 exhausted, ~7 workers ×
6 concurrent). Originates in `src/shm_worker.c` (untouched here), reproduces with
a NON-top-N aggregate, fail-closed (clean error, PG alive, zero leaks). Logged as
a known SHM-offload capacity limit (D0018); out of scope for top-N.
