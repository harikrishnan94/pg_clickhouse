# Phase-deadlock evidence — index & caveats

Root cause + fix for the Q8/Q9/Q11/Q14 hash-join-BUILD deadlock. See `MECHANISM.md`
and `dev/tpch/FULL-OFFLOAD-DECISIONS.md` D0008 for the writeup.

## Authoritative (taken in ISOLATION on an idle host)
- `PRE-REGISTRATION.md`, `PHASE-B-PREREG.md` — pre-registered hypotheses + predictions.
- `MECHANISM.md` — root cause with the 3 converging evidence classes.
- `dl_q14_diag.{liveness.log,gdb.key.txt,producers.txt,ringstates.txt}` — the FROZEN state
  (read_rows stuck at 1,048,576; CH epoll-parked; PG producers ring-full; part build
  slots refcount=1 retained vs lineitem probe slots refcount=0).
- `dl_q14_default.err` — canonical `Code 781 SHM_PRODUCER_STALL` on the part build side.
- `dl_q14_fixed.{out,driver.log}` — POST-FIX Q14: read_rows advances, returns
  16.647594941615093 (native 16.6475949416150953), clean teardown.
- `dl_q9_chk.*`, `dl_q3_chk.*`, `dl_q5_chk.*` — isolated post-fix offload + query_log
  oracle (Q9/Q5 full join+groupby pushed; correct).
- `wsweep-clean/RESULTS.md` — W-sweep on an IDLE host (perf no-regression).

## CAVEAT — do NOT read these literally (taken under contention)
- `elig/SUMMARY.md`, `elig9/SUMMARY.md` — eligibility scans run back-to-back while a
  concurrent adversarial reviewer also issued offload queries; multi-source queries
  raced `max_worker_processes=32` (`could not register SHM streaming background worker
  x/8`) and fell back to non-SHM, and the harness diff is RAW/unsorted (counts ORDER BY
  reordering + CHAR(n) padding + Decimal trailing zeros as "differences"). They show
  transient `scan_only?`/`ON_ERR`/`DIFF(N)` that do NOT reflect steady state. The
  isolated re-runs above are authoritative; `wsweep.log` (NOT `-clean`) is likewise the
  contaminated first W-sweep.
