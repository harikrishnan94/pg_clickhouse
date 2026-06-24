# Phase 2 — avg(bigint) Float64 fix (Q4)

Pre-registration: CH avg(Int64) overflows full-range UserID -> wrong answer.
Deparse avg(int8)->avg(toFloat64(..)) so accumulation is Float64. Predicted:
Q4 becomes bounded avg->Float64 (~1e-15 rel); smallint avgs stay exact.

Observed:
- Q4 native AVG(UserID)=2513100748938099884; offload=~2513100748938099700.
  max abs err ~184-1900, max rel err ~6.1e-16 (Float64 epsilon). Was -653315757734
  (rel ~1.0) before -> a wrong answer; now bounded+documented avg->Float64.
- Dispatched CH SQL: `avg(toFloat64(userid))` (oracle QueryFinish, blk=164, read 10M).
- Regression: avg(ResolutionWidth) (smallint) native==offload==1508.8046441 (exact,
  unchanged -- int8-only scope). Q3/Q10/Q28/Q31/Q32/Q33 avg unaffected.
- Perf wsweep W=8 N=5: native 486ms vs offload 376ms = 1.29x.
