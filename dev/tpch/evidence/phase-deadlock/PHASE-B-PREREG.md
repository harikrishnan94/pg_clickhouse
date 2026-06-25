# Phase B (Fix) — PRE-REGISTRATION (written BEFORE editing/measuring)

## Scope + acceptance
Make the hash-join BUILD side not retain zero-copy adopted SHM columns, so Q8/Q9/Q11/
Q14 fully offload with no deadlock, and the 9 working queries + unit tests do not
regress.

## Hypothesised fix
In `src/Interpreters/HashJoin/HashJoin.cpp`, free function
`materializeColumnsFromRightBlock(...)`: after the existing const/sparse/lowcardinality
un-wrapping and before assigning `column.column = actual_column`, call
`actual_column = actual_column->convertToFullColumnIfAdopted();`. This is the single
chokepoint through which both `HashJoin::addBlockToJoin` (679/685) and
`ConcurrentHashJoin::addBlockToJoin` (283) pass every stored right block. Result: the
hash table stores owned columns; the source's adopted column (and its RetainToken) is
released as soon as the build transform pulls the next chunk → the SHM slot returns to
EMPTY → the producer keeps publishing → the build completes → the probe runs.
`.cpp`-only change (fast rebuild, no header cascade).

## Predicted observables (pre-registered)
1. Re-running Q14 offload: `system.processes.read_rows` ADVANCES past 1,048,576 and
   climbs toward ~60M (lineitem fully probed); the query returns the correct
   `promo_revenue` (matches native within Decimal/Float64 fidelity); NO Code 781.
2. During the run, the part build-side ring slots cycle (retain_refcount returns to 0;
   sequence advances past 1) instead of being pinned at seq1/ref1.
3. query_log oracle: streamed_table QueryFinish, ShmAdoptedBlocks≥1, the JOIN+sum in
   the dispatched CH SQL, no residual PG aggregate; same for Q8/Q9/Q11.
4. No regression: Q1,Q3,Q4,Q5,Q6,Q7,Q10,Q12,Q19 stay correct; unit tests
   `*Adopted*` green; verify_offload.sh 137/0; W-sweep on the 9 shows no new
   hotspot/regression (build side gains a copy of the SMALL dimension tables only —
   predicted within noise; probe side unchanged zero-copy).

## Failure modes to watch (would falsify / need investigation)
- read_rows still freezes → the hash table is not the (only) holder; check squashing
  accumulation or another retain path (would need to also materialize at squash ingest).
- A working query slows measurably on the W-sweep → the build-side copy is non-trivial;
  quantify vs noise.
- Unit test break → convertToFullColumnIfAdopted interaction with const/nullable/etc.
