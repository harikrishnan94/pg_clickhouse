# TPC-H full SHM-offload — results write-up

**Date:** 2026-06-25 · **Branch:** streamed-table-shm-offload · **DB:** tpch_sf10
(SF10, schema `pg`) · **ClickHouse:** patched `streamed_table` build (repo
`/home/ubuntu/ClickHouse`, branch `streamed_table`), RUN_ID=tpchcb, HTTP 21002.

This is the consolidated result of extending SHM-offload coverage across the 22
TPC-H queries and measuring offload vs best-tuned native PostgreSQL. "Offloaded"
is proven from ClickHouse `system.query_log` (a `streamed_table()` `QueryFinish`
with `ShmAdoptedBlocks ≥ 1`, the heavy fragment in the dispatched SQL) **and** the
PostgreSQL plan (no residual heavy op above the topmost `ClickHouseShmScan`), per
`FULL-OFFLOAD-DECISIONS.md` D0001.

Commits: `ffd024e` (Phase 0 baseline) · `52d52e4`,`ec15978`,`c5db16b` (Phase 1) ·
ClickHouse `498959fa5ee` + pg_clickhouse `5ef2de8` (Phase 2) · `5e0cfdb` (Phase 3).

## Coverage progression

| Milestone | correct answers | fully offloaded | key change |
|-----------|-----------------|-----------------|------------|
| Baseline (HEAD bbd2d00) | 7/22 | 1 (Q4) | — |
| Phase 1 | 9/22 | 3 (Q1,Q4,Q6) | single-table Decimal aggregate output |
| Phase 2 | 13/22 | 4 (+Q12) | CH READONLY squash fix + bpchar trailing-blank trim |
| Phase 3 | 11/22* | **9** (Q1,Q3,Q4,Q5,Q6,Q7,Q10,Q12,Q19) | numeric aggregate output over joins |

\* Phase 3 deliberately trades Q8/Q11 (scan_only-correct → join+agg deadlock-error)
for +5 fully-offloaded revenue joins; see D0006. The 9 fully-offloaded + Q13,Q18
scan_only-correct produce correct answers; Q8/Q9/Q11/Q14 hit the residual deadlock.

## Final per-query status

| Q | status | fragment in ClickHouse | blocker (if not full) |
|---|--------|------------------------|-----------------------|
| 1 | **fully** | `sum×4, avg×3, count(*) GROUP BY` over lineitem | — |
| 2 | blocked | — | correlated `min` subquery → rescan of SHM scan |
| 3 | **fully** | customer⋈orders⋈lineitem + `sum` GROUP BY | — |
| 4 | **fully** | orders SEMI lineitem + `count(*)` GROUP BY | — |
| 5 | **fully** | 6-way join + `sum` GROUP BY n_name | — |
| 6 | **fully** | lineitem filter + `sum` | — |
| 7 | **fully** | 6-way join + `sum` GROUP BY | — |
| 8 | blocked | (join+agg) | multi-ring join+agg deadlock (D0006) |
| 9 | blocked | (join+agg) | multi-ring join+agg deadlock (D0006) |
| 10 | **fully** | 4-way join + `sum` GROUP BY (381105 groups) | — |
| 11 | blocked | (join+agg) | join+agg deadlock; scalar HAVING subquery |
| 12 | **fully** | orders⋈lineitem + `sum(case)→bigint` GROUP BY | — |
| 13 | scan_only | inner customer⟕orders + `count` (outer GROUP BY in PG) | two-level aggregation |
| 14 | blocked | (join+agg) | multi-ring join+agg deadlock (D0006) |
| 15 | blocked | — | view + correlated scalar `max` subquery `{p1}` |
| 16 | blocked | — | `NOT IN` anti-join + rescan + count(distinct) |
| 17 | blocked | — | correlated `avg` subquery → rescan |
| 18 | scan_only | IN-subquery lineitem scan (joins+agg in PG) | IN-subquery with aggregate |
| 19 | **fully** | lineitem⋈part + `sum` (OR-predicate) | — |
| 20 | blocked | — | nested correlated subqueries → rescan |
| 21 | blocked | (partial join) | `NOT EXISTS` anti-join + join+agg deadlock |
| 22 | blocked | — | correlated `avg` subquery `{p1}` + `NOT EXISTS` anti-join |

## Mechanisms unlocked (what was fixed)

1. **Single-table Decimal aggregate output** (`shm_customscan.c`): the
   NUMERICOID-output decline was gating every `sum`/`avg` revenue query. The
   deparser already emits bare `sum`/`avg`; read-back via `numeric_in` accepts
   CH's Decimal/Float64 text. Unlocked Q1, Q6.
2. **ClickHouse READONLY squash crash** (CH `498959fa5ee`): the join pipeline's
   `SimpleSquashingTransform` mutated a zero-copy adopted column in place
   (`IColumn::mutate()` is a no-op at refcount 1). Added
   `convertToFullColumnIfAdopted()`; the join no longer crashes (`Code 164`).
3. **bpchar (CHAR(n)) equality under pushdown** (`shm_offload.c`): PG's
   `bpchareq` ignores trailing blanks but a pushed CH `String =` is byte-exact, so
   `c_mktsegment='BUILDING'` matched 0 padded rows. Strip trailing blanks from
   bpchar at columnization. This (not a CH bug) was the dominant cause of the
   "empty join" symptom across Q3,Q5,Q7,Q8,Q11,Q17,Q19,Q20.
4. **Numeric aggregate output over joins** (`shm_customscan.c`): with the join
   correct, push the whole join+aggregate fragment. Unlocked Q3,Q5,Q7,Q10,Q19.

## Fidelity ledger (offload vs native)

Every fully-offloaded query's answer equals native within bounded, documented
deviations — there are **no unexplained deviations**:

- **Decimal `sum`/`min`/`max`**: numerically exact; differ from PG only in display
  scale (trailing zeros, e.g. `439855.3250` vs `439855.325`). Verified by
  full-precision re-derivation (Phase 1 F1/F4; Phase 3 Q3/Q5/Q10/Q19).
- **`avg(Decimal)` → Float64**: max relative error ≤ **1.6e-16** (≤ machine
  epsilon), independently re-derived (F3). Q1 only among the fully-offloaded set.
- **`CHAR(n)` projection**: offload strips trailing blanks ("GERMANY" vs
  "GERMANY⎵…"); semantically identical (F5). Detected as `exact(bpchar)`.
- **`count` / integer aggregates**: bit-exact.

## Performance — native vs offload, W-sweep under shared cgroup cpu.max cap

Method: both the PostgreSQL postmaster tree and the ClickHouse server in one
cgroup capped to `cpu.max = W·100ms`; median of N=5 warm runs (spread shown);
cores = CPU-s/wall split CH vs PG. Harness `dev/tpch/wsweep.sh`. Raw:
`dev/tpch/evidence/phase1/wsweep/RESULTS.md` (Q1/Q4/Q6),
`dev/tpch/evidence/phase3/wsweep/RESULTS.md` (Q3/Q5/Q10).

### Single-table aggregate (Q1) — offload wins 3.2–4.3×

(See PHASE1-RESULTS.md.) At W=16, offload 745ms@10.7 cores vs native 2386ms@15.9
cores — faster AND fewer cores, because CH aggregates vectorised over zero-copy
Decimal columns vs PG evaluating 7 numeric aggregates/row over 60M rows.

### Join + aggregate (Q3, Q5, Q10) — offload wins on the CPU-heavy join, par otherwise

median ms (offload speedup = native/offload), N=5 warm, shared cgroup cap:

| W | Q3 native | Q3 offload | Q3 | Q5 native | Q5 offload | Q5 | Q10 native | Q10 offload | Q10 |
|--:|----------:|-----------:|---:|----------:|-----------:|---:|-----------:|------------:|----:|
| 2 | 7559 | 4704 | **1.61×** | 3289 | 3191 | 1.03× | 7829 | 8513 | 0.92× |
| 4 | 4763 | 2650 | **1.80×** | 1769 | 1955 | 0.90× | 6488 | 6980 | 0.93× |
| 8 | 2894 | 1476 | **1.96×** | 1231 | 1132 | 1.09× | 5986 | 6047 | 0.99× |
| 16 | 1857 | 890 | **2.09×** | 745 | 722 | 1.03× | 5286 | 5543 | 0.95× |

- **Q3** (customer⋈orders⋈lineitem + sum GROUP BY, ~76M rows joined): offload
  beats native **1.6–2.1×**, the margin growing with W, at roughly equal cores —
  the CH-side join+aggregate over zero-copy columns scales better than PG's
  parallel hash-join+aggregate.
- **Q5** (6-way join + sum GROUP BY n_name): **~parity** (0.90–1.09×). The join is
  producer-scan-bound; offload and native track each other.
- **Q10** (4-way join, 381105 output groups): **~parity, slightly behind**
  (0.92–0.99×). Low parallelism on both sides (native ≤3.4 cores even at W=16) —
  it is dominated by the large grouped result (381k rows) which the offload must
  stream back from ClickHouse, not by the join compute.

Takeaway: the offload's advantage is **CPU-heavy aggregation** (Q1 single-table
3.2–4.3×; Q3 join+agg up to 2.1×). For producer-scan-bound joins (Q5) or
large-result joins (Q10) it is competitive at parity; for light/selective
reductions (Q4, Q6) tuned native wins. No query where offload is engaged returns a
wrong answer (within the documented fidelity deviations).

### Where native wins (Q4, Q6)

Light/selective reductions (SEMI-join+count, a highly selective filtered sum) —
the offload pays full-table SHM streaming for a cheap reduction, so tuned native
PG wins (0.34–0.74×).

## Residual blockers (root-caused, not yet fixed)

- **Multi-ring join+aggregate deadlock** (Q8, Q9, Q11, Q14): CH freezes with
  `read_rows` stuck at one block on the join build side → `SHM_PRODUCER_STALL`.
  Independent of the READONLY fix / `max_threads` / `final` / `group_by_use_nulls`
  (all ruled out). Needs deeper ClickHouse pipeline/`PollableShmSource`
  scheduling work. (D0006)
- **Rescan of a SHM-offload scan** (Q2, Q16, Q17, Q20): the CustomScan lands on
  the inner side of a correlated/nested-loop rescan — "not supported in phase 1".
- **Correlated subquery parameter `{p1}`** (Q15, Q22): the offloaded fragment
  references an unbound outer parameter.
- **Anti-joins** (Q16, Q21, Q22): `foreign_join_ok` (`fdw.c:1865`) rejects
  `JOIN_ANTI` (`NOT EXISTS`/`NOT IN`); ClickHouse supports `LEFT ANTI JOIN`.

## Reproduce

```
PGDB=tpch_sf10 dev/tpch/eligibility-scan.sh            # coverage + fidelity scan
QUERIES="1 3 4 5 6 10 12 19" W_LIST="2 4 8 16" N=5 \
  dev/tpch/wsweep.sh                                   # native-vs-offload W-sweep
CH_BIN=… PG_DB=shmdemo test/shm/verify_offload.sh      # regression (137/0)
```
