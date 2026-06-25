# Consolidated W-sweep — TPC-H SF10, after the hash-join-BUILD deadlock fix

Idle dedicated host. N=5 warm runs/cell. Both the PostgreSQL postmaster tree AND the
ClickHouse server in ONE cgroup v2 capped to W cores (cpu.max = W*period), so native and
offload compete for the SAME core budget. Median wall-ms; `speedup = native_med /
offload_med` (>1 = offload faster). Sources: `wsweep-clean/` (Q1,5,10,19) and
`wsweep-unlocked/` (Q8,9,14). Q11 is excluded — it does not complete under offload
(separate HAVING-subquery param bug, D0008).

| Q | shape / role | W | native_med | offload_med | speedup |
|---|---|--:|--:|--:|--:|
| **Q14** | 2-way join — UNLOCKED by the fix | 8 | 9109 | 1338 | **6.81×** |
| **Q14** | | 16 | 5140 | 807 | **6.37×** |
| **Q9**  | 6-way join — UNLOCKED by the fix | 8 | 9452 | 2881 | **3.28×** |
| **Q9**  | | 16 | 8072 | 1700 | **4.75×** |
| **Q8**  | 8-way join — UNLOCKED by the fix | 8 | 752 | 2486 | 0.30× |
| **Q8**  | (W16 cell FAILED — see note) | 16 | 500 | 74✗ | — |
| Q1  | single-table agg — control (fix never runs) | 8 | 4742 | 1651 | 2.87× |
| Q1  | | 16 | 2504 | 956 | 2.62× |
| Q5  | 6-way join (working pre-fix) | 8 | 1342 | 1534 | 0.87× |
| Q5  | | 16 | 852 | 958 | 0.89× |
| Q10 | join, 381k-row output (working pre-fix) | 8 | 6049 | 6253 | 0.97× |
| Q10 | | 16 | 5302 | 5613 | 0.94× |
| Q19 | lineitem×part, very selective (working pre-fix) | 8 | 231 | 2256 | 0.10× |
| Q19 | | 16 | 169 | 1292 | 0.13× |

## Headline
The deadlock fix UNLOCKS real offload wins: **Q14 6.4–6.8×, Q9 3.3–4.8×** vs tuned native
(16-worker, JIT) at SF10. Q8 also offloads (eligibility: blocks 1237, read_rows
78,586,107) — see the note about its W16 cell.

## No-regression (vs pre-fix baselines)
- **Q1 control** (single-table; `materializeColumnsFromRightBlock` is never called) moved
  ~+28% offload vs the phase-1 baseline — purely environmental host drift, since the fix
  cannot touch it.
- **Q10** (fix-affected join) offload W16 5543→5613ms = +1.3% vs phase-3 baseline (≤ noise).
- **Q5** +33% ≈ the Q1 control's environmental drift.
- perf: `materializeColumnsFromRightBlock` = 0.04% of CPU during a Q10 join offload
  (join-build `RowRefList::Batch::insert` dominates at ~14%) — the build-side copy is not
  a new hotspot.
Conclusion: the fix's perf effect is ≤ the environmental noise the fix-independent control
exhibits; the directly-affected joins are unchanged.

## Notes (NOT caused by the fix)
- **Q8 W16 `74ms` is a FAILED offload, not a 6.77× win** (`off_cons=0.00` → CH did no
  work). The sweep sets per-table `parallel_workers = W/2 = 8`; Q8 streams 7 tables, so it
  requests >`max_worker_processes` (32) producers and hits "could not register SHM
  streaming background worker". Pre-existing infra limit (reviewer-flagged), independent of
  the fix; Q8 offloads correctly in isolation (eligibility above). At W8 (per=4) it
  completes: 2486ms / 0.30×.
- **Offload < native (speedup <1×) for Q5/Q8/Q10/Q19** is pre-existing: native PG uses
  selective filters/indexes (Q19/Q8 read far fewer rows) while offload streams the full
  fact tables. The deadlock fix does not change this; offload's clear win is on heavy
  full-scan aggregates/joins (Q1 2.6–2.9×, Q14 6.4–6.8×, Q9 3.3–4.8×).
