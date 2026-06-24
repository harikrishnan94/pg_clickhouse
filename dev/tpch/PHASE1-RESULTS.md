# Phase 1 results — single-table Decimal/numeric aggregate offload (Q1, Q6)

**Date:** 2026-06-24 · **Branch:** streamed-table-shm-offload · code commit 52d52e4
**DB:** tpch_sf10 (SF10) · **CH:** patched streamed_table, RUN_ID=tpchcb (HTTP 21002)

## Scope & pre-registration

**Change.** `shm_create_upper_paths` (`src/shm_customscan.c`) previously declined
the grouped-aggregate CustomScan whenever any output column was `numeric`. That
decline is now gated to **join inputs only**; a single-table aggregate over a
heap-offload base relation pushes its `sum`/`avg`/`min`/`max` numeric output to
ClickHouse. Unlocks Q1 and Q6.

**Pre-registered mechanism + magnitude.**
- Mechanism: with the decline lifted, `shm_create_upper_paths` adds the aggregate
  CustomScan; the deparser emits `sum(...)`, `avg(...)` over `streamed_table()`;
  CH computes them and returns text parsed by `numeric_in`.
- Fidelity prediction: `sum/min/max` exact (CH Decimal, modulo display scale);
  `avg(Decimal)`→Float64 deviation ~1e-15 relative.
- Performance prediction (unregistered direction, measured): a heavy GROUP BY
  aggregate (Q1) could win under offload because CH aggregates vectorised over
  zero-copy columns; a light/selective single-table sum (Q6) likely loses because
  the offload still streams the whole table. Measured both.

## Correctness (oracle-proven)

| Q | PG plan (offload on) | CH dispatched fragment | read_rows | ShmAdoptedBlocks |
|---|----------------------|------------------------|-----------|------------------|
| 1 | `Sort → Custom Scan (ClickHouseShmScan)` (no residual Aggregate) | `sum()×4, avg()×3, count(*) … GROUP BY l_returnflag,l_linestatus` | 59 986 052 | 927 |
| 6 | `Custom Scan (ClickHouseShmScan)` (no residual Aggregate) | `sum(l_extendedprice*l_discount) … WHERE …` | 59 986 052 | 926 |

Both satisfy the oracle: a new `streamed_table()` `QueryFinish`, `ShmAdoptedBlocks
≥ 1`, the **aggregate** (not a base scan) in the dispatched SQL, and no residual
aggregate in the PG plan. Q1 previously offloaded **nothing** (a parallel seq scan
won on cost); Q6 previously offloaded only the scan.

Regression suite `test/shm/verify_offload.sh`: **137 PASS / 0 FAIL** (was 131/6;
the 6 numeric-aggregate cases now pass by value-equivalence, deviation printed).

## Fidelity (measured, SF10, `dev/tpch/evidence/phase1/`)

| Q / columns | engine difference | max abs err | max rel err | class |
|-------------|-------------------|-------------|-------------|-------|
| Q1 sum_qty / sum_base_price / sum_disc_price / sum_charge | CH Decimal sum | 0 | 0 | exact (display-scale only) |
| Q1 count_order | count → Int | 0 | 0 | exact |
| Q1 avg_qty / avg_price / avg_disc | CH avg(Decimal) → Float64 | 6e-12 (avg_price) | **1.57e-16** | bounded Decimal→Float64 (≤ machine epsilon) |
| Q6 revenue | CH Decimal sum | 0 | 0 | exact (`1230113636.0101`) |

Only deviation is `avg`→Float64 at ≤ machine epsilon (~2.2e-16), the explicitly
allowed, documented deviation (`FULL-OFFLOAD-DECISIONS.md` D0003/F3). Quantified
independently with a 60-digit-precision `Decimal` re-derivation.

## Performance — W-sweep under shared cgroup cpu.max cap

Native (offload off, tuned: parallel_workers=W, work_mem=2GB, JIT on) vs offload,
both process trees in one cgroup capped to `cpu.max = W·100ms`. Median of N=5 warm
runs (min/max + stdev); cores = CPU-seconds/wall from whole-host `/proc/stat` split
ClickHouse-server vs PostgreSQL (host−CH). Harness: `dev/tpch/wsweep.sh`. Raw:
`dev/tpch/evidence/phase1/wsweep/RESULTS.md`. **Reproduced across 3 independent
runs** (wall times stable to <1%).

### Q1 — GROUP BY aggregate (sum×4, avg×3, count over 60M) — OFFLOAD WINS 3.2–4.3×

| W | native ms (±sd) | nat cores | offload ms (±sd) | off cores (prod+cons) | speedup |
|--:|-----------------|-----------|------------------|-----------------------|--------:|
| 2 | 17383 (±10) | 2.05 | 4024 (±17) | 0.98+0.97 = 1.95 | **4.32×** |
| 4 | 8829 (±7) | 4.04 | 2219 (±9) | 2.05+0.91 = 2.96 | **3.98×** |
| 8 | 4541 (±6) | 8.32 | 1290 (±4) | 4.05+1.58 = 5.63 | **3.52×** |
| 16 | 2386 (±2) | 15.86 | 745 (±2) | 7.76+2.93 = 10.69 | **3.20×** |

Offload is faster at every W **and uses fewer cores** (W=16: 745ms@10.7c vs
2386ms@15.9c). Mechanism: CH aggregates vectorised over zero-copy adopted Decimal
columns; native PG evaluates 7 numeric aggregates per row over 60M rows even with
JIT — far more CPU. The core measurement (offload draws <W cores while native
saturates the cap) is an independent confirmation of the wall-time win.

### Q4 — orders SEMI lineitem + count(*) GROUP BY (already offloaded) — native wins

| W | native ms | offload ms | speedup |
|--:|-----------|------------|--------:|
| 2 | 1674 | 3002 | 0.56× |
| 4 | 1036 | 1690 | 0.61× |
| 8 | 592 | 963 | 0.61× |
| 16 | 423 | 573 | 0.74× |

### Q6 — selective filtered sum (single value) — native wins, converging at high W

| W | native ms | offload ms | speedup |
|--:|-----------|------------|--------:|
| 2 | 1144 | 3328 | 0.34× |
| 4 | 745 | 1777 | 0.42× |
| 8 | 536 | 1063 | 0.50× |
| 16 | 561 | 624 | 0.90× |

For Q4/Q6 the offload must stream 75M/60M rows into ClickHouse, and the heavy op
(semi-join+count for Q4, a highly selective filtered sum for Q6) is cheap relative
to that streaming. Native PG, with no streaming cost, wins. The offload's
advantage is in **CPU-heavy aggregation** (Q1), not in moving large volumes for a
cheap reduction.

## Evidence triangulation (per the standard)

- **End-to-end wall time** — median N=5, stdev shown, 3 runs agree to <1%.
- **Core attribution** — CPU-seconds/wall confirms the cap (nat cores ≈ W) and
  that offload Q1 uses fewer cores than native (mechanism, not just outcome).
- **ClickHouse query_log oracle** — read_rows = full table, ShmAdoptedBlocks ≥ 925
  prove the work ran in CH over SHM.
- **PG plan** — no residual aggregate confirms the heavy fragment is pushed.

These converge: Q1 offload is faster (wall) for a clear reason (fewer cores / CH
vectorised agg), proven from CH's own logs. No claim rests on wall time alone.

## Verdict

Phase 1 green: Q1, Q6 fully offload, correct within bounded+documented fidelity;
regression suite green; Q1 demonstrates the offload's core-efficiency advantage on
heavy aggregates. Coverage: **correct fully-offloaded queries now 3/22** (Q1, Q4,
Q6), up from 1 (Q4). Next: Phase 2 fixes the ClickHouse join-squashing bug to
unlock the join queries.
