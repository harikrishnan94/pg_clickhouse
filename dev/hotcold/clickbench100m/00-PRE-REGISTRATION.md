# Hot/Cold ClickBench-100M — PRE-REGISTRATION

Written FIRST, before acceptance evidence (§9.3). Amendments are append-only at the bottom.
Conventions mirror `dev/wsweep-report/00-PRE-REGISTRATION.md` and `dev/hotcold/phase3/00-PRE-REGISTRATION.md`.

Author: unattended run, branch `streamed-table-shm-offload`. Date: 2026-06-27.

---

## 0. Environment of record (verified 2026-06-27; re-confirm in 10-REPRODUCTION.md)

- Host: AWS Graviton aarch64, 32 cores, 61 GiB RAM (~41 GiB available), single VPC NIC (loopback-only PG↔CH).
- Disk `/`: 290 GiB total, ~66 GiB free at start. `/dev/shm`: 31 GiB.
- PostgreSQL 18 on `:5432`, peer auth via `sudo -u postgres psql`. Extension `pg_clickhouse` installs into schema `pg`.
- ClickHouse 26.6.1.1, RUN_ID=`tpchcb`, HTTP `:21002` / TCP `:21003`, **live** server PID resolved from `ss -ltnp`
  (manifest CH_PID is stale — D0013). Binary `/home/ubuntu/ClickHouse/build/reldeb/programs/clickhouse`.
- FDW server `ch_bench` (in PG db `clickbench`) → `127.0.0.1:21002 dbname=clickbench`.
- Cold store: `clickbench.hits_idx` (built from `clickbench.hits_100m` = 99,997,497 rows; see 01_build_hits_idx.sh).

---

## 1. The merge architecture (proven by spike — see METHODOLOGY-LOG L0033)

The PG planner does **not** fuse `(foreign cold) UNION ALL (heap hot)` into one CH query (it yields a PG-side
Append+Aggregate with two independent arms — no `streamed_table`, no overlap). **Refuted as a dead end.**

The POC merge runs the merged query **directly in ClickHouse**, where:
- cold arm = a CH-resident table (`clickbench.hits_idx WHERE hc_row_id > N(f)`), scanned by CH from its own MergeTree;
- hot arm = `streamed_table('pgch_hot_<f>', '<schema>')`, fed by a **standalone PG producer** launched via the
  SQL function `clickhouse_stream_relation(rel regclass, shm_name text, rows_per_block int)` (src/shm_offload.c:1051):
  it creates the SHM ring + control socket, streams all snapshot-visible rows of the hot heap, and **waits for the
  CH consumer to drain** before returning.

**Merge template** (the aggregation runs ONCE over the unioned raw rows, so it is correct for every aggregate —
COUNT(DISTINCT), AVG, etc. — no partial-aggregate decomposition):

```
SELECT <query agg / projection>
FROM (
    SELECT <referenced cols> FROM streamed_table('pgch_hot_<f>', '<full hot schema>')   -- hot, streamed from PG
    UNION ALL
    SELECT <referenced cols> FROM clickbench.hits_idx WHERE hc_row_id > N(f)            -- cold, CH-resident
) AS hits
[WHERE <query predicate>] [GROUP BY ...] [ORDER BY ...] [LIMIT ...]
```

The per-query CH body (agg/predicate/group/order/limit + type-mapping gotchas) is taken from the
extension's own deparser output (captured from a full-offload run), so the merge is deparser-faithful;
only the FROM source is swapped to the hot∪cold subquery.

Orchestration: a background `psql` runs `clickhouse_stream_relation(...)`; the harness polls for the control
socket, then issues the merged query to CH (HTTP, `?allow_experimental_streamed_table_function=1`), times it,
and joins the producer. Single CH execution → hot transfer overlaps the cold scan.

---

## 2. The hot/cold split (D-HC-0403/0404)

- `hc_row_id` (UInt64, 1..99,997,497) is a deterministic EXACT recency rank persisted in `hits_idx`
  (smallest = most recent). Fractions: N(1%)=1,000,000, N(5%)=5,000,000, N(10%)=10,000,000.
- hot(f) = `hc_row_id <= N(f)` (lives as a heap table in PG, exported from `hits_idx`).
- cold(f) = `hc_row_id > N(f)` (filtered subquery over `hits_idx`, no per-f materialization).
- Disjoint and total **by construction** (a bijective rank partitioned at N).

---

## 3. Baseline feasibility (D-HC-0401, HIGH-IMPACT)

`native-PG-100M` and `full-offload-100M` are **infeasible to measure** on this host: a PG heap for 100M
ClickBench rows is ≈75 GiB > 66 GiB free (§13 acknowledges this). Therefore:

| Baseline | At 100M | Treatment |
| --- | --- | --- |
| pure-CH-100M | MEASURED | `SELECT <agg> FROM hits_idx` — the no-streaming upper bound AND the 100M correctness ground truth |
| hot/cold-merge-100M | MEASURED | the merge template above, per (f, W) |
| native-PG | NOT measurable @100M | MEASURED @10M (PG `hits`), + clearly-labeled linear PROJECTION to 100M (never as a measured 100M number) |
| full-offload | NOT measurable @100M | MEASURED @10M, + labeled PROJECTION; full-offload streams all rows with no cold overlap |

CH-query-template fidelity to native-PG semantics is established at **10M** (the existing 42/43 ClickBench match,
`dev/clickbench/FULL-OFFLOAD-RESULTS.md`), which is the bridge that lets pure-CH-100M stand as the 100M truth.

---

## UNIT 0 — POC harness + correctness oracle (profile BUILD: expected behavior + acceptance oracle)

**Expected behavior.** For every eligible query and fraction f, the merged hot/cold result equals the
pure-CH-100M result (= the 100M ground truth) within fidelity bounds F1 (Decimal display-scale), F3
(avg→Float64 ≤~1e-15 rel), F5 (bpchar trailing blanks), and top-N deterministic-tiebreak reshuffle. The hot
arm offloads (CH `query_log` shows `streamed_table` with `ShmAdoptedBlocks≥1`); the producer streams exactly
N(f) rows. Teardown leaves no `/dev/shm/pgch_*`, control sockets, or stray PG workers.

**Acceptance oracle (pass/fail), run per (eligible query × f):**
1. **Partition exactness** (per f, once): `count(hot)=N(f)` AND `count(cold)=99,997,497−N(f)` AND
   `count(hot)+count(cold)=count(hits_idx)=99,997,497` AND arms disjoint (hot: hc_row_id≤N, cold: hc_row_id>N).
   PASS iff all hold exactly.
2. **Result equivalence**: `cmp_results.py merge.out pureCH.out 1e-6` returns class ∈ {exact, float, approx}
   (float must have max-rel-err ≤ tol). class ∈ {float!, DIFF} = FAIL (manual root-cause).
3. **Push-down proof**: CH `query_log` for the merged query's `log_comment` tag shows a `QueryFinish` whose
   query text contains `streamed_table`, with `ProfileEvents['ShmAdoptedBlocks'] ≥ 1`; AND the standalone
   producer's returned row count = N(f). PASS iff both hold.
4. **Cleanup**: after the run, `/dev/shm/pgch_*` count, control-socket count, and `pg_stat_activity`
   shm-stream backends all return to baseline. PASS iff no residual.

**Eligible-query selection rule.** Start from the 42 ClickBench queries proven to fully offload at 10M (exclude
Q1 = bare `COUNT(*)`, which references no column to stream — D0010). For the hot/cold merge a query is eligible
iff its CH body can be wrapped over the hot∪cold subquery with the LIMIT/ORDER BY applied at the top (outside
the union). Document and root-cause every exclusion (e.g. if `SELECT *` Q24's 105-col stream is impractical, or
a query's shape resists the wrap). Pass/fail of eligibility is decided by the same 4 oracles above.

**Iteration:** review-driven (BUILD). Unit is green only when all 4 oracles pass across the eligible set and
all fractions, with clean teardown and a passing adversarial review.

---

## UNIT 1 — benchmark + overlap mechanism (profile OPTIMIZATION: expected MECHANISM + predicted MAGNITUDE)

**Expected mechanism.** In the merged CH execution the cold MergeTree scan+aggregate is the long pole; the hot
transfer (N(f) rows from PG over SHM) proceeds concurrently and is *hidden* under the cold processing whenever
cold-time > hot-transfer-time. On loopback the SHM wire is nearly free, so this proves the COMPUTE/overlap thesis
but **NOT** the network thesis (deferred to Unit 2).

**Predicted magnitude (to be confirmed/refuted per (f,W) with median-of-N≥5 under the cgroup cap):**
- merge ≈ pure-CH-100M **within the noise band** for heavy queries where cold-scan ≫ hot-transfer (the overlap
  is full). For light queries and large f (f=10%), the single-producer hot transfer may exceed the short cold
  scan → merge measurably **slower** than pure-CH (overlap incomplete). Reported per query, losses included.
- merge ≫ native-PG (projected from 10M): the known vectorized-CH factor (geomean ≈2–3× at 10M full-offload)
  applies a fortiori at 100M since native PG scans 100M rows row-at-a-time.
- merge < full-offload (projected): merge streams only N(f) ≤ 10M rows vs full-offload's 100M; the saved
  transfer is (100M − N(f)) rows.

**Overlap shown by ≥2 independent instruments (pre-registered):**
- (i) producer-side: `clickhouse_stream_relation` wall time (hot transfer duration) vs the merged-query wall time
  — overlap iff merged-wall ≈ max(cold-only-wall, hot-wall) ≪ (cold-only-wall + hot-wall); cross-checked with the
  phase-split STALL accounting (`shm_log_stream_stats`).
- (ii) CH `query_log` timeline / ProfileEvents for the merged query: the streamed_table read and the MergeTree
  scan progress concurrently (compare merged query_duration_ms against a serial cold-then-hot sum).

**Confirm/refute.** Confirm: merged-wall ≤ cold-only-wall + noise for the heavy-query majority, with the two
instruments agreeing within max(5%, 1 stdev). Refute: merged-wall ≈ cold-wall + hot-wall (no overlap) → root-cause.

**Iteration floor: ≥3 evidence-based cycles.** Disclaimer (mandatory): loopback proves compute, not network.

---

## UNIT 2 — real-wire (NIC) bound + NO RESULT (profile RESEARCH: hypothesis + confirm/refute + NO-RESULT condition)

**Hypothesis.** On a real ~3 GB/s wire with cloud RTT, the hot transfer (N(f) rows, measured bytes) still fits
inside the CH cold-processing window for the heavy-query majority, so the overlap survives the network — but this
cannot be measured cross-box on a single-NIC host.

**Three independent angles (each gathered as a LEAD/bound with raw artifacts):**
- (a) injected-per-frame-latency microbench (`pg_clickhouse.tcp_send_delay_us`, reuse phase3/p2_kbench.sh): show
  K-deep pipelining (`tcp_send_inflight_blocks`) hides a per-frame latency representative of cloud RTT.
- (b) `tc qdisc … netem rate ~3gbit delay <RTT>` shaped loopback run of the Unit-1 merge (note the repo's prior
  netem caveat).
- (c) analytic projection: measured hot-bytes(f) ÷ 3 GB/s vs measured CH cold-time(query) — does hot fit inside cold?

**Confirm/refute for each angle:** support = hot-transfer-time(angle) < cold-time for the heavy majority; refute =
hot-transfer dominates. **NO-RESULT condition (expected):** the true cross-box throughput/overlap headline is
returned as NO RESULT (§9.7); settling source = a 2nd same-VPC/placement-group instance (CH on B, PG on A) re-running
the Unit-1 matrix over the real NIC. BANNED: presenting any loopback/netem/projection figure as the real-NIC result.

**Iteration floor: ≥3 cycles.**

---

## UNIT 3 — cold-IO added-pressure (profile RESEARCH: predicted magnitude + mechanism + confirm/refute + cold-cache verification)

**Predicted magnitude + mechanism.** Streaming adds a *bounded* memory increment over native PG processing the
same cold pages: the columnar staging buffers + the SHM ring/frame pool (capped 128 MiB/stream, NB-4) +
rows_per_block staging — on the order of tens-to-~128 MiB per stream, NOT runaway/unbounded. Native PG already
incurs the heap-scan + work_mem footprint; the streaming arm's *delta* over native is the columnar staging + ring.

**Confirm/refute instruments (≥3):** (1) `/proc/<pid>/status` VmHWM (peak RSS) of the PG producer backend/worker;
(2) cgroup `memory.peak`; (3) wall throughput; with disk-read confirmation via `/proc/<pid>/io` and/or
`EXPLAIN (ANALYZE, BUFFERS)` showing real reads (not cache hits). Confirm = delta(streaming − native) is small and
bounded (≤ a few × 128 MiB) and stable across N≥5; refute = delta grows with data size / is unbounded.

**Cold-cache verification.** Shrink PG `shared_buffers` (128 MiB–1 GiB) + `echo 3 > /proc/sys/vm/drop_caches`
before each run; confirm real disk reads (BUFFERS shows `read=` not `hit=`, `/proc/<pid>/io` rchar/read_bytes > 0).

**Iteration floor: ≥3 cycles.**

---

## Noise band (all quantitative claims)
An effect counts only if relative diff > max(5%, 1 sample stdev of the cell); within that band, state
"within noise" explicitly (§1, §9.4). G1–G4 convergence gates reuse the `dev/wsweep-report` definitions.

## Amendments (append-only)
- (none yet)
