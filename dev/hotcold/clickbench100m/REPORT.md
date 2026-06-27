# Hot/Cold ClickBench-100M offload — REPORT (for the pgClickHouse team meeting)

Audience: David, Philip, Josh. Bar: measurable improvement, no correctness issues, honest about
what is not yet proven. Branch `streamed-table-shm-offload`; artifacts in `dev/hotcold/clickbench100m/`.

**One-paragraph bottom line.** A hot/cold streaming split — keep the most-recent f∈{1,5,10%} of
ClickBench-100M in Postgres, stream it on demand into ClickHouse, and merge it with the cold bulk
(already in CH) in a single CH query — is **correct** (proven equal to the 100M ground truth) and
delivers a **real, mechanism-explained speedup over the honest no-CH alternatives**: at f=1% the
merged query is **15.6× faster than native-PG-100M** (projected) and **5.4× faster than streaming all
100M (full-offload)**, all 42/42 eligible queries. It is **slower than pure-CH-100M** (the
ETL-everything baseline) by 4–24× — the price of keeping the hot data fresh in PG instead of ETLing
it. The headline win shrinks as f grows (at f=10% the *single-threaded* hot producer streaming 10M
rows erases the advantage over full-offload). On loopback the hot transfer is **CPU-bound** and only
*hides* under the cold scan for the slowest queries — the **network-overlap thesis is NOT proven here
and is deferred to Unit 2 (NO RESULT)**. Cold-IO pressure: Unit 3.

---

## Unit 0 — POC harness + correctness — **GREEN**

- **Architecture** (D-HC-0402): the PG planner does *not* fuse `(foreign cold) UNION ALL (heap hot)`
  into one CH query (proven by spike). The POC issues the merged query **directly to ClickHouse**,
  with the hot rows fed by a standalone background producer (`clickhouse_stream_relation`) streaming a
  PG heap into a SHM ring; the merged CH query is `streamed_table(hot) UNION ALL (hits_100m WHERE
  cold)` wrapped by each query's aggregation. No planner/extension routing, no C changes.
- **Exact split** (D-HC-0403): the recency 5-tuple `(EventTime,WatchID,UserID,CounterID,EventDate)`
  is *unique* over the 99,997,497 rows, so hot(f)=`tuple≥B(f)`, cold(f)=`tuple<B(f)` is exact and
  disjoint — verified: hot=1,000,000/5,000,000/10,000,000, hot+cold=99,997,497 for all f.
- **Correctness oracle** (05_oracle.sh): for 42 eligible queries × 3 fractions, merged == pure-CH-100M
  (the 100M ground truth; native-PG-100M is infeasible to materialize — PG heap ≈70 GiB > free — so
  pure-CH-100M is the truth, with CH-template fidelity to native-PG proven at 10M). Result: **112/126
  exact + 14 DIFF, all top-N tie-boundary reshuffle proven benign** (18/18 collapse to exact under a
  deterministic total order). Partition exact; push-down proven (ShmAdoptedBlocks 49/239/478);
  producer streams exactly N(f); **zero SHM/socket/bgworker leaks**.
- **Adversarial review** (evidence/ADVERSARIAL-REVIEW.md, 5 agents + synthesis) found one BLOCKING
  correctness bug — a server-TZ (Asia/Kolkata) timestamp relabel that put the cold/pure arm −5:30 off
  the hot arm, masked in the eligible set by row placement. **Fixed** (`toDateTime64(toString(col),
  'UTC')`) and re-verified 3 ways (row value, a 10M native-PG bridge, a hot-reaching minute histogram).

## Unit 1 — benchmark + overlap mechanism — **(in progress)**

**Measured (W=8, N=5, shared cgroup cap; overlap via CH `query_duration_ms`):**

| comparison | f=1% (1M hot) | f=5% (5M) | f=10% (10M) |
| --- | --- | --- | --- |
| merge wall-100M (measured, geomean) | 763 ms | 2387 ms | 4415 ms |
| **vs native-PG-100M** (×10 projection) | **15.6× faster** (42/42) | 5.0× (42/42) | 2.7× (42/42) |
| **vs full-offload-100M** (×10 projection) | **5.4× faster** (42/42) | 1.72× (40/42) | 0.93× (11/42) |
| vs pure-CH-100M (measured) | 4.2× slower | 13.2× slower | 24.4× slower |

- **The win** is vs the honest no-CH alternatives: hot/cold-merge runs the analytic query at ClickHouse
  speed over the cold bulk while streaming only the recent f% from PG — far faster than native-PG over
  100M (CH vectorization) and than streaming all 100M (transfer saved). Strongest at small f.
- **The cost** is vs pure-CH-100M (everything pre-ETLed to CH): merge is 4–24× slower because (a) CH
  scans 100M extremely fast (cold_ch median **308 ms**; 15/42 queries <100 ms) and (b) the
  **single-threaded** hot producer streams ~2.6M rows/s. So the hot stream is the long pole for the
  many fast queries; it only *hides* under the cold scan for the slow-cold tail (q17/18/19/29/33/34/35,
  cold≥~1 s) at f=1% (overhead 5–19%). This is the price of fresh-hot-in-PG vs stale ETL.
- **Mechanism** (≥2 independent instruments): `EXPLAIN PIPELINE` shows the hot (`PollableShmSource`) and
  cold (`MergeTreeSelect×8`) arms feed one `Union→AggregatingTransform×16` *concurrently*, and the
  producer finishes mid-merge-window — so the arms *do* overlap temporally; but under the shared CPU
  cap the hot transfer (CPU-bound: PG scan+columnize) competes with the cold scan, so it is hidden only
  in the cold long-pole's slack. CH `query_duration_ms` (median+sd, N=5) is the fair merge-execution
  metric (the bash wall additionally carries a POC producer-launch overhead a persistent producer
  would not pay).
- **Single-threaded producer** is the dominant limiter at large f (D-HC-0405). **Parallelizing it (P=8 hot
  producers → P rings, iter-2b) recovers the large-f win**: at f=10% the merge is 2.64× faster than single-hot
  (geomean over 8 representative queries; up to 6× for fast queries), lifting merge to **9.3× vs native-PG-100M**
  (was 2.7×) and **2.45× vs full-offload-100M** (was 0.93× — the win over full-offload is RESTORED). At f=1%
  parallel-hot gives no benefit (the hot stream is already small). [Subset of 8 queries; the clean TIER-1
  single-hot is the baseline — the in-session single-hot was cache-inflated, root-caused.]


- **W-dependence** (W∈{1,2,4,8}, iter-3): cold_ch scales ~linearly with the cap (q29 70s→8.6s, W1→W8) while the
  single-threaded hot_ch is ~constant; W=1 is a CPU-starvation point (producer+consumer share one core → merge >
  cold+hot), not evidence against overlap (review C3). At high W the fixed hot stream is relatively more dominant
  (cold faster) — reinforcing the parallel-hot lever.

**DISCLAIMER (the §7 tension, binding).** These are **loopback** results: they prove the **compute
thesis** (vectorized CH over streamed PG rows + the cold bulk beats native PG). They do **NOT** prove
the **network-overlap thesis** (hot transfer hidden under cold processing on a real ~3 GB/s wire),
because on loopback the hot transfer is CPU-bound and competes with the cold scan for the same CPU.
The network thesis is Unit 2, which returns **NO RESULT** for the true cross-box headline.

## Unit 2 — real-wire (NIC) bound — *(pending; expected NO RESULT + bounds)*
## Unit 3 — cold-IO added-pressure — *(pending)*

---
*(Numbers above are committed in `results/bench/{cells.tsv,baselines10m.tsv}`; reproduction in
`10-REPRODUCTION.md`; every claim's sources in the evidence matrix, methodology log L0031–L0042.)*
