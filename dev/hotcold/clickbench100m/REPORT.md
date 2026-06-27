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

## Unit 1 — benchmark + overlap mechanism — **GREEN**

**Measured (W=8, N=5, shared cgroup cap; overlap via CH `query_duration_ms`):**

| comparison | f=1% (1M hot) | f=5% (5M) | f=10% (10M) |
| --- | --- | --- | --- |
| merge wall-100M (measured, geomean) | 763 ms | 2387 ms | 4415 ms |
| **vs native-PG-100M** (×10 projection) | **15.6× faster** (42/42) | 5.0× (42/42) | 2.7× (42/42) |
| **vs full-offload-100M** (×10 projection) | **5.4× faster** (42/42) | 1.72× (40/42) | 0.93× (11/42) |
| vs pure-CH-100M (measured) | 4.2× slower | 13.2× slower | 24.4× slower |

(The ×10 projection is **conservative**: native-PG-100M would scale super-linearly — high-cardinality GROUP BY spills more at 100M, and a 100M PG heap exceeds RAM → IO-bound — so the real native-PG-100M is *slower* than ×10, i.e. the 15.6× is an under-claim. merge & pure-CH are measured at 100M.)

- **The win** is vs the honest no-CH alternatives: hot/cold-merge runs the analytic query at ClickHouse
  speed over the cold bulk while streaming only the recent f% from PG — far faster than native-PG over
  100M (CH vectorization) and than streaming all 100M (transfer saved). Strongest at small f.
- **The cost** is vs pure-CH-100M (everything pre-ETLed to CH): merge is 4–24× slower (geomean), the price
  of fresh-hot-in-PG vs stale ETL. Two components, BOTH measured: (a) pure-CH is projection-optimized
  (cold_ch median 308 ms; q-with-few-cols answered in tens of ms via the sparse index), whereas the merge's
  cold arm must read the 5 recency-boundary columns over 100M to evaluate `tuple<B(f)` — a real cost
  (e.g. q3 cold-arm 200 ms vs pure-CH 28 ms) that is a *fixable* POC artifact (D-HC-0404: materialize a
  partition column or per-f cold table); and (b) the single-threaded hot producer (~2.6M rows/s; the f=10%
  limiter, recovered by parallel-hot below). vs the no-CH alternatives (native-PG / full-offload) the merge
  WINS — that is the relevant comparison when the hot data is NOT pre-ETLed.
- **Mechanism — overlap IS shown** (cache-controlled, ≥2 independent instruments; the headline metric is CH
  `query_duration_ms`, median+sd N=5 — the bash wall additionally carries a POC producer-launch overhead, but
  merge_wall−merge_ch ≈ 0 since the producer runs concurrently). Measured against the merge's *actual cold arm*
  (NOT pure-CH — that mistake, review C1, inverted an earlier reading): **merge_ch ≈ max(cold-arm, hot)**, not
  the sum — the smaller arm is hidden under the larger. Of 16 cache-controlled cells, **11 HIDDEN** (overlap_ratio
  = (cold-arm+hot)/merge = 1.03–1.49): e.g. p01 q3 cold-arm 200 + hot 384 → merge 402 (cold hidden); p01 q33
  cold-arm 4286 + hot 404 → merge 4364 (hot hidden); p10 q3 cold-arm 196 + hot 4833 → merge 4813 (cold hidden).
  Independent instrument B: the producer active-window ≈ the merge window (to ~1%) in every cell → concurrent,
  not drain-first. The merge degrades to **additive only when BOTH arms are large and CPU-comparable** (5/16,
  e.g. p10 q33 cold-arm 4096 ≈ hot 4563 → merge 8472 ≈ serial) — the loopback shared-CPU contention limit (§7:
  on loopback hot=CPU competes with cold=CPU; a real wire where hot=network would overlap cold-CPU for free).
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
`10-REPRODUCTION.md`; every claim's sources in the evidence matrix, methodology log L0031–L0045.)*
