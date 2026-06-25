# W-sweep + producer-phase-split — PRE-REGISTRATION

Written BEFORE running any sweep, so post-hoc rationalization is impossible. Any deviation
from this document during execution is recorded as a dated amendment at the bottom, with cause.

Author: performance-engineering pass. Date: 2026-06-25.

## Environment of record (frozen at pre-registration)
- Host: AWS Graviton (aarch64), 32 cores / 61 GiB, dedicated, idle (load avg 0.06 at start).
  clocksource = arch_sys_counter. Confirm `uptime` load < 0.5 before each sweep.
- pg_clickhouse git SHA: 35776e62c53d6ce557c07f2e0ce21cc69d9d10f0 (+ the phase-timer instrumentation
  commit landed in step 2; recorded in the repro block of the final report).
- ClickHouse: git SHA 93381ccf9fba48170df206554afc48c9b48e54ea, version 26.6.1.1, build
  /home/ubuntu/ClickHouse/build/reldeb. HEAD commit IS the hash-join-BUILD deadlock fix.
- CH live: pid resolved from `ss -ltnp` (manifest CH_PID is stale, D0013). HTTP=21002, native=21003,
  config /home/ubuntu/ch-bench/tpchcb/config.xml. FDW `ch_bench`: tpch_sf10->21002 (ok),
  clickbench was ->21000 (DEAD); re-pointed to 21002 via ALTER SERVER (D0013 operational fix, logged).
- PG: v18, port 5432, postmaster.pid /var/lib/postgresql/18/main/postmaster.pid, server log
  /var/log/postgresql/postgresql-18-main.log (stderr; logging_collector off).
- Datasets: TPC-H tpch_sf10 schema pg (lineitem ~60M, exactly 59,986,052 visible rows measured).
  ClickBench clickbench schema pg, table hits = 10M-row subset (hits_0..9).

## Queries (candidate sets; eligibility GATED per query at each W, not assumed)
- TPC-H candidate set (FULL-OFFLOAD-DECISIONS.md): 1 3 4 5 6 7 9 10 12 14 19.
  Also RUN the offload+correctness oracle on 8 and 11; INCLUDE only if they offload AND pass
  correctness at that W. Pre-registered expectation (from D0008): Q8 FAILS correctness
  (Decimal/Decimal division ~5e-4 rel) -> excluded-with-reason; Q11 ERRORS (HAVING-subquery
  param unbound, Code 456) -> excluded-with-reason. Recorded honestly either way.
- ClickBench candidate set: Q2..Q43 (Q1 excluded by design — SELECT COUNT(*) references no column,
  native count(*) wins, D0010). Gate each by the offload oracle at each W.

## W and repetition
- W = {1, 2, 4, 8} (cores). W=1 INCLUDED and reported honestly even if offload loses (single core
  shared by producer(s) + CH consumer). N = 5 measured WARM runs per cell + 1 discarded warmup.
- Shared cap: ONE cgroup v2 (pgch_wsweep / pgch_cbwsweep) holding BOTH the PG postmaster tree AND
  the live CH server, cpu.max = W*100000us/100000us. Measuring shell stays outside. cpu.max read
  back each W; cores ~= W confirmed on a CPU-bound cell.

## Tuning (identical across compared runs; recorded, not changed to flatter)
- Native PG (per W): max_parallel_workers=64, max_parallel_workers_per_gather=W, per-table
  parallel_workers=W, work_mem=2GB, hash_mem_multiplier=4, jit=on, jit_above_cost=0,
  parallel_setup_cost=0, parallel_tuple_cost=0.01, min_parallel_table_scan_size=0.
- Offload: enable_shm_offload=on, session_settings final=1 + max_threads (CH consumer) = 16
  (ClickBench MT=16; TPC-H 16), per-table producer parallel_workers = W/2 (harness "two-table
  balance"; >=1). max_parallel_workers_per_gather: TPC-H 16, ClickBench MT=16.
- ASYMMETRY (documented, NOT changed): native gets W workers; offload gets W/2 producer workers
  per base table sharing the W-core cap with the CH consumer. This is the established harness
  config; altering it to favor either side is banned by the brief.
- EXPLAIN ANALYZE spot-check on native cells confirms NO Sort/HashAgg spill (work_mem sufficient).

## Headline metric
- WARM median wall-ms per cell (median of N=5). Report min/max/stdev for every cell.
- speedup = native_median_ms / offload_median_ms  (>1 => offload faster).
- Aggregate per (benchmark, W) over ALL eligible queries incl. losses: count faster/slower,
  geomean, median, min, max speedup. No cherry-picking.

## Producer/consumer split (the new measurement) — units and method
- Producer phases (PG side): READ (heap page read + visibility classify, shm_page_reader.c
  827-841), DEFORM (columnar deform/columnize groups A/B/C, 846-895), PUBLISH (publish_block
  memcpy into the SHM ring slot, shm_producer.c 625-721, EXCLUDING the stall), PUBLISH_STALL
  (the ring-full backpressure pg_usleep loop 613-623 — idle wait on the consumer, reported
  SEPARATELY, kept OUT of PUBLISH).
- Consumer = ClickHouse.
- Primary unit = CPU-cores of work = CPU-seconds / wall-seconds, matching the harness off_prod
  (host-minus-CH /proc/stat) and off_cons (CH /proc/CHPID/stat). off_prod is decomposed into
  read_cores / deform_cores / publish_cores. Also a WITHIN-producer wall-% decomposition
  [read:deform:publish] summing to producer wall, with publish_stall reported separately.
- Producer and consumer run CONCURRENTLY (streaming pipeline): CORES (work) are additive,
  wall-times are NOT. We never add producer_ms + consumer_ms as "end-to-end".

## Two independent instruments (split is INVALID unless they converge)
1. In-code per-phase timers: CLOCK_THREAD_CPUTIME_ID (CPU) + CLOCK_MONOTONIC (wall), stopwatch
   model, aggregated across all W stream workers, surfaced in the shm_log_stream_stats LOG.
   Measured so read+deform+publish(+stall) CPU == the existing aggregate getrusage cpu_ms.
2. perf record/report (ARM PMU `cycles`, or `task-clock` fallback) of the "pg_clickhouse shm
   stream" workers during an offload cell, attributing on-CPU samples to read- / deform- /
   publish-family functions.

## Convergence gates (ALL required; failure => no split result, root-cause first)
- G1: sum(read+deform+publish) CPU  ~=  aggregate producer getrusage cpu_ms (timer self-check).
- G2: timer per-phase CPU fractions  ~=  perf per-phase on-CPU fractions (instrument 1 vs 2).
- G3: off_prod cores (/proc/stat host-minus-CH)  ~=  sum of read+deform+publish cores (in-code).
- G4: off_cons cores (/proc/CHPID/stat)  ~=  CH query_log consumer time (query_duration_ms and/or
   ProfileEvents-derived) expressed as cores.

## Noise threshold (committed now)
- "~=" := relative difference <= max(5%, 1 sample stdev of the cell). Justification: warm
  run-to-run wall stdev on this idle host is typically a few %; the 5% floor absorbs scheduler/
  timer jitter and the ~10% probe overhead present only in the instrumented split pass (which
  affects both sides of G1 equally). If two sources disagree beyond this, the cell has NO split
  result until root-caused. An effect <= noise is reported as "within noise", never as a win.

## Oracles (every offloaded cell; reported per query)
- Offload: plan has Custom Scan (ClickHouseShmScan) AND CH query_log streamed_table QueryFinish
  with ShmAdoptedBlocks>=1, correlated by a unique log_comment tag (poll-retry the async flush).
- Liveness: CH read_rows advances to completion == full relation (TPC-H lineitem 59,986,052;
  ClickBench hits 10,000,000 for full-table fragments) with a QueryFinish (no freeze/Code781/
  timeout). A frozen read_rows is a deadlock, not a slow query. Raising shm_source_stall_timeout
  is NOT a fix and is not used.
- Correctness: offload result == native within documented bounded deviations ONLY (F1 display-
  scale decimal; F3 avg->Float64 <= ~1e-15 rel; F5 bpchar trailing blanks; top-N tie reshuffle
  under deterministic tiebreak). Any other deviation = failed cell; its perf is NOT a "win".

## Pre-registered expectations (so surprises are visible)
- W=1: offload likely LOSES many cells (producer+consumer contend for one core). Reported, not dropped.
- Offload wins concentrate on CPU-heavy aggregation/joins at higher W (Q1/Q9/Q14 TPC-H; high-
  cardinality GROUP BY ClickBench) where CH consumer >> PG single-backend deserialization.
- Likely offload losers (pre-registered): TPC-H low-compute scans; ClickBench Q24 (SELECT * 105
  cols, publish/deform-bound), Q25, and any tiny-output query where native parallel scan already
  saturates W cores. These are reported as losses, included in the aggregate.
- Producer-bound vs consumer-bound: expect READ to dominate producer cores on simple scans
  (Q6), DEFORM to rise with column count/width, PUBLISH small (memcpy ~1% historically),
  PUBLISH_STALL large only when the consumer is the bottleneck (offload-loses cells).

## Deliverables (final report)
1. Per-benchmark per-(Q,W) table with the full split columns + oracle flags.
2. Aggregate speedup distribution per (benchmark, W) over ALL eligible queries.
3. Convergence appendix (G1-G4 side-by-side with noise) for a representative sample.
4. Excluded-queries table with root-caused reason.
5. Exact reproduction block (commands, env, SHAs, GUCs, cgroup setup, port resolution).
6. "What the split shows" — each claim backed by its two converging sources.

## Amendments (append-only)
- (none yet)
