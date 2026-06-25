# Exact reproduction block

## Source versions
- pg_clickhouse: branch `streamed-table-shm-offload`, base SHA `35776e62c53d6ce557c07f2e0ce21cc69d9d10f0`
  + the producer phase-timer instrumentation landed for this report (files: `src/include/shm_phase.h`
  new; `src/include/shm_visibility.h`, `src/include/shm_producer.h`, `src/shm_producer.c`,
  `src/shm_page_reader.c`, `src/shm_worker.c` edited). The instrumentation is BENCHMARK-GATED on the
  `pg_clickhouse.shm_log_stream_stats` GUC: when off (every headline timing run) it is an early-return,
  so headline wall times carry zero probe cost.
- ClickHouse: SHA `93381ccf9fba48170df206554afc48c9b48e54ea`, version `26.6.1.1`, build
  `/home/ubuntu/ClickHouse/build/reldeb/programs/clickhouse` (reldeb = RelWithDebInfo). This HEAD commit
  is the streamed_table() hash-join-BUILD deadlock fix (`materializeColumnsFromRightBlock` ->
  `convertToFullColumnIfAdopted`), which is why TPC-H Q9/Q14 offload+complete here.

## Host / environment of record
- AWS Graviton (aarch64), 32 cores / 61 GiB, dedicated. Idle confirmed (load avg) before each sweep.
  clocksource = arch_sys_counter (vDSO CLOCK_MONOTONIC ~31 ns; CLOCK_THREAD_CPUTIME_ID ~228 ns).
- cgroup v2 (`/sys/fs/cgroup`, controllers include cpu, cpuset).

## ClickHouse server / FDW resolution (D0013-safe)
- Live CH pid + HTTP port resolved from the listening socket, NOT the manifest (manifest CH_PID is stale):
  `sudo ss -ltnp | grep ":<CH_HTTP_PORT> "` -> pid. CH_HTTP_PORT=21002, CH_TCP_PORT=21003.
- FDW server `ch_bench` port re-pointed before the ClickBench sweep (it was stale at 21000 after a CH
  restart): `psql -d clickbench -c "ALTER SERVER ch_bench OPTIONS (SET port '21002');"`
  (tpch_sf10's ch_bench already pointed at 21002). This is an operational re-point per D0013, not tuning.
- Manifest: `/home/ubuntu/ch-bench/tpchcb/manifest.env` (CH_HOST=127.0.0.1, CH_HTTP_PORT=21002). RUN_ID=tpchcb.

## Datasets (liveness baselines)
- TPC-H: DB `tpch_sf10`, schema `pg`, SF10. lineitem = 59,986,052 rows (full-read liveness target).
- ClickBench: DB `clickbench`, schema `pg`, table `hits` = 10,000,000 rows (subset hits_0..9).

## PostgreSQL GUCs (server-wide, unchanged across all runs)
- server_version=18.4 ; shared_buffers=16GB ; effective_cache_size=48GB ; io_method=worker (PG18 AIO
  via io workers -- relevant to the G3 residual) ; huge_pages=try ; max_worker_processes=32 ;
  max_parallel_workers=32 ; max_wal_size=8GB ; checkpoint_timeout=300s.

## Per-run tuning (set by the harness, identical across compared cells; recorded, not changed to flatter)
NATIVE (per W):
  SET max_parallel_workers=64; SET max_parallel_workers_per_gather=W;
  SET parallel_leader_participation=on; SET min_parallel_table_scan_size=0;
  SET parallel_setup_cost=0; SET parallel_tuple_cost=0.01;
  SET work_mem='2GB'; SET hash_mem_multiplier=4; SET jit=on; SET jit_above_cost=0;
  ALTER TABLE <each base table> SET (parallel_workers=W);
OFFLOAD:
  SET pg_clickhouse.enable_shm_offload=on; SET pg_clickhouse.local_ch_server='ch_bench';
  SET pg_clickhouse.shm_min_rows=0;
  SET pg_clickhouse.session_settings='join_use_nulls 1, group_by_use_nulls 1, final 1,
      allow_experimental_streamed_table_function 1[, max_threads 16 (ClickBench)], log_comment <tag>';
  SET max_parallel_workers_per_gather=16 (TPC-H) / =MT=16 (ClickBench);
  ALTER TABLE <each base table> SET (parallel_workers = W/2 (TPC-H) / W (ClickBench));
  (instrumented split pass additionally: SET pg_clickhouse.shm_log_stream_stats=on)
- ClickHouse server: v26.6.1.1; session max_threads=16; max_block_size=65409;
  count_distinct_implementation=uniqExact (count(DISTINCT) is EXACT, not HLL); use_uncompressed_cache=0.
- DOCUMENTED ASYMMETRY (not changed -- altering it would be tuning to flatter): native gets W parallel
  workers; offload gets W/2 producer workers per base table (TPC-H) or W (ClickBench), sharing the W-core
  cap with the CH consumer (max_threads 16, throttled by the cap). The invariant held constant is the
  W-core cgroup cap; both engines compete for exactly W cores.

## Shared cgroup cap (the apples-to-apples control)
ONE cgroup per benchmark (`/sys/fs/cgroup/pgch_rep_tpch`, `pgch_rep_cb`) holds BOTH the PG postmaster
tree AND the live CH server: `echo <PMPID> > cg/cgroup.procs; echo <CHPID> > cg/cgroup.procs`. Per W:
`echo "$((W*100000)) 100000" > cg/cpu.max` (= W cores). Measuring shell stays outside. cgroups restored
on exit. Cores reported = CPU-seconds/wall-seconds from /proc/stat (host busy = user+nice+sys+irq+
softirq+steal, excl idle/iowait) minus /proc/<CHPID>/stat utime+stime (= CH); host-minus-CH = producer.

## Exact commands
```
# (one-time) re-point clickbench FDW to the live CH port
sudo -u postgres psql -d clickbench -c "ALTER SERVER ch_bench OPTIONS (SET port '21002');"

# build + install the phase-timer instrumentation
cd /home/ubuntu/pg_clickhouse && make -j"$(nproc)" && sudo make install

# full sweeps (sequential -- both move the same PM/CH pids into a cgroup, cannot overlap)
BENCH=tpch       QUERIES="1 3 4 5 6 7 8 9 10 11 12 14 19" W_LIST="1 2 4 8" N=5 K=3 \
  bash dev/wsweep-report/wsweep_split.sh
BENCH=clickbench QUERIES="$(seq 2 43)"                    W_LIST="1 2 4 8" N=5 K=3 \
  bash dev/wsweep-report/wsweep_split.sh
# (driver dev/wsweep-report/run_all.sh runs both in order)

# assemble the report from the per-cell TSVs
python3 dev/wsweep-report/assemble.py \
  dev/wsweep-report/results/tpch/cells.tsv \
  dev/wsweep-report/results/clickbench/cells.tsv > dev/wsweep-report/REPORT.md
```

## Instruments (per the convergence appendix)
- Instrument 1 (in-code): per-phase CLOCK_THREAD_CPUTIME_ID (CPU) + CLOCK_MONOTONIC (wall) stopwatch,
  emitted in the per-worker `pg_clickhouse shm phase:` LOG (PG server log
  /var/log/postgresql/postgresql-18-main.log); summed across workers by the harness.
- Instrument 2 (perf): `sudo perf record -a --call-graph fp -F 999` during an offload loop; attribution by
  full-stack marker classification with DWARF inline expansion (`perf script --inline`).
- /proc/stat (off_prod, off_cons) and CH system.query_log (query_duration_ms, ProfileEvents UserTime+
  SystemTimeMicroseconds, read_rows) are the third/fourth independent OS/DB-level sources (G3/G4).
