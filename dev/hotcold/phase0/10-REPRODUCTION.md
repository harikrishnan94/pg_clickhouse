# Phase 0 — exact reproduction

## Source versions
- pg_clickhouse: branch `streamed-table-shm-offload`, base `f5acf8dc28544089e3747651b8e3293d9f977566`
  + the Phase-0 diff: GUC `pg_clickhouse.shm_transport_mode` (`src/shm_offload.{c,h}`), the
  per-stream transport arg in `src/shm_customscan.c` (`shm_transport_arg_literal` +
  `shm_build_union_sql`), and the transport-aware test harnesses (`test/shm/verify_offload.sh`,
  `verify_columnar.sh`, `verify_visibility.sh`, `dev/wsweep-report/wsweep_split.sh`).
- ClickHouse: `93381ccf9fba48170df206554afc48c9b48e54ea` v26.6.1.1 + the Phase-0 consumer diff:
  `Storages/SharedMemorySource/Source/{TransportMode.h (new), PollableShmSource.{cpp,h}, StorageShm.{cpp,h}}`,
  `Tracker/AdoptedByteCharger.{cpp,h}`, `Common/ProfileEvents.cpp` (ShmCopied{Blocks,BytesCharged,
  BytesLogical}, ShmCopyTimeMicroseconds), `TableFunctions/TableFunctionShm.{cpp,h}`, and the gtest
  `tests/gtest_pollable_shm_source.cpp` (copy-mode + microbench).

## Build + install
```
# ClickHouse (server + unit tests)
ninja -C /home/ubuntu/ClickHouse/build/reldeb clickhouse unit_tests_dbms
# restart CH onto the new binary (the running server predates the rebuild):
LIVE=$(sudo ss -ltnp | grep ':21002 ' | grep -oP 'pid=\K[0-9]+' | head -1)
sudo kill -TERM "$LIVE"; timeout 90 tail --pid="$LIVE" -f /dev/null
cd /home/ubuntu/ch-bench/tpchcb && nohup /home/ubuntu/ClickHouse/build/reldeb/programs/clickhouse \
  server --config-file=/home/ubuntu/ch-bench/tpchcb/config.xml > restart.log 2>&1 & disown
# wait for :21002, re-resolve live pid via `sudo ss -ltnp | grep :21002` (manifest CH_PID is stale).

# pg_clickhouse extension (loaded per-session via LOAD; no shared_preload, no PG restart)
cd /home/ubuntu/pg_clickhouse && make -j"$(nproc)" && sudo make install
```

## Selecting copy mode
- Per query at the SQL surface (consumer): `streamed_table('name','schema','shm:copy')` (default/2-arg
  = adopt). Reserved: `'tcp:<host>:<port>'` (Phase 1).
- PG-side driver: `SET pg_clickhouse.shm_transport_mode = 'copy';` (enum adopt|copy) — the deparser/
  union-rewriter emits the per-stream transport arg.

## Correctness gates
```
# transport-aware; each spins its own CH on :8123 from CH_BIN, or uses ch_bench :21002
CH_BIN=/home/ubuntu/ClickHouse/build/reldeb/programs/clickhouse PG_DB=shmdemo TRANSPORT=copy  bash test/shm/verify_offload.sh
CH_BIN=/home/ubuntu/ClickHouse/build/reldeb/programs/clickhouse PG_DB=shmdemo TRANSPORT=adopt bash test/shm/verify_offload.sh
TRANSPORT=copy bash test/shm/verify_columnar.sh      # uses ch_bench :21002, tpch_sf10
TRANSPORT=copy bash test/shm/verify_visibility.sh
/home/ubuntu/ClickHouse/build/reldeb/src/unit_tests_dbms --gtest_filter='PollableShmSource.*:Ac3*:AdoptedByteCharger.*:AdoptionLayer.*'
```

## W=8 sweep (adopt fresh baseline + copy, both benches; SEQUENTIAL — shared cgroup)
```
bash dev/hotcold/phase0/run_sweeps.sh
# = TRANSPORT={adopt,copy} OUT=dev/hotcold/phase0/results/<mode>/<bench> BENCH={tpch,clickbench} \
#   W_LIST=8 N=5 K=3 bash dev/wsweep-report/wsweep_split.sh   (4 runs, sequential)
python3 dev/hotcold/phase0/overhead_table.py > dev/hotcold/phase0/evidence/overhead-table.md
```

## Direct copy-cost + mechanism evidence
```
BENCH=tpch       QUERIES="1 6 9 14 19" bash dev/hotcold/phase0/capture_copy_cost.sh   # C2/C3
BENCH=clickbench QUERIES="17 19 24 29" bash dev/hotcold/phase0/capture_copy_cost.sh
BENCH=clickbench Q=24 N=20 bash dev/hotcold/phase0/perf_capture.sh                    # PMU + flamegraph
# microbench ns/byte (hot/cold): the CopyRateConvertNsPerByteMicrobench gtest above.
```

## Environment of record
- AWS Graviton aarch64, 32c/61GiB, dedicated. PG 18 :5432 (log /var/log/postgresql/postgresql-18-main.log).
- CH reldeb, RUN_ID=tpchcb, HTTP :21002 native :21003, config /home/ubuntu/ch-bench/tpchcb/config.xml.
  FDW server `ch_bench` → :21002 (both tpch_sf10 and clickbench).
- Shared cgroup `/sys/fs/cgroup/pgch_rep_{tpch,cb}`, `cpu.max = 8*100000/100000` at W=8; harness
  creates + tears it down (restore trap). Idle host: `uptime` load < 0.5 before each sweep (the
  driver logs the launch-instant load; see pre-reg amendment A3 on the decaying-transient caveat).
- hardware PMU available under `sudo perf` (perf_event_paranoid=4); ARM `cycles` etc. count on CPU-bound load.
```
