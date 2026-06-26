# Phase 1 — exact reproduction

## Source versions
- pg_clickhouse: branch `streamed-table-shm-offload`, base `f5acf8dc…` + Phase-0 + the Phase-1 TCP
  producer: `src/shm_producer.{c,h}` (TCP listener + `tcp_publish_block` + frame serializer +
  `shm_producer_tcp_port`), `src/shm_worker.{c,h}` (per-worker `tcp_port` in `ShmWorkerSlot`,
  `hdr->transport`, `pgch_shm_worker_tcp_port`), `src/shm_customscan.c` (`shm_build_union_sql` emits
  `tcp:127.0.0.1:<port_w>` per worker), `src/shm_offload.c` (GUC enum gains `tcp`).
- ClickHouse: `93381ccf…` v26.6.1.1 reldeb + Phase-0 + the Phase-1 TCP consumer:
  `Storages/SharedMemorySource/Wire/TcpFrame.h` (new), `Source/TcpStreamSource.{cpp,h}` (new),
  `Source/StorageShm.{cpp,h}` (Tcp dispatch + host/port), `TableFunctions/TableFunctionShm.cpp`
  (host/port), gtest `tests/gtest_tcp_stream_source.cpp` (new).

## Build + install + restart
```
ninja -C /home/ubuntu/ClickHouse/build/reldeb clickhouse unit_tests_dbms
# restart CH onto the new binary (resolve live pid from ss; manifest CH_PID is stale):
LIVE=$(sudo ss -ltnp | grep ':21002 ' | grep -oP 'pid=\K[0-9]+' | head -1)
sudo kill -TERM "$LIVE"; timeout 90 tail --pid="$LIVE" -f /dev/null
cd /home/ubuntu/ch-bench/tpchcb && nohup /home/ubuntu/ClickHouse/build/reldeb/programs/clickhouse \
  server --config-file=/home/ubuntu/ch-bench/tpchcb/config.xml > restart.log 2>&1 & disown
cd /home/ubuntu/pg_clickhouse && make -j"$(nproc)" && sudo make install
```

## Socket buffers (TCP transport tuning — the TCP analog of the SHM K=4 × 16 MiB = 64 MiB ring)
The producer sets `SO_SNDBUF` and the consumer `SO_RCVBUF` to 32 MiB so the producer can run several
blocks ahead while a (possibly single-threaded) consumer processes a block — without it producer-send
and consumer-process serialize. The kernel cap must allow it (default `net.core.{w,r}mem_max` = 4 MiB):
```
sudo sysctl -w net.core.wmem_max=67108864 net.core.rmem_max=67108864
```
This is a transport-intrinsic config, not flattering: SHM gets a 64 MiB ring; TCP gets 32 MiB socket
buffers. Recorded, applied identically to every TCP cell.

## Selecting TCP transport
- Consumer (per query): `streamed_table('name','schema','tcp:<host>:<port>')`.
- PG driver: `SET pg_clickhouse.shm_transport_mode='tcp';` — each producer worker binds an ephemeral
  127.0.0.1 port, reports it to the backend, and `shm_build_union_sql` emits one
  `streamed_table(..., 'tcp:127.0.0.1:<port_w>')` per worker (one TCP connection per CH stream).

## Correctness gates
```
CH_BIN=/home/ubuntu/ClickHouse/build/reldeb/programs/clickhouse PG_DB=shmdemo TRANSPORT=tcp bash test/shm/verify_offload.sh
/home/ubuntu/ClickHouse/build/reldeb/src/unit_tests_dbms --gtest_filter='TcpStreamSource.*:PollableShmSource.*'
```
verify_offload TRANSPORT=tcp = 137/137 (scans, SEMI/ANTI joins with one TCP connection per relation,
decimal, fail-closed, leak teardown). Asserts `ShmCopiedBlocks≥1` (TCP increments the copy family).

## W=8 three-way sweep (adopt + copy + tcp, fresh, both benches; SEQUENTIAL)
```
bash dev/hotcold/phase1/run_sweeps.sh         # -> results/{adopt,copy,tcp}/{tpch,clickbench}/cells.tsv
python3 dev/hotcold/phase1/overhead_table.py > dev/hotcold/phase1/evidence/overhead-table.md
```
IMPORTANT: TCP needs multiple producer streams to overlap producer-send with consumer-process. The
harness sets per-table `parallel_workers` (W/OFF_PROD_DIV for TPC-H, W for ClickBench) so each cell
gets W (or W/2) producers and `max_threads = total_producers` — a single-stream cell with
`max_threads=1` would serialize the blocking consumer recv with aggregation (a known limitation, see
REPORT §limitations).

## Environment of record: as Phase 0 (AWS Graviton aarch64, PG18 :5432, CH reldeb :21002, shared
cgroup `cpu.max=8*100000/100000`), plus the sysctl above.
