# Hot-Cold Phase 2 — exact reproduction (extends phase1/10-REPRODUCTION.md)

## Source versions (at pre-registration)
- pg_clickhouse: branch `streamed-table-shm-offload`, base `3ac38402f917`.
- ClickHouse: branch `streamed_table`, base `2847326da921` (reldeb v26.6.1.1).
- liburing-dev **2.14** (`/usr/include/liburing.h`, `sudo apt-get install -y liburing-dev`).
- Arrow bundled in CH `contrib/arrow` = **23.0.1**. nanoarrow vendored under `src/nanoarrow/` (Branch A; version logged in DECISIONS).

## Environment of record
- AWS Graviton **aarch64**, 32c / 61 GiB, dedicated/idle. Kernel `7.0.0-1006-aws`.
- PG18 `:5432`; CH reldeb `:21002` (resolve LIVE pid from `ss`, manifest stale); shared cgroup-v2
  `cpu.max=8*100000/100000` at W=8.
- `lo`: no ring (`ethtool -g lo` → Operation not supported). ENA `ens34`: `TCP data split: n/a`.

## Build + install + restart (same as phase 1)
```
# ClickHouse (consumer):
ninja -C /home/ubuntu/ClickHouse/build/reldeb clickhouse unit_tests_dbms
# restart CH onto the new binary (live pid from ss; manifest CH_PID is stale):
LIVE=$(sudo ss -ltnp | grep ':21002 ' | grep -oP 'pid=\K[0-9]+' | head -1)
sudo kill -TERM "$LIVE"; timeout 90 tail --pid="$LIVE" -f /dev/null
cd /home/ubuntu/ch-bench/tpchcb && nohup /home/ubuntu/ClickHouse/build/reldeb/programs/clickhouse \
  server --config-file=/home/ubuntu/ch-bench/tpchcb/config.xml > restart.log 2>&1 & disown
# pg_clickhouse (producer):
cd /home/ubuntu/pg_clickhouse && make -j"$(nproc)" && sudo make install
# (PG must reconnect / new backend to pick up a reinstalled .so — new psql session.)
```

## Socket buffers + sysctls (identical to phase 1; applied to every TCP cell, bespoke + Arrow)
```
sudo sysctl -w net.core.wmem_max=67108864 net.core.rmem_max=67108864
# producer SO_SNDBUF=32 MiB, consumer SO_RCVBUF=32 MiB (in code).
```

## Transport selection (per query, 3rd literal arg to streamed_table)
- `''`/`'shm'`/`'shm:adopt'` → SHM adopt; `'shm:copy'` → SHM copy.
- `'tcp:<host>:<port>'` → bespoke TCP (phase 1).
- Phase 2 Arrow-TCP token: see DECISIONS (D-HC-02xx) — `'arrow:<host>:<port>'` (keeps bespoke selectable).
- PG driver GUC `pg_clickhouse.shm_transport_mode` selects which `streamed_table(...)` arg the deparser emits.

## Correctness gates (run in the NEW mode before measuring; never measure on red)
```
CH_BIN=/home/ubuntu/ClickHouse/build/reldeb/programs/clickhouse PG_DB=shmdemo TRANSPORT=tcp \
  bash test/shm/verify_offload.sh          # bespoke TCP regression oracle (137/137 floor)
# (+ TRANSPORT=arrow once Branch A lands)
CH_BIN=… PG_DB=shmdemo bash test/shm/verify_columnar.sh
CH_BIN=… PG_DB=shmdemo bash test/shm/verify_visibility.sh
/home/ubuntu/ClickHouse/build/reldeb/src/unit_tests_dbms --gtest_filter='TcpStreamSource.*:PollableShmSource.*:Adoption*:*Arrow*'
```

## W=8 sweep (fresh, both benches; mirror phase0/1 run_sweeps.sh)
```
bash dev/hotcold/phase2/run_sweeps.sh        # -> results/<mode>/{tpch,clickbench}/cells.tsv
python3 dev/hotcold/phase2/overhead_table.py > dev/hotcold/phase2/evidence/overhead-table.md
```
Modes per branch: B0 = {bespoke-tcp, iouring-tcp} (+ single-stream overlap probe); A = {bespoke-tcp,
arrow-tcp, shm-adopt, shm-copy}; B = {arrow-tcp-copy, arrow-tcp-zerocopy, bespoke-tcp, shm-adopt}.

## Fresh-baseline discipline
Re-measure EVERY compared mode FRESH on the SAME binary in the SAME session (the binary changes when the
consumer changes). Record exact commands + env + SHAs per run in METHODOLOGY-LOG.md.

## This session's fresh baseline (L0001)
`verify_offload TRANSPORT=tcp` = 137/137 PASS on `3ac38402` / `2847326da921` (green HEAD, pre-change).
