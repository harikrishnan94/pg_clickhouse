# Hot-Cold Phase 3 — exact reproduction

## SHAs
- pg_clickhouse `streamed-table-shm-offload`: C1 baseline parent = `29a1e501…`; C1 work is consumer-only
  (no pg_clickhouse code change in C1). P1/P2 land here.
- ClickHouse `streamed_table`: C1 baseline parent = `c286c6cc…`; C1 = `c27379bcdaf` + review-fix
  `97fc5b8b536`.

## Environment of record
- AWS Graviton aarch64, 32c / 61 GiB, dedicated/idle (pre-sweep load < 0.5).
- PG 18 on :5432; ClickHouse reldeb live on 127.0.0.1:21002 (HTTP), :21003 (TCP); resolve the live pid:
  `sudo ss -ltnp | grep ':21002 ' | grep -oP 'pid=\K[0-9]+'`.
- sysctls (TCP transport buffers): `sudo sysctl -w net.core.wmem_max=67108864 net.core.rmem_max=67108864`.
  Producer SO_SNDBUF + consumer SO_RCVBUF hard-set 32 MiB.
- `tc qdisc show dev lo` MUST be `noqueue` before any bare-loopback parity measurement (H12). No netem
  installed for C1/P1 (netem is a P2 instrument).
- Shared cgroup-v2 cap: the wsweep harness moves the PG postmaster tree + the live CH into one cgroup with
  `cpu.max = W*100000 / 100000` (W=8).

## Build + restart
```
ninja -C /home/ubuntu/ClickHouse/build/reldeb clickhouse unit_tests_dbms
# restart CH onto the new binary:
LIVE=$(sudo ss -ltnp | grep ':21002 ' | grep -oP 'pid=\K[0-9]+' | head -1)
sudo kill -TERM "$LIVE"; timeout 90 tail --pid="$LIVE" -f /dev/null
cd /home/ubuntu/ch-bench/tpchcb && nohup /home/ubuntu/ClickHouse/build/reldeb/programs/clickhouse \
  server --config-file=/home/ubuntu/ch-bench/tpchcb/config.xml > restart.log 2>&1 & disown
# (P1/P2 also:) cd /home/ubuntu/pg_clickhouse && make -j"$(nproc)" && sudo make install
```

## Correctness gates (C1; spins up its own ephemeral CH from CH_BIN)
```
CH_BIN=/home/ubuntu/ClickHouse/build/reldeb/programs/clickhouse PG_DB=shmdemo TRANSPORT=tcp   bash test/shm/verify_offload.sh   # 137/137
CH_BIN=/home/ubuntu/ClickHouse/build/reldeb/programs/clickhouse PG_DB=shmdemo TRANSPORT=arrow bash test/shm/verify_offload.sh   # 137/137
/home/ubuntu/ClickHouse/build/reldeb/src/unit_tests_dbms --gtest_filter='TcpStreamSource.*:ArrowStreamSource.*'  # 20/20
```

## W=8 parity sweeps (phase3 driver)
```
# treatment / baseline full matrix {tcp,arrow}x{clickbench,tpch}, W=8, N=5, K=3:
LABEL=C1      N=5 K=3 W_LIST=8 bash dev/hotcold/phase3/run_parity_sweeps.sh   # results/C1/...
LABEL=iouring N=5 K=3 W_LIST=8 bash dev/hotcold/phase3/restart_ch_and_sweep.sh # restart onto baseline binary, sweep
python3 dev/hotcold/phase3/compare_parity.py dev/hotcold/phase3/results/C1 dev/hotcold/phase3/results/iouring --label-t C1 --label-b iouring
```
The harness needs `EXTRA_SS=""` `EXTRA_SET=""` exported (Phase-2 hooks; the drivers set them). It resolves
the live CH pid from the manifest `/home/ubuntu/ch-bench/tpchcb/manifest.env` (CH_HTTP_PORT=21002).

## Drift-controlled interleaved A/B (the trustworthy parity verdict — required on this host)
Sequential treatment-then-baseline campaigns are confounded by host/CH-warmth drift (TPC-H sf10 shows
±~25% between-run variance the within-run N=5 sd hides). Save both binaries aside and interleave:
```
cp build/reldeb/programs/clickhouse <scratch>/ch_baseline   # baseline built first
# ... rebuild treatment ...; cp build/reldeb/programs/clickhouse <scratch>/ch_c1
ROUNDS=5 N=5 bash dev/hotcold/phase3/interleave_ab.sh <scratch>/ch_c1 <scratch>/ch_baseline
python3 dev/hotcold/phase3/agg_interleave.py dev/hotcold/phase3/results/interleave
```
Each round restarts CH from each binary and measures the decisive subset, so C1 and baseline samples are
adjacent in time. Verdict band = max(5%, observed between-round sd). C1 result: 8/8 PARITY.
