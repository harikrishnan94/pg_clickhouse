#!/usr/bin/env bash
# Hot-Cold Phase 3 P2 K-sweep (run-ahead depth). Interleaves K across rounds (drift control); the run-ahead
# K is set per-query via EXTRA_SET (the wsweep PG-GUC hook) — no .so swap / no restart. Consumer = live C1.
#   REGIME=bare|netem METHOD=epoll|msg_zerocopy ROUNDS=3 KLIST="1 2 4 8" bash dev/hotcold/phase3/p2_ksweep.sh
set -uo pipefail
export EXTRA_SS=""
REGIME="${REGIME:?bare|netem<tag>}"; ROUNDS="${ROUNDS:-3}"; N="${N:-5}"
KLIST="${KLIST:-1 2 4 8}"; METHOD="${METHOD:-epoll}"
CBQ="${CBQ:-2 17 33}"; TPQ="${TPQ:-6 9}"
HERE=/home/ubuntu/pg_clickhouse; WS="$HERE/dev/wsweep-report/wsweep_split.sh"
ROOT="$HERE/dev/hotcold/phase3/results/p2_ksweep_$REGIME"; rm -rf "$ROOT"; mkdir -p "$ROOT"
echo "=== P2 K-sweep REGIME=$REGIME METHOD=$METHOD KLIST='$KLIST' ROUNDS=$ROUNDS N=$N  $(date -u +%FT%TZ) ==="
echo "lo qdisc: $(tc qdisc show dev lo | head -1)"
# NB: the run-ahead level is KV (NOT K) — the wsweep uses `K` for its own instrumented-run count, so a
# loop var named K collides with the `K=1` prefix and silently routes every level to the same OUT dir.
for r in $(seq 1 "$ROUNDS"); do
  for KV in $KLIST; do
    export EXTRA_SET="SET pg_clickhouse.tcp_send_method='$METHOD'; SET pg_clickhouse.tcp_send_inflight_blocks=$KV; SET pg_clickhouse.tcp_sndbuf_bytes=${SNDBUF:-0};"
    echo "-- round $r KV=$KV  $(date -u +%T) --"
    BENCH=clickbench TRANSPORT=tcp W_LIST=8 N="$N" K=1 QUERIES="$CBQ" OUT="$ROOT/k$KV/r$r/tcp/clickbench" bash "$WS" >/dev/null 2>&1
    BENCH=tpch       TRANSPORT=tcp W_LIST=8 N="$N" K=1 QUERIES="$TPQ" OUT="$ROOT/k$KV/r$r/tcp/tpch"       bash "$WS" >/dev/null 2>&1
  done
done
echo "=== P2 KSWEEP DONE REGIME=$REGIME  $(date -u +%FT%TZ) ==="
