#!/usr/bin/env bash
# Drift-controlled interleaved A/B: alternate restart-from-C1 / restart-from-baseline each round,
# measuring a decisive query subset (N warm runs) so the BETWEEN-RUN variance (not the tiny within-run
# stdev) is captured and the parity verdict is robust to host/time drift.
#   bash dev/hotcold/phase3/interleave_ab.sh <c1_binary> <baseline_binary>
set -uo pipefail
export EXTRA_SS="" EXTRA_SET=""
C1BIN="${1:?c1 binary}"; BLBIN="${2:?baseline binary}"
ROUNDS="${ROUNDS:-5}"; N="${N:-5}"
CFG=/home/ubuntu/ch-bench/tpchcb/config.xml
HERE=/home/ubuntu/pg_clickhouse
WS="$HERE/dev/wsweep-report/wsweep_split.sh"
ROOT="$HERE/dev/hotcold/phase3/results/interleave"
CBQ="${CBQ:-2 17 33 38}"; TPQ="${TPQ:-6 7 9 14}"
rm -rf "$ROOT"; mkdir -p "$ROOT"

restart_from(){
  local bin="$1"
  local LIVE; LIVE=$(sudo ss -ltnp 2>/dev/null | grep ':21002 ' | grep -oP 'pid=\K[0-9]+' | head -1)
  [ -n "$LIVE" ] && { sudo kill -TERM "$LIVE"; timeout 90 tail --pid="$LIVE" -f /dev/null 2>/dev/null || true; }
  ( cd /home/ubuntu/ch-bench/tpchcb && nohup "$bin" server --config-file="$CFG" > restart_il.log 2>&1 & disown )
  local i; for i in $(seq 1 60); do curl -s --max-time 2 http://127.0.0.1:21002/ --data-binary "SELECT 1" 2>/dev/null | grep -q 1 && return 0; sleep 1; done
  echo "WARN: CH did not come up from $bin" >&2
}
measure_set(){   # $1=out_root
  BENCH=clickbench TRANSPORT=tcp W_LIST=8 N="$N" K=1 QUERIES="$CBQ" OUT="$1/tcp/clickbench" bash "$WS" >/dev/null 2>&1
  BENCH=tpch       TRANSPORT=tcp W_LIST=8 N="$N" K=1 QUERIES="$TPQ" OUT="$1/tcp/tpch"       bash "$WS" >/dev/null 2>&1
}
echo "=== interleave A/B  C1=$C1BIN  baseline=$BLBIN  ROUNDS=$ROUNDS N=$N  $(date -u +%FT%TZ) ==="
echo "lo qdisc: $(tc qdisc show dev lo | head -1)"
for r in $(seq 1 "$ROUNDS"); do
  echo "-- round $r / C1       $(date -u +%T) --"; restart_from "$C1BIN"; measure_set "$ROOT/c1/r$r"
  echo "-- round $r / baseline $(date -u +%T) --"; restart_from "$BLBIN"; measure_set "$ROOT/baseline/r$r"
done
echo "=== INTERLEAVE DONE  $(date -u +%FT%TZ) ==="
