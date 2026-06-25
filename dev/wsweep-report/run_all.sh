#!/usr/bin/env bash
# Driver: full TPC-H then ClickBench W-sweep+split. Sequential (shared PG/CH/cgroup).
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
ts(){ date '+%F %T'; }
echo "[$(ts)] START full sweeps"
echo "[$(ts)] === TPC-H ==="
BENCH=tpch QUERIES="1 3 4 5 6 7 8 9 10 11 12 14 19" W_LIST="1 2 4 8" N=5 K=3 \
  bash "$HERE/wsweep_split.sh" > "$HERE/results/tpch.log" 2>&1
echo "[$(ts)] TPC-H rc=$?"
echo "[$(ts)] === ClickBench ==="
BENCH=clickbench QUERIES="$(seq 2 43)" W_LIST="1 2 4 8" N=5 K=3 \
  bash "$HERE/wsweep_split.sh" > "$HERE/results/clickbench.log" 2>&1
echo "[$(ts)] ClickBench rc=$?"
echo "[$(ts)] ALL DONE"
