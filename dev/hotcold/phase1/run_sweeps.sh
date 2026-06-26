#!/usr/bin/env bash
# Hot-Cold Phase 1 — W=8 three-way transport sweep (adopt, copy, tcp), both benchmarks.
#
# All three modes are re-measured FRESH on the SAME CH binary in the SAME session (the binary
# changed when TcpStreamSource landed, so the Phase-0 adopt/copy numbers are not reused — the
# adversarial standard requires the baseline measured fresh under identical caps). SEQUENTIAL: the
# sweeps move the same PG postmaster + CH server into a shared cgroup and cannot overlap.
#
# Output: dev/hotcold/phase1/results/<mode>/<bench>/{cells.tsv,RESULTS.md}
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
WS="$HERE/../../wsweep-report/wsweep_split.sh"
RESULTS="$HERE/results"
N="${N:-5}"; K="${K:-3}"
MODES="${MODES:-adopt copy tcp}"
QUERIES_TPCH="${QUERIES_TPCH:-1 3 4 5 6 7 8 9 10 11 12 14 19}"
QUERIES_CB="${QUERIES_CB:-$(seq 2 43)}"

echo "host load(1m)=$(awk '{print $1}' /proc/loadavg)  (want < 0.5; idle-host control)"

run_one(){ # <mode> <bench> <queries>
  local mode="$1" bench="$2" queries="$3" out="$RESULTS/$1/$2"
  mkdir -p "$out"
  echo "===== sweep mode=$mode bench=$bench W=8 N=$N K=$K  ($(date -u +%H:%M:%S)) ====="
  TRANSPORT="$mode" OUT="$out" BENCH="$bench" QUERIES="$queries" W_LIST=8 N="$N" K="$K" \
    bash "$WS" 2>&1 | tee "$out/sweep.log" | tail -3
}

for mode in $MODES; do
  run_one "$mode" tpch       "$QUERIES_TPCH"
done
for mode in $MODES; do
  run_one "$mode" clickbench "$QUERIES_CB"
done
echo "===== ALL PHASE-1 SWEEPS DONE ($(date -u +%H:%M:%S)) ====="
