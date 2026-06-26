#!/usr/bin/env bash
# Hot-Cold Phase 0 — W=8 transport sweep driver.
#
# Runs dev/wsweep-report/wsweep_split.sh for BOTH transport modes (adopt baseline +
# copy) across BOTH benchmarks, SEQUENTIALLY (the sweeps move the same PG postmaster
# and CH server pids into a shared cgroup, so they cannot overlap — 10-REPRODUCTION).
# adopt is re-measured FRESH here (not reused from the stale W-sweep report) so the
# copy-vs-adopt comparison is under identical caps in the same session.
#
# Output: dev/hotcold/phase0/results/<mode>/<bench>/{cells.tsv,RESULTS.md}
# Env knobs: QUERIES_TPCH, QUERIES_CB (override query lists for a smoke run);
#            N (warm reps, default 5), K (instrumented reps, default 3).
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
WS="$HERE/../../wsweep-report/wsweep_split.sh"
RESULTS="$HERE/results"
N="${N:-5}"; K="${K:-3}"
QUERIES_TPCH="${QUERIES_TPCH:-1 3 4 5 6 7 8 9 10 11 12 14 19}"
QUERIES_CB="${QUERIES_CB:-$(seq 2 43)}"

la=$(awk '{print $1}' /proc/loadavg)
echo "host load(1m)=$la  (sweeps want < 0.5; idle host control, NON-NEGOTIABLE EVIDENCE STANDARD rule 5)"

run_one(){ # <mode> <bench> <queries>
  local mode="$1" bench="$2" queries="$3" out="$RESULTS/$1/$2"
  mkdir -p "$out"
  echo "===== sweep mode=$mode bench=$bench W=8 N=$N K=$K  ($(date -u +%H:%M:%S)) ====="
  TRANSPORT="$mode" OUT="$out" BENCH="$bench" QUERIES="$queries" W_LIST=8 N="$N" K="$K" \
    bash "$WS" 2>&1 | tee "$out/sweep.log" | tail -3
}

# adopt first (fresh baseline), then copy — per benchmark, all sequential.
run_one adopt tpch       "$QUERIES_TPCH"
run_one copy  tpch       "$QUERIES_TPCH"
run_one adopt clickbench "$QUERIES_CB"
run_one copy  clickbench "$QUERIES_CB"
echo "===== ALL SWEEPS DONE ($(date -u +%H:%M:%S)) ====="
