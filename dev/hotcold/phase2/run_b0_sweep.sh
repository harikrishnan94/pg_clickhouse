#!/usr/bin/env bash
# Hot-Cold Phase 2 — Branch 0 W=8 A/B sweep: io_uring-async-TCP vs bespoke-blocking-TCP.
#
# Both modes run FRESH on the SAME CH binary in the SAME session (fresh-baseline discipline). They
# differ ONLY by:
#   iouring-async  (Branch-0 default): tcp_send_method=io_uring (default) + shm_tcp_source_async=1 (default)
#   blocking-bespoke (Phase-1 baseline): tcp_send_method=blocking + shm_tcp_source_async=0
# Pre-registered prediction (00-PRE-REGISTRATION Branch 0): multi-stream W=8 delta ~0 (cost is the kernel
# copy, not syscalls). This sweep CONFIRMS no-regression-within-noise; it is a representative SUBSET (a
# handful of queries per bench), not the full 55-query matrix, because the predicted effect is ~0 and the
# full matrix is covered by the bespoke-TCP Phase-1 baseline. Scope stated honestly in the REPORT.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
WS="$HERE/../../wsweep-report/wsweep_split.sh"
RESULTS="$HERE/results"
N="${N:-5}"; K="${K:-3}"
# Representative subsets (agg-heavy, scan, filter; wide SELECT* on ClickBench Q24 = worst transfer cell).
QUERIES_TPCH="${QUERIES_TPCH:-1 6 19}"
QUERIES_CB="${QUERIES_CB:-2 24}"

echo "host load(1m)=$(awk '{print $1}' /proc/loadavg)  (want < 0.5)"

run_one(){ # <mode_tag> <bench> <queries> <extra_set> <extra_ss>
  local tag="$1" bench="$2" queries="$3" extra_set="$4" extra_ss="$5"
  local out="$RESULTS/$tag/$bench"; mkdir -p "$out"
  echo "===== B0 sweep mode=$tag bench=$bench W=8 N=$N K=$K ($(date -u +%H:%M:%S)) ====="
  TRANSPORT=tcp OUT="$out" BENCH="$bench" QUERIES="$queries" W_LIST=8 N="$N" K="$K" \
    EXTRA_SET="$extra_set" EXTRA_SS="$extra_ss" \
    bash "$WS" 2>&1 | tee "$out/sweep.log" | tail -3
}

for bench_q in "tpch:$QUERIES_TPCH" "clickbench:$QUERIES_CB"; do
  b="${bench_q%%:*}"; q="${bench_q#*:}"
  run_one b0-iouring-async   "$b" "$q" "" ""
  run_one b0-blocking-bespoke "$b" "$q" "SET pg_clickhouse.tcp_send_method='blocking';" "shm_tcp_source_async 0"
done
echo "===== B0 SWEEP DONE ($(date -u +%H:%M:%S)) ====="
