#!/usr/bin/env bash
# Hot-Cold Phase 2 — Branch B iteration 3 (lean Arrow buffer-extraction) W=8 sweep.
# Five modes, ALL fresh on the SAME binaries in the SAME session (fresh-baseline), differing ONLY by the
# transport GUC and the two CH settings shm_arrow_zero_copy / shm_arrow_lean_extract:
#   arrow-lean : Arrow IPC + ZERO-COPY adopt via the LEAN direct-flatbuffer extraction (it3 default)
#   arrow-it2  : Arrow IPC + ZERO-COPY adopt via arrow::ipc::ReadRecordBatch (it2; shm_arrow_lean_extract=0)
#   arrow-copy : Arrow IPC + COPYING decode (Branch A; shm_arrow_zero_copy=0)
#   tcp        : bespoke TcpFrame.h + zero-copy adopt (the parity target)
#   adopt      : SHM zero-copy adoption (the floor; no kernel recv copy)
#
# Pre-registered (B-it3, 00-PRE-REGISTRATION amendment): the lean extraction eliminates the per-block
# arrow::Array/ArrayData/Buffer-slice construction (~105/block on CB Q24 SELECT *) → predicted cons_user
# drops ~40-47 ms vs arrow-it2 on Q24; wall recovers from the it2 +7.4% toward bespoke-tcp parity
# (predicted +0..4%). Fixed-width stays parity. "Beat SHM-adopt on loopback" is BANNED.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
WS="$HERE/../../wsweep-report/wsweep_split.sh"
RESULTS="$HERE/results"
N="${N:-5}"; K="${K:-3}"
QUERIES_TPCH="${QUERIES_TPCH:-1 6 19}"
QUERIES_CB="${QUERIES_CB:-2 24}"
# MODES lets a quick smoke run pick a subset, e.g. MODES="arrow-lean arrow-it2 tcp" BENCHES="clickbench".
MODES="${MODES:-arrow-lean arrow-it2 arrow-copy tcp adopt}"
BENCHES="${BENCHES:-tpch clickbench}"

echo "host load(1m)=$(awk '{print $1}' /proc/loadavg)  (want < 0.5)"

run_one(){ # <tag> <bench> <queries> <transport> <extra_ss>
  local tag="$1" bench="$2" queries="$3" transport="$4" extra_ss="$5"
  local out="$RESULTS/bit3-$tag/$bench"; mkdir -p "$out"
  echo "===== bit3 sweep tag=$tag bench=$bench W=8 N=$N K=$K ($(date -u +%H:%M:%S)) ====="
  TRANSPORT="$transport" OUT="$out" BENCH="$bench" QUERIES="$queries" W_LIST=8 N="$N" K="$K" \
    EXTRA_SET="" EXTRA_SS="$extra_ss" \
    bash "$WS" 2>&1 | tee "$out/sweep.log" | tail -3
}

mode_transport(){ case "$1" in arrow-*) echo arrow;; tcp) echo tcp;; adopt) echo adopt;; esac; }
mode_ss(){ case "$1" in
  arrow-lean) echo "";;
  arrow-it2)  echo "shm_arrow_lean_extract 0";;
  arrow-copy) echo "shm_arrow_zero_copy 0";;
  *) echo "";;
esac; }

for bench in $BENCHES; do
  case "$bench" in tpch) q="$QUERIES_TPCH";; clickbench) q="$QUERIES_CB";; *) echo "unknown bench $bench"; exit 1;; esac
  for m in $MODES; do
    run_one "$m" "$bench" "$q" "$(mode_transport "$m")" "$(mode_ss "$m")"
  done
done
echo "===== bit3 SWEEP DONE ($(date -u +%H:%M:%S)) ====="
