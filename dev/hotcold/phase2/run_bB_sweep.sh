#!/usr/bin/env bash
# Hot-Cold Phase 2 — Branch B W=8 sweep: Arrow-TCP ZERO-COPY (adopt) vs Arrow-TCP COPY (Branch A)
# vs bespoke-TCP vs SHM-adopt. All FRESH on the SAME binaries in the SAME session (fresh-baseline).
# Modes differ ONLY by the transport (shm_transport_mode GUC) and, for the two arrow modes, the CH
# setting shm_arrow_zero_copy (1 = Branch-B adopt, default; 0 = Branch-A copying decode):
#   arrow-adopt : standard Arrow IPC + ZERO-COPY adoption (Branch B; D-HC-0207, recovers the Q24 copy)
#   arrow-copy  : standard Arrow IPC + COPYING decode (Branch A baseline; shm_arrow_zero_copy=0)
#   tcp         : bespoke TcpFrame.h + zero-copy adopt over the recv buffer (the parity target)
#   adopt       : SHM zero-copy adoption (the floor; no kernel recv copy)
#
# Pre-registered (Branch B): arrow-adopt RECOVERS the Branch-A String-heavy regression to bespoke-TCP
# PARITY (the consumer copy-decode layer is removed); the residual gap to SHM-adopt = the irreducible
# kernel recv copy (NOT closable on loopback). "Beat SHM-adopt on loopback" is BANNED.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
WS="$HERE/../../wsweep-report/wsweep_split.sh"
RESULTS="$HERE/results"
N="${N:-5}"; K="${K:-3}"
QUERIES_TPCH="${QUERIES_TPCH:-1 6 19}"
QUERIES_CB="${QUERIES_CB:-2 24}"

echo "host load(1m)=$(awk '{print $1}' /proc/loadavg)  (want < 0.5)"

run_one(){ # <tag> <bench> <queries> <transport> <extra_ss>
  local tag="$1" bench="$2" queries="$3" transport="$4" extra_ss="$5"
  local out="$RESULTS/bB-$tag/$bench"; mkdir -p "$out"
  echo "===== bB sweep tag=$tag bench=$bench W=8 N=$N K=$K ($(date -u +%H:%M:%S)) ====="
  TRANSPORT="$transport" OUT="$out" BENCH="$bench" QUERIES="$queries" W_LIST=8 N="$N" K="$K" \
    EXTRA_SET="" EXTRA_SS="$extra_ss" \
    bash "$WS" 2>&1 | tee "$out/sweep.log" | tail -3
}

for bench_q in "tpch:$QUERIES_TPCH" "clickbench:$QUERIES_CB"; do
  b="${bench_q%%:*}"; q="${bench_q#*:}"
  run_one arrow-adopt "$b" "$q" arrow ""
  run_one arrow-copy  "$b" "$q" arrow "shm_arrow_zero_copy 0"
  run_one tcp         "$b" "$q" tcp   ""
  run_one adopt       "$b" "$q" adopt ""
done
echo "===== bB SWEEP DONE ($(date -u +%H:%M:%S)) ====="
