#!/usr/bin/env bash
# Hot-Cold Phase 2 — Branch A W=8 sweep: Arrow-TCP vs bespoke-TCP vs SHM-adopt.
#
# All modes run FRESH on the SAME CH binary in the SAME session (fresh-baseline discipline). They differ
# ONLY by the streamed_table() transport (the pg_clickhouse.shm_transport_mode GUC -> the 3rd arg):
#   arrow  : standard Apache Arrow IPC stream + COPYING consumer decode (Branch A; D-HC-0207)
#   tcp    : bespoke TcpFrame.h blocks + zero-copy adopt over the recv buffer (Phase 1 baseline)
#   adopt  : SHM zero-copy adoption straight out of the ring (the absolute floor; no kernel recv copy)
#
# Pre-registered prediction (00-PRE-REGISTRATION Branch A): fixed-width/numeric cells Arrow ~= bespoke
# (PARITY within noise; a fixed-width regression BLOCKS Branch A); String-heavy cells (the copying decode
# rebuilds chars+offsets) MAY regress vs bespoke by a bounded +10-30%, measured + logged, RECOVERED to
# parity in Branch B. Representative SUBSET (agg/filter/join fixed-width + the wide SELECT* String cell),
# scope stated honestly in the REPORT.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
WS="$HERE/../../wsweep-report/wsweep_split.sh"
RESULTS="$HERE/results"
N="${N:-5}"; K="${K:-3}"
# TPC-H: Q1 (agg over decimals+dates = fixed-width parity), Q6 (filter+sum), Q19 (join).
# ClickBench: Q2 (numeric agg = fixed-width parity), Q24 (SELECT * wide ~8 GB = String-heavy regression cell).
QUERIES_TPCH="${QUERIES_TPCH:-1 6 19}"
QUERIES_CB="${QUERIES_CB:-2 24}"
MODES="${MODES:-arrow tcp adopt}"

echo "host load(1m)=$(awk '{print $1}' /proc/loadavg)  (want < 0.5)"

run_one(){ # <transport> <bench> <queries>
  local transport="$1" bench="$2" queries="$3"
  local out="$RESULTS/bA-$transport/$bench"; mkdir -p "$out"
  echo "===== bA sweep transport=$transport bench=$bench W=8 N=$N K=$K ($(date -u +%H:%M:%S)) ====="
  TRANSPORT="$transport" OUT="$out" BENCH="$bench" QUERIES="$queries" W_LIST=8 N="$N" K="$K" \
    EXTRA_SET="" EXTRA_SS="" \
    bash "$WS" 2>&1 | tee "$out/sweep.log" | tail -3
}

for bench_q in "tpch:$QUERIES_TPCH" "clickbench:$QUERIES_CB"; do
  b="${bench_q%%:*}"; q="${bench_q#*:}"
  for m in $MODES; do
    run_one "$m" "$b" "$q"
  done
done
echo "===== bA SWEEP DONE ($(date -u +%H:%M:%S)) ====="
