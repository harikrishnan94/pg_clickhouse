#!/usr/bin/env bash
# Hot-Cold Phase 3 — W=8 parity sweep driver (C1 / P1 baselines & treatments).
# Runs the 4 cells of the parity matrix {tcp,arrow} x {clickbench,tpch} at W=8 for the
# CURRENTLY-RUNNING ClickHouse binary. LABEL picks the OUT subdir (e.g. C1, baseline-c286c6cc).
#   LABEL=C1 bash dev/hotcold/phase3/run_parity_sweeps.sh
set -uo pipefail
LABEL="${LABEL:?set LABEL=<dirname>}"
N="${N:-5}"; K="${K:-3}"; W_LIST="${W_LIST:-8}"
export EXTRA_SS="${EXTRA_SS:-}"     # Phase-2 hook (extra CH session settings); empty here
export EXTRA_SET="${EXTRA_SET:-}"   # Phase-2 hook (extra PG GUC SET lines); empty here
HERE=/home/ubuntu/pg_clickhouse
WS="$HERE/dev/wsweep-report/wsweep_split.sh"
ROOT="$HERE/dev/hotcold/phase3/results/$LABEL"
mkdir -p "$ROOT"
echo "=== parity sweeps LABEL=$LABEL W=$W_LIST N=$N K=$K  $(date -u +%FT%TZ) ==="
echo "lo qdisc: $(tc qdisc show dev lo | head -1)"   # must be noqueue for parity (H12)
for TR in tcp arrow; do
  for B in clickbench tpch; do
    OUT="$ROOT/$TR/$B"
    mkdir -p "$OUT"
    echo "--- sweep transport=$TR bench=$B -> $OUT  $(date -u +%T) ---"
    BENCH="$B" TRANSPORT="$TR" W_LIST="$W_LIST" N="$N" K="$K" OUT="$OUT" bash "$WS" \
        > "$OUT/sweep.log" 2>&1
    echo "    rc=$? cells=$(wc -l < "$OUT/cells.tsv" 2>/dev/null || echo NA)"
  done
done
echo "=== DONE LABEL=$LABEL  $(date -u +%FT%TZ) ==="
