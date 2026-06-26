#!/usr/bin/env bash
# Restart the live ClickHouse onto the CURRENTLY-BUILT reldeb binary, wait until ready, then run the
# W=8 parity sweeps under the given LABEL. Used for the baseline (and to restore/re-measure a treatment).
#   LABEL=iouring bash dev/hotcold/phase3/restart_ch_and_sweep.sh
set -uo pipefail
LABEL="${LABEL:?set LABEL}"
CHBIN=/home/ubuntu/ClickHouse/build/reldeb/programs/clickhouse
CFG=/home/ubuntu/ch-bench/tpchcb/config.xml
echo "=== restart CH onto $CHBIN  $(date -u +%FT%TZ) ==="
LIVE=$(sudo ss -ltnp 2>/dev/null | grep ':21002 ' | grep -oP 'pid=\K[0-9]+' | head -1)
if [ -n "$LIVE" ]; then
  sudo kill -TERM "$LIVE"
  timeout 90 tail --pid="$LIVE" -f /dev/null 2>/dev/null || true
fi
cd /home/ubuntu/ch-bench/tpchcb && nohup "$CHBIN" server --config-file="$CFG" > "restart_${LABEL}.log" 2>&1 & disown
for i in $(seq 1 60); do
  curl -s --max-time 2 "http://127.0.0.1:21002/" --data-binary "SELECT 1" 2>/dev/null | grep -q 1 && { echo "CH up after ${i}s"; break; }
  sleep 1
done
NEWPID=$(sudo ss -ltnp 2>/dev/null | grep ':21002 ' | grep -oP 'pid=\K[0-9]+' | head -1)
VER=$(curl -s --max-time 5 "http://127.0.0.1:21002/" --data-binary "SELECT version()" 2>/dev/null)
echo "CH pid=$NEWPID version=$VER"
echo "=== launch parity sweeps LABEL=$LABEL ==="
LABEL="$LABEL" N="${N:-5}" K="${K:-3}" W_LIST="${W_LIST:-8}" bash /home/ubuntu/pg_clickhouse/dev/hotcold/phase3/run_parity_sweeps.sh
echo "=== restart_ch_and_sweep DONE LABEL=$LABEL  $(date -u +%FT%TZ) ==="
