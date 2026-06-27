#!/usr/bin/env bash
# Hot/Cold ClickBench-100M — Unit 2 angle (b): netem-shaped TCP measurement of the HOT transfer.
#
# The hot/cold MERGE uses SHM (clickhouse_stream_relation), which does not traverse the network stack, so
# netem cannot shape it. Instead we measure the HOT ARM in isolation over the bespoke TCP transport
# (shm_transport_mode=tcp, the path a cross-box deployment would use) under `tc netem` rate+delay on lo
# (reuses dev/hotcold/phase3/p2_netem_kbench.sh's mechanism), then combine analytically with the measured
# loopback cold-arm time. This gives a MEASURED real-wire hot-transfer-time to corroborate angle (a)'s
# bytes/bandwidth analytic — NOT a true cross-box number (that is NO RESULT, §UNIT2).
# Two queries per fraction: a narrow agg (few cols) and a wide SELECT*-count (all 105 cols).
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; CH_PORT="${CH_PORT:-21002}"
PSQLU="sudo -u postgres psql -d clickbench -tAqX -v ON_ERROR_STOP=0"
N="${N:-3}"; FRACS="${FRACS:-p01 p10}"; K="${K:-4}"
OUT="$HERE/results/bench"; mkdir -p "$OUT"; NT="$OUT/nic_netem.tsv"
echo -e "wire\tf\tcols\twall_ms" > "$NT"
# narrow (2 cols) and wide (all 105) hot-transfer probes via the deparser/offload path over TCP
declare -A Q
Q[narrow]="SELECT sum(WatchID::numeric), sum(UserID::numeric) FROM pg.hits_hot_FRAC"
Q[wide]="SELECT count(*) FROM (SELECT * FROM pg.hits_hot_FRAC) t"
restore(){ sudo tc qdisc del dev lo root 2>/dev/null || true; }
trap restore EXIT
runq(){ # $1=sql -> wall_ms (TCP transport, offload)
  local t0 t1; t0=$(date +%s.%N)
  $PSQLU >/dev/null 2>&1 <<SQL
LOAD 'pg_clickhouse'; SET search_path=pg,public; SET pg_clickhouse.local_ch_server='ch_bench';
SET pg_clickhouse.shm_min_rows=0;
SET pg_clickhouse.session_settings='allow_experimental_streamed_table_function 1, max_threads 8';
SET pg_clickhouse.enable_shm_offload=on; SET pg_clickhouse.shm_transport_mode='tcp';
SET pg_clickhouse.tcp_send_method='epoll'; SET pg_clickhouse.tcp_send_inflight_blocks=$K;
SET max_parallel_workers=8; SET max_parallel_workers_per_gather=8; SET statement_timeout='600s';
$1
SQL
  t1=$(date +%s.%N); awk -v a=$t0 -v b=$t1 'BEGIN{printf "%.0f",(b-a)*1000}'; }
med(){ sort -g|awk '{a[NR]=$1}END{print (NR? a[int((NR+1)/2)]:0)}'; }

for wire in bare netem3g; do
  sudo tc qdisc del dev lo root 2>/dev/null || true
  [ "$wire" = netem3g ] && sudo tc qdisc add dev lo root netem delay 50us rate 3gbit
  echo "== wire=$wire ($(tc qdisc show dev lo|head -1)) =="
  for f in $FRACS; do for cols in narrow wide; do
    sql="${Q[$cols]//FRAC/$f}"
    runq "$sql" >/dev/null   # warm
    w=$(for _ in $(seq 1 $N); do runq "$sql"; echo; done | med)
    echo -e "${wire}\t${f}\t${cols}\t${w}" | tee -a "$NT"
  done; done
done
sudo tc qdisc del dev lo root 2>/dev/null || true
echo "== nic netem done: $NT =="