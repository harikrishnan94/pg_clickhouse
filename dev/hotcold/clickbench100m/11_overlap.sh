#!/usr/bin/env bash
# Hot/Cold ClickBench-100M — Unit 1 iter-4 (review C1, cache-controlled): the DEFINITIVE overlap test.
#
# C1: the overlap reference must be the merge's actual COLD ARM (hits_100m WHERE tuple<B(f), reads the 5
# tuple-filter columns), NOT pure-CH (projection-optimized). And cold-arm vs merge MUST be measured in the
# SAME cache state (the earlier cross-run comparison was cache-confounded by the parallel-hot sub-tables).
# Here, per (f,q), measure BACK-TO-BACK, warm, N runs: cold_arm-only / hot-only / merge — all CH
# query_duration_ms. Overlap is SHOWN when merge_ch ≈ max(cold_arm, hot) << cold_arm + hot.
# Instruments (≥2, independent): (A) wall decomposition merge vs max(cold_arm,hot); (B) the producer's
# active window (clickhouse_stream_relation start→exit) vs the merge window — independent producer-side
# signal that can fail differently (producer drains-first => serial). Representative subset spanning the
# cold-arm range. cgroup cap W cores (shared PG+CH).
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; CH_PORT="${CH_PORT:-21002}"
SET_BASE="allow_experimental_streamed_table_function=1&join_use_nulls=1&group_by_use_nulls=1&final=1&count_distinct_implementation=uniqExact&max_bytes_before_external_group_by=4000000000&max_bytes_before_external_sort=4000000000&max_execution_time=600"
PSQLU="sudo -u postgres psql -d clickbench -X -q -v ON_ERROR_STOP=0"
N="${N:-5}"; W="${W:-8}"; FRACS="${FRACS:-p01 p10}"; QLIST="${QLIST:-3 22 13 8 38 17 33 29}"
OUT="$HERE/results/bench"; mkdir -p "$OUT"; OV="$OUT/overlap.tsv"
echo -e "f\tq\tcoldarm_ch\thot_ch\tmerge_ch\tmerge_sd\tmax_arm\tserial\tmerge_over_max\toverlap_ratio\tverdict\tprod_window_ms\tmerge_window_ms" > "$OV"
CG=/sys/fs/cgroup/pgch_hc100m; PERIOD=100000
PMPID=$(sudo head -1 /var/lib/postgresql/18/main/postmaster.pid); CHPID=$(ss -ltnp|grep ":$CH_PORT "|grep -oP 'pid=\K[0-9]+'|head -1)
grep -qw cpu /sys/fs/cgroup/cgroup.subtree_control||echo +cpu|sudo tee /sys/fs/cgroup/cgroup.subtree_control>/dev/null
sudo mkdir -p "$CG"; echo "$PMPID"|sudo tee "$CG/cgroup.procs">/dev/null; echo "$CHPID"|sudo tee "$CG/cgroup.procs">/dev/null
echo "$((W*PERIOD)) $PERIOD"|sudo tee "$CG/cpu.max">/dev/null
restore(){ echo "$PMPID"|sudo tee /sys/fs/cgroup/cgroup.procs>/dev/null 2>&1||true; echo "$CHPID"|sudo tee /sys/fs/cgroup/cgroup.procs>/dev/null 2>&1||true; sudo rmdir "$CG" 2>/dev/null||true; }
trap restore EXIT
chc(){ curl -s "127.0.0.1:${CH_PORT}/" --data-binary @-; }
mms(){ sort -g|awk '{a[NR]=$1;s+=$1;ss+=$1*$1}END{n=NR;if(n==0){print "0 0";exit}m=a[int((n+1)/2)];mean=s/n;sd=sqrt((ss/n-mean*mean>0)?ss/n-mean*mean:0);printf "%s %.1f",m,sd}'; }
chmed(){ local tb=$1 t n; for t in $(seq 1 20); do echo "SYSTEM FLUSH LOGS"|chc>/dev/null 2>&1; n=$(echo "SELECT count() FROM system.query_log WHERE log_comment LIKE '${tb}%' AND type='QueryFinish'"|chc 2>/dev/null); [ "${n:-0}" -ge "$2" ]&&break; sleep 0.5; done
  echo "SELECT query_duration_ms FROM system.query_log WHERE log_comment LIKE '${tb}%' AND type='QueryFinish' ORDER BY event_time_microseconds"|chc 2>/dev/null|mms; }
launch_p(){ local f="$1"; rm -f "/tmp/ov_${f}.out"; ( $PSQLU -tA -c "LOAD 'pg_clickhouse'; SET search_path=pg,public; SET statement_timeout='600s'; SELECT clickhouse_stream_relation('pg.hits_hot_${f}'::regclass,'pgch_hot_${f}',65536);" >"/tmp/ov_${f}.out" 2>&1 ) & PRODPID=$!; local i; for i in $(seq 1 600); do [ -S "/tmp/clickhouse_shm_pgch_hot_${f}.sock" ]&&break; sleep 0.02; done; }

for f in $FRACS; do for q in $QLIST; do
  [ -f "$HERE/templates/q${q}.ch.sql" ] || continue
  csql=$(python3 "$HERE/mk_merge_sql.py" "$HERE/templates/q${q}.ch.sql" "${f}:cold")
  hsql=$(python3 "$HERE/mk_merge_sql.py" "$HERE/templates/q${q}.ch.sql" "${f}:hot")
  msql=$(python3 "$HERE/mk_merge_sql.py" "$HERE/templates/q${q}.ch.sql" "$f")
  # warm each path once, then N timed runs, interleaved minimally (same cache window)
  ct="ovc_${f}_q${q}_$(date +%s%N)"; for k in $(seq 1 $N); do printf '%s' "$csql"|curl -s "127.0.0.1:${CH_PORT}/?${SET_BASE}&max_threads=${W}&log_comment=${ct}_$k" --data-binary @->/dev/null 2>&1; done
  ht="ovh_${f}_q${q}_$(date +%s%N)"; for k in $(seq 1 $N); do launch_p "$f"; printf '%s' "$hsql"|curl -s "127.0.0.1:${CH_PORT}/?${SET_BASE}&max_threads=${W}&log_comment=${ht}_$k" --data-binary @->/dev/null 2>&1; wait $PRODPID 2>/dev/null; done
  mt="ovm_${f}_q${q}_$(date +%s%N)"; pw=0; mw=0
  for k in $(seq 1 $N); do
    launch_p "$f"; t0=$(date +%s.%N)
    printf '%s' "$msql"|curl -s "127.0.0.1:${CH_PORT}/?${SET_BASE}&max_threads=${W}&log_comment=${mt}_$k" --data-binary @->/dev/null 2>&1
    t1=$(date +%s.%N); wait $PRODPID 2>/dev/null; tpe=$(date +%s.%N)
    # producer window = launch→exit ≈ from before t0 to producer exit; merge window = t0→t1
    mw=$(awk -v a=$t0 -v b=$t1 'BEGIN{printf "%.0f",(b-a)*1000}'); pw=$(awk -v a=$t0 -v b=$tpe 'BEGIN{printf "%.0f",(b-a)*1000}')
  done
  read cm csd <<<"$(chmed "$ct" "$N")"; read hm hsd <<<"$(chmed "$ht" "$N")"; read mm msd <<<"$(chmed "$mt" "$N")"
  mx=$(awk -v a="$cm" -v b="$hm" 'BEGIN{print (a>b)?a:b}'); ser=$(awk -v a="$cm" -v b="$hm" 'BEGIN{print a+b}')
  mom=$(awk -v m="$mm" -v x="$mx" 'BEGIN{print (x>0)?m/x:0}'); ovr=$(awk -v s="$ser" -v m="$mm" 'BEGIN{print (m>0)?s/m:0}')
  verdict=$(awk -v m="$mm" -v x="$mx" -v sd="$msd" 'BEGIN{nb=0.08*x; if(sd>nb)nb=sd; print ((m-x)<=nb)?"HIDDEN":"ADD"}')
  echo -e "${f}\t${q}\t${cm}\t${hm}\t${mm}\t${msd}\t${mx}\t${ser}\t${mom}\t${ovr}\t${verdict}\t${pw}\t${mw}" | tee -a "$OV"
done; done
echo "== overlap (cache-controlled) done: $OV =="
