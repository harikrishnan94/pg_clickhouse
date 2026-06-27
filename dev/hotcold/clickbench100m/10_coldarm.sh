#!/usr/bin/env bash
# Hot/Cold ClickBench-100M — Unit 1 iter-4 (review C1 fix): measure the merge's COLD-ARM-ONLY time.
#
# Review C1: the overlap verdict wrongly used pure-CH (hits_dt64, projection-optimized, no filter) as the
# cold reference. The merge's actual cold arm is `hits_100m WHERE tuple<B(f)` — it reads the 5 boundary-tuple
# columns over 100M and loses CH's projection short-circuit, so it costs MUCH more than pure-CH for narrow
# queries. The CORRECT overlap reference is this cold-arm-only time. No producer needed (pure CH query).
# Writes results/bench/coldarm.tsv (f q coldarm_ch coldarm_sd). cgroup cap = W cores (shared PG+CH).
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; CH_PORT="${CH_PORT:-21002}"
SET_BASE="allow_experimental_streamed_table_function=1&join_use_nulls=1&group_by_use_nulls=1&final=1&count_distinct_implementation=uniqExact&max_bytes_before_external_group_by=4000000000&max_bytes_before_external_sort=4000000000&max_execution_time=600"
N="${N:-5}"; W="${W:-8}"; FRACS="${FRACS:-p01 p10}"
QLIST="${QLIST:-$(grep -P '\tELIGIBLE' "$HERE/templates/eligibility.tsv" | sed 's/^q//;s/\t.*//' | sort -n)}"
OUT="$HERE/results/bench"; mkdir -p "$OUT"; CA="$OUT/coldarm.tsv"; echo -e "f\tq\tcoldarm_ch\tcoldarm_sd\tcoldarm_mb" > "$CA"
CG=/sys/fs/cgroup/pgch_hc100m; PERIOD=100000
PMPID=$(sudo head -1 /var/lib/postgresql/18/main/postmaster.pid); CHPID=$(ss -ltnp|grep ":$CH_PORT "|grep -oP 'pid=\K[0-9]+'|head -1)
grep -qw cpu /sys/fs/cgroup/cgroup.subtree_control||echo +cpu|sudo tee /sys/fs/cgroup/cgroup.subtree_control>/dev/null
sudo mkdir -p "$CG"; echo "$PMPID"|sudo tee "$CG/cgroup.procs">/dev/null; echo "$CHPID"|sudo tee "$CG/cgroup.procs">/dev/null
echo "$((W*PERIOD)) $PERIOD"|sudo tee "$CG/cpu.max">/dev/null
restore(){ echo "$PMPID"|sudo tee /sys/fs/cgroup/cgroup.procs>/dev/null 2>&1||true; echo "$CHPID"|sudo tee /sys/fs/cgroup/cgroup.procs>/dev/null 2>&1||true; sudo rmdir "$CG" 2>/dev/null||true; }
trap restore EXIT
chc(){ curl -s "127.0.0.1:${CH_PORT}/" --data-binary @-; }
mms(){ sort -g|awk '{a[NR]=$1;s+=$1;ss+=$1*$1}END{n=NR;if(n==0){print "0 0";exit}m=a[int((n+1)/2)];mean=s/n;sd=sqrt((ss/n-mean*mean>0)?ss/n-mean*mean:0);printf "%s %.1f",m,sd}'; }
for f in $FRACS; do for q in $QLIST; do
  [ -f "$HERE/templates/q${q}.ch.sql" ] || continue
  sql=$(python3 "$HERE/mk_merge_sql.py" "$HERE/templates/q${q}.ch.sql" "${f}:cold")
  tb="ca_${f}_q${q}_$(date +%s%N)"
  for k in $(seq 1 $N); do printf '%s' "$sql" | curl -s "127.0.0.1:${CH_PORT}/?${SET_BASE}&max_threads=${W}&log_comment=${tb}_${k}" --data-binary @- >/dev/null 2>&1; done
  for t in $(seq 1 20); do echo "SYSTEM FLUSH LOGS"|chc>/dev/null 2>&1; n=$(echo "SELECT count() FROM system.query_log WHERE log_comment LIKE '${tb}%' AND type='QueryFinish'"|chc 2>/dev/null); [ "${n:-0}" -ge "$N" ]&&break; sleep 0.5; done
  read cm csd <<<"$(echo "SELECT query_duration_ms FROM system.query_log WHERE log_comment LIKE '${tb}%' AND type='QueryFinish' ORDER BY event_time_microseconds"|chc 2>/dev/null|mms)"
  cmb=$(echo "SELECT round(read_bytes/1048576,1) FROM system.query_log WHERE log_comment LIKE '${tb}%' AND type='QueryFinish' ORDER BY event_time_microseconds DESC LIMIT 1"|chc 2>/dev/null)
  echo -e "${f}\t${q}\t${cm}\t${csd}\t${cmb}" | tee -a "$CA"
done; done
echo "== coldarm done: $CA =="
