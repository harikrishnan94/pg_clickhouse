#!/usr/bin/env bash
# Hot/Cold ClickBench-100M — Unit 1 benchmark + overlap mechanism.
#
# Per cell (fraction f in {p01,p05,p10}, cap W in W_LIST, query q in QLIST), under a shared cgroup
# cpu cap (cap = W cores over BOTH the PG postmaster tree AND the live CH server, re-asserted per W),
# measures N runs of THREE queries, capturing BOTH the bash end-to-end wall AND the CH-internal
# query_duration_ms (median + stdev) of each:
#   pure_cold(W)   = pure-CH-100M (source = hits_dt64)         -- the cold long-pole, f-independent
#   hot_only(f,W)  = the SAME body over the HOT arm only       -- stream + aggregate just N(f) rows
#   merge(f,W)     = hot streamed UNION ALL cold, one CH exec   -- the thesis
#
# OVERLAP is assessed CH-INTERNALLY (query_duration_ms), which excludes the POC producer-LAUNCH
# overhead the bash wall carries (a persistent producer would not pay it; the pre-registered
# phase-split STALL instrument is dead on the standalone path -- review C2 / prereg AM3):
#   overlap_ch   = (pure_ch + hot_ch) / merge_ch         (>1 beyond noise => some hiding)
#   hidden_frac  = 1 - (merge_ch - pure_ch)/hot_ch       (1 = hot fully hidden under cold; 0 = fully added)
#   verdict      = HIDDEN if (merge_ch - pure_ch) <= noise band [max(5% of pure_ch, merge_ch_sd, pure_ch_sd)]
#                  else ADD (hot cost additive). Only meaningful when both arms are non-trivial.
# read_bytes (C1 cold-filter inflation), spill flag (C4), ShmAdoptedBlocks (push-down) recorded per cell.
# W=1 is a CPU-starvation point (one core shared by producer+consumer), labelled, not read against the thesis (C3).
# clickhouse_stream_relation drains ONCE per consumer -> a fresh producer per merge/hot run.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; ROOT="$(cd "$HERE/../../.." && pwd)"
CH_PORT="${CH_PORT:-21002}"
SET_BASE="allow_experimental_streamed_table_function=1&join_use_nulls=1&group_by_use_nulls=1&final=1&count_distinct_implementation=uniqExact&max_bytes_before_external_group_by=4000000000&max_bytes_before_external_sort=4000000000&max_execution_time=600"
PSQLU="sudo -u postgres psql -d clickbench -X -q -v ON_ERROR_STOP=0"
N="${N:-5}"; FRACS="${FRACS:-p01 p05 p10}"; W_LIST="${W_LIST:-8}"
QLIST="${QLIST:-$(grep -P '\tELIGIBLE' "$HERE/templates/eligibility.tsv" | sed 's/^q//;s/\t.*//' | sort -n)}"
OUT="$HERE/results/bench"; mkdir -p "$OUT"; CELLS="${CELLS:-$OUT/cells.tsv}"
[ -s "$CELLS" ] || echo -e "f\tW\tq\tpure_wall\tpure_ch\tpure_ch_sd\thot_wall\thot_ch\tmerge_wall\tmerge_wall_sd\tmerge_ch\tmerge_ch_sd\toverlap_ch\thidden_frac\tverdict\tread_rows\tread_mb\tpeak_mb\tspill\tshmblk" > "$CELLS"

CG=/sys/fs/cgroup/pgch_hc100m; PERIOD=100000
PMPID=$(sudo head -1 /var/lib/postgresql/18/main/postmaster.pid)
CHPID=$(ss -ltnp 2>/dev/null | grep ":$CH_PORT " | grep -oP 'pid=\K[0-9]+' | head -1)
setup_cg(){ grep -qw cpu /sys/fs/cgroup/cgroup.subtree_control || echo +cpu | sudo tee /sys/fs/cgroup/cgroup.subtree_control >/dev/null
  sudo mkdir -p "$CG"; echo "$PMPID" | sudo tee "$CG/cgroup.procs" >/dev/null; echo "$CHPID" | sudo tee "$CG/cgroup.procs" >/dev/null; }
set_cap(){ echo "$(($1*PERIOD)) $PERIOD" | sudo tee "$CG/cpu.max" >/dev/null; }
chk_cap(){ [ "$(cat "$CG/cpu.max")" = "$(($1*PERIOD)) $PERIOD" ] || { echo "  [cap-assert FAIL]"; return 1; }; }
restore(){ echo "$PMPID" | sudo tee /sys/fs/cgroup/cgroup.procs >/dev/null 2>&1 || true
  echo "$CHPID" | sudo tee /sys/fs/cgroup/cgroup.procs >/dev/null 2>&1 || true; sudo rmdir "$CG" 2>/dev/null || true; }
trap restore EXIT
echo "cgroup setup: PMPID=$PMPID CHPID=$CHPID"; setup_cg

chc(){ curl -s "127.0.0.1:${CH_PORT}/" --data-binary @- ; }
mms(){ sort -g | awk '{a[NR]=$1;s+=$1;ss+=$1*$1} END{n=NR;if(n==0){print "0 0 0 0";exit} m=a[int((n+1)/2)];mean=s/n;sd=sqrt((ss/n-mean*mean>0)?ss/n-mean*mean:0);printf "%s %s %s %.1f",m,a[1],a[n],sd}'; }
launch_producer(){ local f="$1"; rm -f "/tmp/bench_prod_${f}.out"
  ( $PSQLU -tA -c "LOAD 'pg_clickhouse'; SET search_path=pg,public; SET statement_timeout='600s'; SELECT clickhouse_stream_relation('pg.hits_hot_${f}'::regclass,'pgch_hot_${f}',65536);" >"/tmp/bench_prod_${f}.out" 2>&1 ) &
  PRODPID=$!; local i; for i in $(seq 1 600); do [ -S "/tmp/clickhouse_shm_pgch_hot_${f}.sock" ] && break; sleep 0.02; done; }

# measure: $1=sql $2=tagbase $3=needs_producer $4=f -> echoes "wmed wmin wmax wsd  cmed cmin cmax csd"
# sets globals M_RR M_RMB M_PMB M_SPILL M_BLK from the last run's query_log.
measure(){ local sql="$1" tb="$2" needp="$3" f="${4:-}" k t0 t1; : > /tmp/m_walls
  for k in $(seq 1 "$N"); do
    [ "$needp" = 1 ] && launch_producer "$f"
    t0=$(date +%s.%N); printf '%s' "$sql" | curl -s "127.0.0.1:${CH_PORT}/?${SET_BASE}&${SET_W}&log_comment=${tb}_${k}" --data-binary @- >/dev/null 2>&1; t1=$(date +%s.%N)
    if [ "$needp" = 1 ]; then grep -q 'Exception' "/tmp/bench_prod_${f}.out" 2>/dev/null && $PSQLU -tAc "SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE query LIKE '%clickhouse_stream_relation%' AND pid<>pg_backend_pid();" >/dev/null 2>&1; wait $PRODPID 2>/dev/null; fi
    awk -v a="$t0" -v b="$t1" 'BEGIN{printf "%.0f\n",(b-a)*1000}' >> /tmp/m_walls
  done
  local chd t
  for t in $(seq 1 20); do echo "SYSTEM FLUSH LOGS" | chc >/dev/null 2>&1
    chd=$(echo "SELECT query_duration_ms FROM system.query_log WHERE log_comment LIKE '${tb}%' AND type='QueryFinish' ORDER BY event_time_microseconds" | chc 2>/dev/null)
    [ "$(printf '%s\n' "$chd" | grep -c .)" -ge "$N" ] && break; sleep 0.5
  done
  # extra per-cell metrics from the last run (echoed as trailing fields; globals don't survive $())
  local rr rmb pmb spill blk
  rr=$(echo "SELECT read_rows FROM system.query_log WHERE log_comment LIKE '${tb}%' AND type='QueryFinish' ORDER BY event_time_microseconds DESC LIMIT 1" | chc 2>/dev/null); rr=${rr:-NA}
  rmb=$(echo "SELECT round(read_bytes/1048576,1) FROM system.query_log WHERE log_comment LIKE '${tb}%' AND type='QueryFinish' ORDER BY event_time_microseconds DESC LIMIT 1" | chc 2>/dev/null); rmb=${rmb:-NA}
  pmb=$(echo "SELECT round(memory_usage/1048576,1) FROM system.query_log WHERE log_comment LIKE '${tb}%' AND type='QueryFinish' ORDER BY event_time_microseconds DESC LIMIT 1" | chc 2>/dev/null); pmb=${pmb:-NA}
  spill=$(echo "SELECT max(if(ProfileEvents['ExternalAggregationWritten']>0 OR ProfileEvents['ExternalSortWritePart']>0,1,0)) FROM system.query_log WHERE log_comment LIKE '${tb}%' AND type='QueryFinish'" | chc 2>/dev/null); spill=${spill:-NA}
  blk=$(echo "SELECT max(ProfileEvents['ShmAdoptedBlocks']+ProfileEvents['ShmCopiedBlocks']) FROM system.query_log WHERE log_comment LIKE '${tb}%' AND type='QueryFinish'" | chc 2>/dev/null); blk=${blk:-NA}
  printf '%s  %s  %s %s %s %s %s' "$(mms < /tmp/m_walls)" "$(printf '%s\n' "$chd" | mms)" "$rr" "$rmb" "$pmb" "$spill" "$blk"
}

for W in $W_LIST; do
  set_cap "$W"; SET_W="max_threads=${W}"; chk_cap "$W" || exit 1; echo "== cap W=$W =="
  for q in $QLIST; do
    [ -f "$HERE/templates/q${q}.ch.sql" ] || continue
    read pwm pwn pwx pwsd pcm pcn pcx pcsd _ <<<"$(measure "$(python3 "$HERE/mk_merge_sql.py" "$HERE/templates/q${q}.ch.sql" pure)" "bp_W${W}_q${q}_$(date +%s%N)" 0)"
    for f in $FRACS; do
      read hwm hwn hwx hwsd hcm hcn hcx hcsd _ <<<"$(measure "$(python3 "$HERE/mk_merge_sql.py" "$HERE/templates/q${q}.ch.sql" "${f}:hot")" "bh_${f}_W${W}_q${q}_$(date +%s%N)" 1 "$f")"
      read mwm mwn mwx mwsd mcm mcn mcx mcsd rr rmb pmb spill blk <<<"$(measure "$(python3 "$HERE/mk_merge_sql.py" "$HERE/templates/q${q}.ch.sql" "$f")" "bm_${f}_W${W}_q${q}_$(date +%s%N)" 1 "$f")"
      ovr=$(awk -v p="$pcm" -v h="$hcm" -v m="$mcm" 'BEGIN{print (m>0&&p+h>0)?(p+h)/m:0}')
      hid=$(awk -v p="$pcm" -v h="$hcm" -v m="$mcm" 'BEGIN{print (h>0)?1-(m-p)/h:0}')
      verdict=$(awk -v p="$pcm" -v m="$mcm" -v msd="$mcsd" -v psd="$pcsd" 'BEGIN{nb=0.05*p; if(msd>nb)nb=msd; if(psd>nb)nb=psd; print ((m-p)<=nb)?"HIDDEN":"ADD"}')
      echo -e "${f}\t${W}\t${q}\t${pwm}\t${pcm}\t${pcsd}\t${hwm}\t${hcm}\t${mwm}\t${mwsd}\t${mcm}\t${mcsd}\t${ovr}\t${hid}\t${verdict}\t${rr}\t${rmb}\t${pmb}\t${spill}\t${blk}" >> "$CELLS"
      echo "  [$f W$W q$q] cold_ch=${pcm} hot_ch=${hcm} merge_ch=${mcm}(sd${mcsd}) ovr=${ovr} hidden=${hid} ${verdict} | rmb=${rmb} spill=${spill} blk=${blk}"
    done
  done
done
echo "== done. cells: $CELLS =="
