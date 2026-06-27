#!/usr/bin/env bash
# Hot/Cold ClickBench-100M — Unit 1 benchmark + overlap mechanism.
#
# Per cell (fraction f in {p01,p05,p10}, cap W in W_LIST, query q in QLIST), under a shared cgroup
# cpu cap (cap = W cores over BOTH the PG postmaster tree AND the live CH server), measures the
# median-of-N wall time of THREE queries that decompose the overlap:
#   pure_cold(W)   = pure-CH-100M (source = hits_dt64)         -- the cold long-pole, f-independent
#   hot_only(f,W)  = the SAME body over the HOT arm only       -- stream + aggregate just N(f) rows
#   merge(f,W)     = hot streamed UNION ALL cold, one CH exec   -- the thesis
# Overlap is SHOWN when merge ~= max(pure_cold, hot_only) (<< pure_cold + hot_only): the hot transfer
# is hidden under the cold scan. TWO INDEPENDENT instruments (the pre-registered producer phase-split
# is DEAD on the standalone-producer path -- out_stats=NULL, worker-only elog -- see ADVERSARIAL-REVIEW
# C2 / prereg AM3, so it is replaced):
#   (A) bash WALL decomposition: merge_med vs (pure_med + hot_med) serial counterfactual -> overlap_ratio.
#   (B) CH-internal clock: query_duration_ms for the median pure vs merge run (independent of bash wall).
#       If merge_chdur ~= pure_chdur, CH itself spent ~cold-scan time and the hot transfer added ~0 -> hidden.
#   (mechanism) read_rows = hot N + full cold scan in ONE execution; ShmAdoptedBlocks>=1 (hot really streamed);
#       read_bytes (C1: cold-arm filter-column inflation) and a spill flag (C4) are recorded per cell.
# W=1 is a CPU-starvation point (one core shared by producer+consumer -> overlap structurally limited, C3) and
# is labelled as such in the report, not read as evidence against the mechanism.
# clickhouse_stream_relation drains ONCE per consumer, so a fresh producer is launched per run.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; ROOT="$(cd "$HERE/../../.." && pwd)"
CH_PORT="${CH_PORT:-21002}"
SET_BASE="allow_experimental_streamed_table_function=1&join_use_nulls=1&group_by_use_nulls=1&final=1&count_distinct_implementation=uniqExact&max_bytes_before_external_group_by=4000000000&max_bytes_before_external_sort=4000000000&max_execution_time=600"
PSQLU="sudo -u postgres psql -d clickbench -X -q -v ON_ERROR_STOP=0"
N="${N:-5}"; FRACS="${FRACS:-p01 p05 p10}"; W_LIST="${W_LIST:-1 2 4 8}"
QLIST="${QLIST:-$(grep -P '\tELIGIBLE' "$HERE/templates/eligibility.tsv" | sed 's/^q//;s/\t.*//' | sort -n)}"
OUT="$HERE/results/bench"; mkdir -p "$OUT"; CELLS="$OUT/cells.tsv"
[ -s "$CELLS" ] || echo -e "f\tW\tq\tpure_med\tpure_min\tpure_max\tpure_chdur\thot_med\thot_chdur\tmerge_med\tmerge_min\tmerge_max\tmerge_sd\tserial\toverlap_ratio\tmerge_chdur\toverlap_ch\tread_rows\tread_mb\tpeak_mb\tspilled\tshmblk" > "$CELLS"

CG=/sys/fs/cgroup/pgch_hc100m; PERIOD=100000
PMPID=$(sudo head -1 /var/lib/postgresql/18/main/postmaster.pid)
CHPID=$(ss -ltnp 2>/dev/null | grep ":$CH_PORT " | grep -oP 'pid=\K[0-9]+' | head -1)
setup_cg() { grep -qw cpu /sys/fs/cgroup/cgroup.subtree_control || echo +cpu | sudo tee /sys/fs/cgroup/cgroup.subtree_control >/dev/null
  sudo mkdir -p "$CG"; echo "$PMPID" | sudo tee "$CG/cgroup.procs" >/dev/null; echo "$CHPID" | sudo tee "$CG/cgroup.procs" >/dev/null; }
set_cap() { echo "$(($1*PERIOD)) $PERIOD" | sudo tee "$CG/cpu.max" >/dev/null; }
chk_cap() { local v; v=$(cat "$CG/cpu.max"); [ "$v" = "$(($1*PERIOD)) $PERIOD" ] || { echo "  [cap-assert FAIL] cpu.max=$v expected $(($1*PERIOD)) $PERIOD"; return 1; }; }  # D-HC-0406
restore() { echo "$PMPID" | sudo tee /sys/fs/cgroup/cgroup.procs >/dev/null 2>&1 || true
  echo "$CHPID" | sudo tee /sys/fs/cgroup/cgroup.procs >/dev/null 2>&1 || true; sudo rmdir "$CG" 2>/dev/null || true; }
trap restore EXIT
echo "cgroup setup: PMPID=$PMPID CHPID=$CHPID"; setup_cg

# The cgroup cap isolates the benched cgroup's CPU from other host processes (which run on the other
# 32-W cores), so an idle-host gate is not needed for fairness; gate only on genuine host saturation
# (loadavg > 24 on 32 cores), since back-to-back capped cells keep the 1-min loadavg elevated anyway.
load_gate() { local l; l=$(awk '{print $1}' /proc/loadavg); awk -v l="$l" 'BEGIN{exit !(l<24)}' || { echo "  [load-gate] loadavg=$l; sleeping"; sleep 10; }; }
mms() { sort -g | awk '{a[NR]=$1;s+=$1;ss+=$1*$1} END{n=NR;if(n==0){print "0 0 0 0";exit} m=a[int((n+1)/2)];mean=s/n;sd=sqrt((ss/n-mean*mean>0)?ss/n-mean*mean:0); printf "%s %s %s %.1f", m, a[1], a[n], sd}'; }
chc() { curl -s "127.0.0.1:${CH_PORT}/" --data-binary @- ; }
qlog() { # $1=tag $2=col -> value of latest QueryFinish (poll-retry for async flush)
  local t; for t in $(seq 1 12); do echo "SYSTEM FLUSH LOGS" | chc >/dev/null 2>&1
    local n; n=$(echo "SELECT count() FROM system.query_log WHERE log_comment='$1' AND type='QueryFinish'" | chc 2>/dev/null)
    [ "${n:-0}" -ge 1 ] 2>/dev/null && { echo "SELECT $2 FROM system.query_log WHERE log_comment='$1' AND type='QueryFinish' ORDER BY event_time_microseconds DESC LIMIT 1" | chc 2>/dev/null; return; }
    sleep 0.5; done; echo "NA"; }

run_ch() { local t0 t1; t0=$(date +%s.%N); printf '%s' "$1" | curl -s "127.0.0.1:${CH_PORT}/?${SET_BASE}&${SET_W}${2:+&log_comment=$2}" --data-binary @- >/dev/null 2>&1; t1=$(date +%s.%N); awk -v a="$t0" -v b="$t1" 'BEGIN{printf "%.0f",(b-a)*1000}'; }
launch_producer() { local f="$1"; rm -f "/tmp/bench_prod_${f}.out"
  ( $PSQLU -tA -c "LOAD 'pg_clickhouse'; SET search_path=pg,public; SET statement_timeout='600s'; SELECT clickhouse_stream_relation('pg.hits_hot_${f}'::regclass, 'pgch_hot_${f}', 65536);" > "/tmp/bench_prod_${f}.out" 2>&1 ) &
  PRODPID=$!; local i; for i in $(seq 1 400); do [ -S "/tmp/clickhouse_shm_pgch_hot_${f}.sock" ] && break; sleep 0.02; done; }
run_merge() { local f="$1"; launch_producer "$f"; local w; w=$(run_ch "$2" "${3:-}");
  grep -q 'Exception' "/tmp/bench_prod_${f}.out" 2>/dev/null && $PSQLU -tAc "SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE query LIKE '%clickhouse_stream_relation%' AND pid<>pg_backend_pid();" >/dev/null 2>&1
  wait $PRODPID 2>/dev/null; printf '%s' "$w"; }   # printf (no newline): the caller loop's `echo` is the separator

for W in $W_LIST; do
  set_cap "$W"; SET_W="max_threads=${W}"; chk_cap "$W" || exit 1; echo "== cap W=$W =="
  for q in $QLIST; do
    [ -f "$HERE/templates/q${q}.ch.sql" ] || continue
    load_gate
    psql_pure=$(python3 "$HERE/mk_merge_sql.py" "$HERE/templates/q${q}.ch.sql" pure)
    pure_stats=$(for _ in $(seq 1 $N); do run_ch "$psql_pure"; echo; done | mms)
    pure_med=$(echo "$pure_stats" | awk '{print $1}')
    ptag="benchp_W${W}_q${q}_$(date +%s%N)"; run_ch "$psql_pure" "$ptag" >/dev/null; pure_chdur=$(qlog "$ptag" "query_duration_ms")
    for f in $FRACS; do
      load_gate
      msql=$(python3 "$HERE/mk_merge_sql.py" "$HERE/templates/q${q}.ch.sql" "$f")
      hsql=$(python3 "$HERE/mk_merge_sql.py" "$HERE/templates/q${q}.ch.sql" "${f}:hot")
      hot_med=$(for _ in $(seq 1 $N); do run_merge "$f" "$hsql"; echo; done | mms | awk '{print $1}')
      htag="benchh_${f}_W${W}_q${q}_$(date +%s%N)"; run_merge "$f" "$hsql" "$htag" >/dev/null; hot_chdur=$(qlog "$htag" "query_duration_ms")
      read mm mn mx sd <<<"$(for _ in $(seq 1 $N); do run_merge "$f" "$msql"; echo; done | mms)"
      # instrumented merge run (CH query_log: duration, read_rows, read_bytes, peak mem, spill, shmblk)
      mtag="benchm_${f}_W${W}_q${q}_$(date +%s%N)"; run_merge "$f" "$msql" "$mtag" >/dev/null
      chdur=$(qlog "$mtag" "query_duration_ms"); rr=$(qlog "$mtag" "read_rows")
      rmb=$(qlog "$mtag" "round(read_bytes/1048576,1)"); pmb=$(qlog "$mtag" "round(memory_usage/1048576,1)")
      spill=$(qlog "$mtag" "if(ProfileEvents['ExternalAggregationWritten']>0 OR ProfileEvents['ExternalSortWritePart']>0,1,0)")
      shmblk=$(qlog "$mtag" "ProfileEvents['ShmAdoptedBlocks']+ProfileEvents['ShmCopiedBlocks']")
      # PRIMARY overlap metric is CH-internal (excludes the POC producer-LAUNCH overhead the bash wall carries):
      #   overlap_ch = (pure_chdur + hot_chdur) / merge_chdur ; >1 (beyond noise) => hot hidden under cold.
      ovr_ch=$(awk -v p="$pure_chdur" -v h="$hot_chdur" -v m="$chdur" 'BEGIN{if(m>0 && p+0>0 && h+0>0) printf "%.3f",(p+h)/m; else print "NA"}')
      serial=$(awk -v a="$pure_med" -v b="$hot_med" 'BEGIN{print a+b}')   # bash-wall serial (end-to-end, incl launch)
      ovr=$(awk -v s="$serial" -v m="$mm" 'BEGIN{print (m>0)?s/m:0}')
      echo -e "${f}\t${W}\t${q}\t${pure_med}\t$(echo "$pure_stats"|awk '{print $2"\t"$3}')\t${pure_chdur}\t${hot_med}\t${hot_chdur}\t${mm}\t${mn}\t${mx}\t${sd}\t${serial}\t${ovr}\t${chdur}\t${ovr_ch}\t${rr}\t${rmb}\t${pmb}\t${spill}\t${shmblk}" >> "$CELLS"
      echo "  [$f W$W q$q] cold_ch=${pure_chdur} hot_ch=${hot_chdur} merge_ch=${chdur} overlap_ch=${ovr_ch}x | wall pure=${pure_med} merge=${mm} spill=${spill}"
    done
  done
done
echo "== done. cells: $CELLS =="
