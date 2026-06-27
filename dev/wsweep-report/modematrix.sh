#!/usr/bin/env bash
# Transport-mode matrix vs optimal-native PG at a fixed W (default 8), WITH producer phase split +
# consumer metrics + transport counters per mode. Reuses the proven shared-cgroup-cap methodology of
# wsweep_split.sh: BOTH the PG postmaster tree AND the live ClickHouse server in ONE cgroup-v2 capped
# to cpu.max=W*period; measuring shell outside.
#
# For EACH query: measure optimal-native ONCE (offload off, work_mem=2GB, JIT, parallel costs 0,
# mpwpg=W), then EACH offload transport mode back-to-back (all modes share one same-session native,
# measured adjacent -> tight drift control). Speedup = native_med / offload_med over N stats-OFF warm
# runs. Then K stats-ON instrumented runs per mode capture:
#   producer phase split  (READ/DEFORM/PUBLISH/STALL, cpu+wall ms; "shm phase" LOG, gate
#                           shm_log_stream_stats=on; vectorized reader; summed across W workers)
#   transport counters    (method, epoll/blocking/zc sends, zc_copied, K, pumps, overlap_frames;
#                           "shm tcp-send" LOG; tcp/arrow only)
#   consumer metrics      (CH system.query_log: query_duration_ms, UserTime/SystemTime us,
#                           ShmCopyTimeMicroseconds, Shm{Adopted,Copied}BytesCharged, blocks)
# NB: consumer has NO true per-phase wall split (only total wall + cpu split + the conflated
# ShmCopyTimeMicroseconds = copy-memcpy OR tcp-recv). Producer STALL is SHM-only (tcp/arrow=0).
#
# Modes (label:transport:tcp_send_method ; method empty for non-tcp):
#   shm_adopt:adopt:  shm_copy:copy:  tcp_epoll:tcp:epoll  tcp_zerocopy:tcp:msg_zerocopy
#   tcp_blocking:tcp:blocking  arrow:arrow:
#
#   BENCH=tpch       QUERIES="$(seq 1 22)" N=4 K=2 W=8 dev/wsweep-report/modematrix.sh
#   BENCH=clickbench QUERIES="$(seq 1 43)" N=4 K=2 W=8 dev/wsweep-report/modematrix.sh
set -uo pipefail

BENCH="${BENCH:?set BENCH=tpch|clickbench}"
N="${N:-3}"           # best-of-N (min), ClickBench-style; all N runs are stats-on, phase from the min run
K="${K:-2}"           # (deprecated: phase split now taken from the min of the N runs)
W="${W:-8}"
RUN_ID="${RUN_ID:-tpchcb}"
MODES="${MODES:-shm_adopt:adopt: shm_copy:copy: tcp_epoll:tcp:epoll tcp_zerocopy:tcp:msg_zerocopy tcp_blocking:tcp:blocking arrow:arrow:}"
REPORT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOGF=/var/log/postgresql/postgresql-18-main.log

case "$BENCH" in
  tpch)
    PGDB="${PGDB:-tpch_sf10}"; QDIR="$REPORT_DIR/../tpch/queries"
    QUERIES="${QUERIES:-$(seq 1 22)}"
    TABLES="lineitem orders customer part supplier nation region partsupp"
    CG=/sys/fs/cgroup/pgch_mm_tpch; USE_MAXTHREADS=""; OFF_MPWPG=16; OFF_PROD_DIV=2 ;;
  clickbench)
    PGDB="${PGDB:-clickbench}"; QDIR="$REPORT_DIR/../clickbench/queries"
    QUERIES="${QUERIES:-$(seq 1 43)}"
    TABLES="hits"
    CG=/sys/fs/cgroup/pgch_mm_cb; MT="${MT:-16}"; USE_MAXTHREADS="$MT"; OFF_MPWPG="$MT"; OFF_PROD_DIV=1 ;;
  *) echo "bad BENCH=$BENCH" >&2; exit 1 ;;
esac
OUT="${OUT:-$REPORT_DIR/results/modematrix/$BENCH}"; mkdir -p "$OUT"
TMPD="$(mktemp -d)"
# shellcheck disable=SC1090
. "/home/ubuntu/ch-bench/$RUN_ID/manifest.env"
TICK=$(getconf CLK_TCK)
PSQL=(sudo -u postgres psql -d "$PGDB" -X -q -v ON_ERROR_STOP=0)
PSQLA=(sudo -u postgres psql -d "$PGDB" -tAqX -F'|' -v ON_ERROR_STOP=0)
PSQLT=(sudo -u postgres psql -d "$PGDB" -tAqX -F'|' -v ON_ERROR_STOP=0)
CHHTTP="http://$CH_HOST:$CH_HTTP_PORT/"
chq(){ curl -s --max-time 120 "$CHHTTP" --data-binary "$1"; }
CHPID=$(sudo ss -ltnp 2>/dev/null | grep ":$CH_HTTP_PORT " | grep -oP 'pid=\K[0-9]+' | head -1)
PMPID=$(sudo head -1 /var/lib/postgresql/18/main/postmaster.pid)
PERIOD=100000
[ -n "$CHPID" ] && [ -n "$PMPID" ] || { echo "could not resolve CH/PM pids" >&2; exit 1; }
echo "BENCH=$BENCH db=$PGDB pm=$PMPID ch=$CHPID N=$N K=$K W=$W nq=$(echo $QUERIES|wc -w) modes='$MODES'"

PM_CG=$(sudo cat /proc/$PMPID/cgroup | cut -d: -f3)
CH_CG=$(sudo cat /proc/$CHPID/cgroup | cut -d: -f3)
restore(){
  echo "$PMPID" | sudo tee "/sys/fs/cgroup${PM_CG}/cgroup.procs" >/dev/null 2>&1 || true
  echo "$CHPID" | sudo tee "/sys/fs/cgroup${CH_CG}/cgroup.procs" >/dev/null 2>&1 || true
  for t in $TABLES; do "${PSQLA[@]}" -c "SET search_path=pg; ALTER TABLE $t RESET (parallel_workers);" >/dev/null 2>&1 || true; done
  sudo rmdir "$CG" 2>/dev/null || true
  rm -rf "$TMPD" 2>/dev/null || true
}
trap restore EXIT
grep -qw cpu /sys/fs/cgroup/cgroup.subtree_control || echo +cpu | sudo tee /sys/fs/cgroup/cgroup.subtree_control >/dev/null
sudo mkdir -p "$CG"
echo "$PMPID" | sudo tee "$CG/cgroup.procs" >/dev/null
echo "$CHPID" | sudo tee "$CG/cgroup.procs" >/dev/null
echo "$((W*PERIOD)) $PERIOD" | sudo tee "$CG/cpu.max" >/dev/null

logsz(){ sudo stat -c %s "$LOGF" 2>/dev/null || echo 0; }
new_tag(){ printf 'mm_%s_%s_%s_%s' "$BENCH" "$1" "$$" "${RANDOM}${RANDOM}" | tr -cd 'a-z0-9_'; }
set_pw(){ local t s="SET search_path=pg;"; for t in $TABLES; do s="$s ALTER TABLE $t SET (parallel_workers=$1);"; done; "${PSQLA[@]}" -c "$s" >/dev/null 2>&1; }

native_set(){ cat <<SQL
SET search_path=pg;
SET pg_clickhouse.enable_shm_offload=off;
SET max_parallel_workers=64;
SET max_parallel_workers_per_gather=$1;
SET parallel_leader_participation=on;
SET min_parallel_table_scan_size=0;
SET parallel_setup_cost=0; SET parallel_tuple_cost=0.01;
SET work_mem='${WORKMEM:-2GB}'; SET hash_mem_multiplier=${HASHMEM_MULT:-4}; SET jit=on; SET jit_above_cost=0;
SET statement_timeout='300s';
SQL
}
# $1=tag $2=transport $3=tcp_send_method(maybe empty) $4=stats(0/1)
offload_set(){
  local ss="join_use_nulls 1, group_by_use_nulls 1, final 1, allow_experimental_streamed_table_function 1"
  [ -n "$USE_MAXTHREADS" ] && ss="$ss, max_threads $USE_MAXTHREADS"
  ss="$ss, log_comment $1"
  echo "LOAD 'pg_clickhouse';"
  echo "SET search_path=pg;"
  echo "SET pg_clickhouse.local_ch_server='ch_bench';"
  echo "SET pg_clickhouse.shm_min_rows=0;"
  echo "SET pg_clickhouse.session_settings='$ss';"
  echo "SET pg_clickhouse.enable_shm_offload=on;"
  echo "SET pg_clickhouse.shm_transport_mode='$2';"
  [ -n "$3" ] && echo "SET pg_clickhouse.tcp_send_method='$3';"
  [ "${4:-0}" = 1 ] && echo "SET pg_clickhouse.shm_log_stream_stats=on;"
  echo "SET max_parallel_workers=64; SET max_parallel_workers_per_gather=$OFF_MPWPG;"
  echo "SET statement_timeout='300s';"
}

# One timed run -> "wall_s". $1=SET block $2=SQL
measure(){ local s0 s1
  s0=$(date +%s.%N)
  printf '%s\n%s;\n' "$1" "$2" | "${PSQL[@]}" >/dev/null 2>&1
  s1=$(date +%s.%N)
  awk -v s0=$s0 -v s1=$s1 'BEGIN{printf "%.4f", s1-s0}'
}
# Fold a new sample into a running (min,max). $1=cur_min(or "") $2=cur_max(or "") $3=new -> "min max"
upd_minmax(){ awk -v a="$1" -v b="$2" -v c="$3" \
  'BEGIN{ mn=(a==""||c+0<a+0)?c:a; mx=(b==""||c+0>b+0)?c:b; printf "%.4f %.4f", mn, mx }'; }

# Sum the producer "shm phase" fields over all worker lines on stdin ->
# "rd_c df_c pb_c st_c rd_w df_w pb_w st_w nlines"  (all ms)
parse_phase(){ awk '
  /shm phase/{ nl++
    for(i=1;i<=NF;i++){ split($i,a,"="); v=a[2]+0;
      if($i ~ /^read_cpu=/) rc+=v; else if($i ~ /^deform_cpu=/) dc+=v;
      else if($i ~ /^publish_cpu=/) pc+=v; else if($i ~ /^stall_cpu=/) sc+=v;
      else if($i ~ /^read_wall=/) rw+=v; else if($i ~ /^deform_wall=/) dw+=v;
      else if($i ~ /^publish_wall=/) pw+=v; else if($i ~ /^stall_wall=/) sw+=v; } }
  END{ printf "%.1f\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f\t%d", rc,dc,pc,sc,rw,dw,pw,sw,nl }'; }

# Sum the producer "shm tcp-send" counters over all worker lines on stdin ->
# "method epoll blk zc_sends zc_copied K pumps overlap nlines"  (method from first line, K=max)
parse_tcp(){ awk '
  /shm tcp-send/{ nl++
    for(i=1;i<=NF;i++){ split($i,a,"="); key=a[1]; v=a[2];
      if(key=="method" && method=="") method=v;
      else if(key=="epoll_sends") ep+=v+0; else if(key=="blocking_sends") bl+=v+0;
      else if(key=="zc_sends") zs+=v+0; else if(key=="zc_copied") zc+=v+0;
      else if(key=="K"){ if(v+0>kk) kk=v+0 } else if(key=="pumps") pu+=v+0;
      else if(key=="overlap_frames") ov+=v+0; } }
  END{ if(method=="") method="-"; printf "%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d", method,ep,bl,zs,zc,kk,pu,ov,nl }'; }

# CH consumer metrics for a tag -> "consms read_rows user_us sys_us copytime_us adopted_bytes copied_bytes nfin nexc"
ch_metrics(){ local tag="$1" r
  for _t in $(seq 1 10); do
    chq "SYSTEM FLUSH LOGS" >/dev/null
    r=$(chq "SELECT maxIf(query_duration_ms,type='QueryFinish'), sumIf(read_rows,type='QueryFinish'), sumIf(ProfileEvents['UserTimeMicroseconds'],type='QueryFinish'), sumIf(ProfileEvents['SystemTimeMicroseconds'],type='QueryFinish'), sumIf(ProfileEvents['ShmCopyTimeMicroseconds'],type='QueryFinish'), sumIf(ProfileEvents['ShmAdoptedBytesCharged'],type='QueryFinish'), sumIf(ProfileEvents['ShmCopiedBytesCharged'],type='QueryFinish'), countIf(type='QueryFinish'), countIf(type='ExceptionWhileProcessing') FROM system.query_log WHERE log_comment='$tag' AND positionCaseInsensitive(query,'streamed_table')>0 AND positionCaseInsensitive(query,'query_log')=0")
    [ "$(echo "$r" | cut -f8)" -ge 1 ] 2>/dev/null && break
    [ "$(echo "$r" | cut -f9)" -ge 1 ] 2>/dev/null && break
    sleep 0.5
  done
  echo "$r" | tr '\t' ' '
}

# plan has the offload customscan? (transport-independent gate)
has_customscan(){ local sql="$1" tag; tag=$(new_tag pl)
  printf '%s\nEXPLAIN (COSTS OFF)\n%s;\n' "$(offload_set "$tag" adopt "" 0)" "$sql" \
    | "${PSQLA[@]}" 2>/dev/null | grep -q 'ClickHouseShmScan'; }

# ONE validation run per mode: captures offload output (cmp vs prebuilt nat.out) AND polls the
# block oracle. Echoes "blocks rows correctverdict". $1=sql $2=transport $3=method
validate_mode(){ local sql="$1" tr="$2" me="$3" tag; tag=$(new_tag val)
  printf '%s\n%s;\n' "$(offload_set "$tag" "$tr" "$me" 0)" "$sql" | "${PSQLT[@]}" > "$TMPD/off.out" 2>"$TMPD/off.err"
  local corr
  if grep -qiE 'error|fatal' "$TMPD/off.err"; then corr="OFFLOAD-ERR($(head -1 "$TMPD/off.err" | cut -c1-30))";
  else corr=$(python3 "$REPORT_DIR/cmp.py" "$TMPD/nat.out" "$TMPD/off.out" 2>/dev/null || echo "cmp-err"); fi
  local m blk=0
  for _t in $(seq 1 12); do
    chq "SYSTEM FLUSH LOGS" >/dev/null
    m=$(chq "SELECT sum(ProfileEvents['ShmAdoptedBlocks'] + ProfileEvents['ShmCopiedBlocks']), sum(read_rows) FROM system.query_log WHERE log_comment='$tag' AND type='QueryFinish' AND positionCaseInsensitive(query,'streamed_table')>0 AND positionCaseInsensitive(query,'query_log')=0")
    blk=$(echo "$m" | cut -f1); blk=${blk:-0}
    [ "${blk:-0}" -ge 1 ] 2>/dev/null && break
    sleep 0.5
  done
  local rows; rows=$(echo "$m" | cut -f2); rows=${rows:-0}
  [ "${blk:-0}" -ge 1 ] 2>/dev/null && echo "$blk $rows $corr" || echo "0 0 $corr"
}

TSV="$OUT/matrix.tsv"
printf 'bench\tq\tmode\ttransport\tmethod\telig_blocks\telig_rows\tnat_min_ms\tnat_max_ms\toff_min_ms\toff_max_ms\tspeedup\tcorrect\tp_read_wall\tp_deform_wall\tp_publish_wall\tp_stall_wall\tp_read_cpu\tp_deform_cpu\tp_publish_cpu\tp_stall_cpu\tcons_ms\tcons_user_us\tcons_sys_us\tcons_copytime_us\tcons_adopt_bytes\tcons_copied_bytes\ttx_method\ttx_epoll\ttx_blocking\ttx_zc_sends\ttx_zc_copied\ttx_K\ttx_pumps\ttx_overlap\n' > "$TSV"
prod=$((W/OFF_PROD_DIV)); [ "$prod" -lt 1 ] && prod=1
# 22 instrumented fields: 8 phase + 6 consumer + (tx_method=-) + 7 tx counts
EMPTY_INSTR="0 0 0 0 0 0 0 0 0 0 0 0 0 0 - 0 0 0 0 0 0 0"

emit_excl(){ # $1=label $2=tr $3=me $4=corr ; uses $nmin/$nmax (seconds)
  printf '%s\t%s\t%s\t%s\t%s\t0\t0\t%.0f\t%.0f\t-\t-\t-\t%s\t' "$BENCH" "$q" "$1" "$2" "$3" \
    "$(awk -v x=$nmin 'BEGIN{print x*1000}')" "$(awk -v x=$nmax 'BEGIN{print x*1000}')" "${4:-no-block}" >> "$TSV"
  echo "$EMPTY_INSTR" | tr ' ' '\t' >> "$TSV"
}

for q in $QUERIES; do
  qf="$QDIR/$q.sql"; [ -f "$qf" ] || { echo "Q$q: no file"; continue; }
  sql="$(sed -e '/^EXPLAIN/d' -e '/^[[:space:]]*--/d' "$qf" | sed 's/;[[:space:]]*$//')"
  if ! has_customscan "$sql"; then
    echo "Q$q  EXCLUDED (no ClickHouseShmScan in plan; native-only)"
    printf '%s\t%s\tNATIVE-ONLY\t-\t-\t0\t0\t-\t-\t-\t-\t-\tno-customscan\t' "$BENCH" "$q" >> "$TSV"
    echo "$EMPTY_INSTR" | tr ' ' '\t' >> "$TSV"
    continue
  fi
  # optimal native: N runs (run1 captures nat.out for correctness), take MIN (ClickBench best-of-N).
  # The cold first run is naturally rejected by min; no separate warmup needed.
  set_pw "$W"
  nmin=""; nmax=""
  for r in $(seq 1 "$N"); do
    if [ "$r" = 1 ]; then
      s0=$(date +%s.%N); printf '%s\n%s;\n' "$(native_set $W)" "$sql" | "${PSQLT[@]}" > "$TMPD/nat.out" 2>/dev/null; s1=$(date +%s.%N)
      w=$(awk -v a=$s0 -v b=$s1 'BEGIN{printf "%.4f",b-a}')
    else
      w=$(measure "$(native_set $W)" "$sql")
    fi
    read nmin nmax <<<"$(upd_minmax "$nmin" "$nmax" "$w")"
  done
  printf 'Q%-3s native=%.0fms  ' "$q" "$(awk -v x=$nmin 'BEGIN{print x*1000}')"
  # each mode: N CLEAN (stats-off) runs -> min/speedup (run1 also = correctness+oracle+warmup);
  # then ONE stats-ON run for the phase split + tcp counters + consumer metrics (kept OUT of the
  # timing because shm_log_stream_stats adds ~tens-of-% on scan-bound queries -> would bias speedup).
  set_pw "$prod"
  for spec in $MODES; do
    label="${spec%%:*}"; rest="${spec#*:}"; tr="${rest%%:*}"; me="${rest#*:}"
    omin=""; omax=""
    for r in $(seq 1 "$N"); do
      tag=$(new_tag run); s0=$(date +%s.%N)
      if [ "$r" = 1 ]; then
        printf '%s\n%s;\n' "$(offload_set $tag "$tr" "$me" 0)" "$sql" | "${PSQLT[@]}" > "$TMPD/off.out" 2>"$TMPD/off.err"
      else
        printf '%s\n%s;\n' "$(offload_set $tag "$tr" "$me" 0)" "$sql" | "${PSQL[@]}" >/dev/null 2>&1
      fi
      s1=$(date +%s.%N); w=$(awk -v a=$s0 -v b=$s1 'BEGIN{printf "%.4f",b-a}')
      read omin omax <<<"$(upd_minmax "$omin" "$omax" "$w")"
      if [ "$r" = 1 ]; then
        # correctness (run1 output) + block oracle; early-exit if this mode does not offload
        if grep -qiE 'error|fatal' "$TMPD/off.err"; then corr="OFFLOAD-ERR($(head -1 "$TMPD/off.err" | cut -c1-30))";
        else corr=$(python3 "$REPORT_DIR/cmp.py" "$TMPD/nat.out" "$TMPD/off.out" 2>/dev/null || echo "cmp-err"); fi
        blk=0; m=""
        for _t in $(seq 1 12); do
          chq "SYSTEM FLUSH LOGS" >/dev/null
          m=$(chq "SELECT sum(ProfileEvents['ShmAdoptedBlocks'] + ProfileEvents['ShmCopiedBlocks']), sum(read_rows) FROM system.query_log WHERE log_comment='$tag' AND type='QueryFinish' AND positionCaseInsensitive(query,'streamed_table')>0 AND positionCaseInsensitive(query,'query_log')=0")
          blk=$(echo "$m" | cut -f1); blk=${blk:-0}
          [ "${blk:-0}" -ge 1 ] 2>/dev/null && break
          sleep 0.5
        done
        rows=$(echo "$m" | cut -f2); rows=${rows:-0}
        if [ "${blk:-0}" -lt 1 ] 2>/dev/null; then
          printf '%s=EXCL ' "$label"; emit_excl "$label" "$tr" "$me" "$corr"; continue 2
        fi
      fi
    done
    sp=$(awk -v n=$nmin -v o=$omin 'BEGIN{printf "%.3f",(o>0)?n/o:0}')
    printf '%s=%.0fms(%sx) ' "$label" "$(awk -v x=$omin 'BEGIN{print x*1000}')" "$sp"
    # ONE stats-ON run for the producer phase split + tcp counters (NOT timed into the min)
    ptag=$(new_tag isp); o0=$(logsz)
    printf '%s\n%s;\n' "$(offload_set $ptag "$tr" "$me" 1)" "$sql" | "${PSQL[@]}" >/dev/null 2>&1
    best_ph=$(sudo tail -c +$((o0+1)) "$LOGF" 2>/dev/null | parse_phase)
    best_tx=$(sudo tail -c +$((o0+1)) "$LOGF" 2>/dev/null | parse_tcp)
    p_rdc=$(echo "$best_ph"|cut -f1); p_dfc=$(echo "$best_ph"|cut -f2); p_pbc=$(echo "$best_ph"|cut -f3); p_stc=$(echo "$best_ph"|cut -f4)
    p_rdw=$(echo "$best_ph"|cut -f5); p_dfw=$(echo "$best_ph"|cut -f6); p_pbw=$(echo "$best_ph"|cut -f7); p_stw=$(echo "$best_ph"|cut -f8)
    tx_m=$(echo "$best_tx"|cut -f1); tx_ep=$(echo "$best_tx"|cut -f2); tx_bl=$(echo "$best_tx"|cut -f3)
    tx_zs=$(echo "$best_tx"|cut -f4); tx_zc=$(echo "$best_tx"|cut -f5); tx_k=$(echo "$best_tx"|cut -f6)
    tx_pu=$(echo "$best_tx"|cut -f7); tx_ov=$(echo "$best_tx"|cut -f8)
    cm=$(ch_metrics "$ptag")
    cons_ms=$(echo "$cm"|cut -d' ' -f1); cons_uu=$(echo "$cm"|cut -d' ' -f3); cons_su=$(echo "$cm"|cut -d' ' -f4)
    cons_ct=$(echo "$cm"|cut -d' ' -f5); cons_ab=$(echo "$cm"|cut -d' ' -f6); cons_cb=$(echo "$cm"|cut -d' ' -f7)
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%.0f\t%.0f\t%.0f\t%.0f\t%s\t%s\t' \
      "$BENCH" "$q" "$label" "$tr" "$me" "${blk:-0}" "${rows:-0}" \
      "$(awk -v x=$nmin 'BEGIN{print x*1000}')" "$(awk -v x=$nmax 'BEGIN{print x*1000}')" \
      "$(awk -v x=$omin 'BEGIN{print x*1000}')" "$(awk -v x=$omax 'BEGIN{print x*1000}')" "$sp" "$corr" >> "$TSV"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
      "${p_rdw:-0}" "${p_dfw:-0}" "${p_pbw:-0}" "${p_stw:-0}" "${p_rdc:-0}" "${p_dfc:-0}" "${p_pbc:-0}" "${p_stc:-0}" \
      "${cons_ms:-0}" "${cons_uu:-0}" "${cons_su:-0}" "${cons_ct:-0}" "${cons_ab:-0}" "${cons_cb:-0}" \
      "${tx_m:--}" "${tx_ep:-0}" "${tx_bl:-0}" "${tx_zs:-0}" "${tx_zc:-0}" "${tx_k:-0}" "${tx_pu:-0}" "${tx_ov:-0}" >> "$TSV"
  done
  echo ""
  for t in $TABLES; do "${PSQLA[@]}" -c "SET search_path=pg; ALTER TABLE $t RESET (parallel_workers);" >/dev/null 2>&1; done
done
echo "DONE BENCH=$BENCH TSV=$TSV"
