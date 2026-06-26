#!/usr/bin/env bash
# Unified native-vs-offload W-sweep + producer-phase-split harness (wsweep-report).
#
# Reuses the proven shared-cgroup-cap methodology of dev/{tpch,clickbench}/wsweep.sh
# (both the PG postmaster tree AND the live ClickHouse server in ONE cgroup v2 capped
# to cpu.max = W*period; measuring shell outside) and ADDS, per (query, W):
#   - clean headline wall (instrumentation OFF): native vs offload, N warm runs,
#     off_prod/off_cons cores from whole-host /proc/stat (host-minus-CH = producer).
#   - producer phase split (READ/DEFORM/PUBLISH/STALL) from K instrumented offload
#     runs (shm_log_stream_stats=on -> "shm phase" LOG lines, summed across workers).
#   - consumer wall + CPU from the CH query_log (heavy streamed_table fragment).
#   - oracles: offload (ShmAdoptedBlocks>=1 + ClickHouseShmScan), liveness (QueryFinish,
#     no exception, read_rows == full/stable), correctness (offload==native, bounded dev).
# Emits a machine-readable TSV (results/<bench>/cells.tsv) + a human MD (RESULTS.md).
#
#   BENCH=tpch       QUERIES="1 3 4 5 6 7 8 9 10 11 12 14 19" W_LIST="1 2 4 8" N=5 K=3 dev/wsweep-report/wsweep_split.sh
#   BENCH=clickbench QUERIES="$(seq 2 43)"                    W_LIST="1 2 4 8" N=5 K=3 dev/wsweep-report/wsweep_split.sh
set -uo pipefail

BENCH="${BENCH:?set BENCH=tpch|clickbench}"
N="${N:-5}"
K="${K:-3}"
W_LIST="${W_LIST:-1 2 4 8}"
RUN_ID="${RUN_ID:-tpchcb}"
# Hot-Cold transport dimension: adopt (default, zero-copy) | copy. Threaded into the
# offload SET block; the offload oracle counts ShmAdopted+ShmCopied blocks so both modes
# are eligible. Set OUT per mode (e.g. results/copy/$BENCH) to keep cells.tsv separate.
TRANSPORT="${TRANSPORT:-adopt}"
REPORT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOGF=/var/log/postgresql/postgresql-18-main.log

case "$BENCH" in
  tpch)
    PGDB="${PGDB:-tpch_sf10}"; QDIR="$REPORT_DIR/../tpch/queries"
    QUERIES="${QUERIES:-1 3 4 5 6 7 8 9 10 11 12 14 19}"
    TABLES="lineitem orders customer part supplier nation region partsupp"
    CG=/sys/fs/cgroup/pgch_rep_tpch; USE_MAXTHREADS=""; OFF_MPWPG=16; OFF_PROD_DIV=2 ;;
  clickbench)
    PGDB="${PGDB:-clickbench}"; QDIR="$REPORT_DIR/../clickbench/queries"
    QUERIES="${QUERIES:-$(seq 2 43)}"
    TABLES="hits"
    CG=/sys/fs/cgroup/pgch_rep_cb; MT="${MT:-16}"; USE_MAXTHREADS="$MT"; OFF_MPWPG="$MT"; OFF_PROD_DIV=1 ;;
  *) echo "bad BENCH=$BENCH" >&2; exit 1 ;;
esac
OUT="${OUT:-$REPORT_DIR/results/$BENCH}"; mkdir -p "$OUT"
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
echo "BENCH=$BENCH db=$PGDB pm=$PMPID ch=$CHPID N=$N K=$K W='$W_LIST' transport=$TRANSPORT nq=$(echo $QUERIES|wc -w)"

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

hb(){ awk '/^cpu /{print $2+$3+$4+$7+$8+$9}' /proc/stat; }
cc(){ awk '{s=$0;sub(/^[^)]*\) /,"",s);split(s,f," ");print f[12]+f[13]}' /proc/"$1"/stat 2>/dev/null || echo 0; }
logsz(){ sudo stat -c %s "$LOGF" 2>/dev/null || echo 0; }
new_tag(){ printf 'rep_%s_%s_%s_%s' "$BENCH" "$1" "$$" "${RANDOM}${RANDOM}" | tr -cd 'a-z0-9_'; }
set_pw(){ local t s="SET search_path=pg;"; for t in $TABLES; do s="$s ALTER TABLE $t SET (parallel_workers=$1);"; done; "${PSQLA[@]}" -c "$s" >/dev/null 2>&1; }

native_set(){ cat <<SQL
SET search_path=pg;
SET pg_clickhouse.enable_shm_offload=off;
SET max_parallel_workers=64;
SET max_parallel_workers_per_gather=$1;
SET parallel_leader_participation=on;
SET min_parallel_table_scan_size=0;
SET parallel_setup_cost=0; SET parallel_tuple_cost=0.01;
SET work_mem='2GB'; SET hash_mem_multiplier=4; SET jit=on; SET jit_above_cost=0;
SET statement_timeout='300s';
SQL
}
# $1=tag $2=stats(0/1)
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
  echo "SET pg_clickhouse.shm_transport_mode='$TRANSPORT';"
  [ "$2" = 1 ] && echo "SET pg_clickhouse.shm_log_stream_stats=on;"
  echo "SET max_parallel_workers=64; SET max_parallel_workers_per_gather=$OFF_MPWPG;"
  echo "SET statement_timeout='300s';"
}

# One timed run -> "wall_s host_cpu_s ch_cpu_s". $1=SET block $2=SQL
measure(){ local h0 k0 s0 s1 h1 k1
  h0=$(hb); k0=$(cc "$CHPID"); s0=$(date +%s.%N)
  printf '%s\n%s;\n' "$1" "$2" | "${PSQL[@]}" >/dev/null 2>&1
  s1=$(date +%s.%N); h1=$(hb); k1=$(cc "$CHPID")
  awk -v h0=$h0 -v h1=$h1 -v k0=$k0 -v k1=$k1 -v s0=$s0 -v s1=$s1 -v t=$TICK \
    'BEGIN{printf "%.4f %.3f %.3f", s1-s0, (h1-h0)/t, (k1-k0)/t}'
}
stats(){ awk '{w[NR]=$1; ch[NR]=$3; ho[NR]=$2}
  END{n=NR; if(n==0){print "0 0 0 0 0 0"; exit}
      for(i=1;i<=n;i++)for(j=i+1;j<=n;j++){if(w[j]<w[i]){t=w[i];w[i]=w[j];w[j]=t;t=ch[i];ch[i]=ch[j];ch[j]=t;t=ho[i];ho[i]=ho[j];ho[j]=t}}
      med=w[int((n+1)/2)]; mn=w[1]; mx=w[n]; s=0;ss=0; for(i=1;i<=n;i++){s+=w[i];ss+=w[i]*w[i]} mean=s/n; sd=sqrt((ss/n-mean*mean>0)?ss/n-mean*mean:0);
      printf "%.4f %.4f %.4f %.4f %.3f %.3f", med, mn, mx, sd, ho[int((n+1)/2)], ch[int((n+1)/2)]}'; }

# Offload oracle -> "blocks<TAB>read_rows" or "NO(reason)". Poll-retry async flush.
check_eligible(){ local sql="$1" tag; tag=$(new_tag elig)
  local plan; plan=$(printf '%s\nEXPLAIN (COSTS OFF)\n%s;\n' "$(offload_set "$tag" 0)" "$sql" | "${PSQLA[@]}" 2>/dev/null)
  echo "$plan" | grep -q 'ClickHouseShmScan' || { echo "NO(no-customscan)"; return; }
  printf '%s\n%s;\n' "$(offload_set "$tag" 0)" "$sql" | "${PSQL[@]}" >/dev/null 2>&1
  local m blk=0
  for _t in $(seq 1 12); do
    chq "SYSTEM FLUSH LOGS" >/dev/null
    m=$(chq "SELECT sum(ProfileEvents['ShmAdoptedBlocks'] + ProfileEvents['ShmCopiedBlocks']), sum(read_rows) FROM system.query_log WHERE log_comment='$tag' AND type='QueryFinish' AND positionCaseInsensitive(query,'streamed_table')>0 AND positionCaseInsensitive(query,'query_log')=0")
    blk=$(echo "$m" | cut -f1); blk=${blk:-0}
    [ "${blk:-0}" -ge 1 ] 2>/dev/null && break
    sleep 0.5
  done
  [ "${blk:-0}" -ge 1 ] 2>/dev/null && echo "$m" || echo "NO(no-block)"
}

# Correctness (once/query, uncapped): native vs offload value-compare. echo verdict.
correctness(){ local sql="$1" tag; tag=$(new_tag corr)
  printf '%s\n%s;\n' "$(native_set 8)" "$sql" | "${PSQLT[@]}" > "$TMPD/nat.out" 2>"$TMPD/nat.err"
  printf '%s\n%s;\n' "$(offload_set "$tag" 0)" "$sql" | "${PSQLT[@]}" > "$TMPD/off.out" 2>"$TMPD/off.err"
  if grep -qiE 'error|fatal' "$TMPD/off.err"; then echo "OFFLOAD-ERR($(head -1 "$TMPD/off.err" | cut -c1-40))"; return; fi
  python3 "$REPORT_DIR/cmp.py" "$TMPD/nat.out" "$TMPD/off.out" 2>/dev/null || echo "cmp-err"
}

# CH consumer metrics for a tag -> "consumer_ms read_rows user_us sys_us nfin nexc"
ch_metrics(){ local tag="$1" r
  for _t in $(seq 1 10); do
    chq "SYSTEM FLUSH LOGS" >/dev/null
    r=$(chq "SELECT maxIf(query_duration_ms,type='QueryFinish'), sumIf(read_rows,type='QueryFinish'), sumIf(ProfileEvents['UserTimeMicroseconds'],type='QueryFinish'), sumIf(ProfileEvents['SystemTimeMicroseconds'],type='QueryFinish'), countIf(type='QueryFinish'), countIf(type='ExceptionWhileProcessing') FROM system.query_log WHERE log_comment='$tag' AND positionCaseInsensitive(query,'streamed_table')>0 AND positionCaseInsensitive(query,'query_log')=0")
    [ "$(echo "$r" | cut -f5)" -ge 1 ] 2>/dev/null && break
    [ "$(echo "$r" | cut -f6)" -ge 1 ] 2>/dev/null && break
    sleep 0.5
  done
  echo "$r" | tr '\t' ' '
}

# Parse summed phase fields from captured "shm phase" lines on stdin ->
# "rd_cpu df_cpu pb_cpu st_cpu rd_w df_w pb_w st_w nlines"
parse_phase(){ awk '
  /shm phase/{ nl++
    for(i=1;i<=NF;i++){ split($i,a,"="); v=a[2]+0;
      if($i ~ /^read_cpu=/) rc+=v; else if($i ~ /^deform_cpu=/) dc+=v;
      else if($i ~ /^publish_cpu=/) pc+=v; else if($i ~ /^stall_cpu=/) sc+=v;
      else if($i ~ /^read_wall=/) rw+=v; else if($i ~ /^deform_wall=/) dw+=v;
      else if($i ~ /^publish_wall=/) pw+=v; else if($i ~ /^stall_wall=/) sw+=v; } }
  END{ printf "%.1f %.1f %.1f %.1f %.1f %.1f %.1f %.1f %d", rc,dc,pc,sc,rw,dw,pw,sw,nl }'; }

TSV="$OUT/cells.tsv"; MD="$OUT/RESULTS.md"
HDR="bench\tq\tW\telig_blocks\telig_rows\tnat_med_ms\tnat_min\tnat_max\tnat_sd\tnat_cores\toff_med_ms\toff_min\toff_max\toff_sd\toff_prod\toff_cons\tspeedup\trd_cpu\tdf_cpu\tpb_cpu\tst_cpu\trd_w\tdf_w\tpb_w\tst_w\tprod_cpu_sum\tinst_wall_ms\tinst_offprod\tinst_offcons\tconsumer_ms\tcons_user_us\tcons_sys_us\tread_rows\tnfin\tnexc\tlive\tcorrect"
printf "%b\n" "$HDR" > "$TSV"
{
  echo "# $BENCH native-vs-offload W-sweep + producer phase split"
  echo ""
  echo "db=$PGDB N=$N warm K=$K instrumented  W in {$W_LIST}  pm=$PMPID ch=$CHPID  $( [ -n "$USE_MAXTHREADS" ] && echo "MT=$USE_MAXTHREADS")"
  echo "cap: one cgroup ($CG) over both trees, cpu.max=W*${PERIOD}us. cores=CPU-s/wall (host-CH=prod, CH=cons)."
  echo "phase cores = off_prod apportioned by in-code CPU fraction; within-prod wall% excludes stall."
  echo ""
} > "$MD"

for q in $QUERIES; do
  qf="$QDIR/$q.sql"; [ -f "$qf" ] || { echo "Q$q: no file"; continue; }
  sql="$(sed -e '/^EXPLAIN/d' -e '/^[[:space:]]*--/d' "$qf" | sed 's/;[[:space:]]*$//')"
  elig=$(check_eligible "$sql")
  if ! echo "$elig" | grep -qE '^[0-9]'; then
    echo "Q$q  EXCLUDED: offload oracle = $elig"
    { echo ""; echo "## Q$q — EXCLUDED (offload oracle: $elig)"; } >> "$MD"
    printf "%s\t%s\t-\t0\t0\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\tNA\toracle:%s\n" "$BENCH" "$q" "$elig" >> "$TSV"
    continue
  fi
  eblk=$(echo "$elig" | cut -f1); erows=$(echo "$elig" | cut -f2)
  corr=$(correctness "$sql")
  echo "Q$q  eligible(blocks=$eblk rows=$erows)  correct=$corr"
  { echo ""; echo "## Q$q  (blocks=$eblk read_rows=$erows  correct=$corr)";
    printf '\n| W | nat_ms(min/max,sd) | off_ms(min/max,sd) | spdup | off_prod[rd/df/pb] | wall%%[r:d:p](+stall) | off_cons | cons_ms | live |\n'
    printf -- '|--:|---|---|--:|---|---|--:|--:|:--:|\n'; } >> "$MD"

  for W in $W_LIST; do
    echo "$((W*PERIOD)) $PERIOD" | sudo tee "$CG/cpu.max" >/dev/null
    # NATIVE: parallel_workers=W
    set_pw "$W"
    printf '%s\n%s;\n' "$(native_set $W)" "$sql" | "${PSQL[@]}" >/dev/null 2>&1
    nat=$(for _ in $(seq 1 "$N"); do measure "$(native_set $W)" "$sql"; echo; done | stats)
    # OFFLOAD headline (stats OFF): parallel_workers = W/OFF_PROD_DIV
    prod=$((W/OFF_PROD_DIV)); [ "$prod" -lt 1 ] && prod=1
    set_pw "$prod"
    otag=$(new_tag run)
    printf '%s\n%s;\n' "$(offload_set ${otag}w 0)" "$sql" | "${PSQL[@]}" >/dev/null 2>&1
    off=$(for _ in $(seq 1 "$N"); do measure "$(offload_set $otag 0)" "$sql"; echo; done | stats)

    # INSTRUMENTED split + oracle: K runs, capture phase LOG + /proc cores + CH metrics
    : > "$TMPD/runs.tsv"
    for kk in $(seq 1 "$K"); do
      itag=$(new_tag isp)
      o0=$(logsz); h0=$(hb); c0=$(cc "$CHPID"); s0=$(date +%s.%N)
      printf '%s\n%s;\n' "$(offload_set $itag 1)" "$sql" | "${PSQL[@]}" >/dev/null 2>&1
      s1=$(date +%s.%N); h1=$(hb); c1=$(cc "$CHPID")
      ph=$(sudo tail -c +$((o0+1)) "$LOGF" 2>/dev/null | parse_phase)
      cm=$(ch_metrics "$itag")
      awk -v ph="$ph" -v cm="$cm" -v h0=$h0 -v h1=$h1 -v c0=$c0 -v c1=$c1 -v s0=$s0 -v s1=$s1 -v t=$TICK 'BEGIN{
        wall=s1-s0; op=(wall>0)?(h1-h0-(c1-c0))/t/wall:0; oc=(wall>0)?(c1-c0)/t/wall:0;
        printf "%.4f %s %.3f %.3f %s\n", wall, ph, op, oc, cm; }' >> "$TMPD/runs.tsv"
    done
    # pick median run by wall (field1)
    med=$(sort -k1 -g "$TMPD/runs.tsv" | awk -v k="$K" 'NR==int((k+1)/2)')
    # fields of med: 1 wall  2 rd_cpu 3 df_cpu 4 pb_cpu 5 st_cpu 6 rd_w 7 df_w 8 pb_w 9 st_w 10 nlines 11 op 12 oc  13 consms 14 rrows 15 uus 16 sus 17 nfin 18 nexc
    read iwall rd df pb st rw dw pw sw nlines iop ioc consms rrows uus sus nfin nexc <<<"$med"
    # liveness: QueryFinish>=1, no exception, read_rows matches eligibility (full fresh read)
    live="yes"
    [ "${nfin:-0}" -ge 1 ] 2>/dev/null || live="no(no-finish)"
    [ "${nexc:-0}" -eq 0 ] 2>/dev/null || live="no(exception)"
    if [ "$live" = "yes" ] && [ "${rrows:-0}" != "${erows}" ]; then live="warn(rows $rrows!=$erows)"; fi
    # emit TSV (raw)
    printf "%s\t%s\t%s\t%s\t%s\t" "$BENCH" "$q" "$W" "$eblk" "$erows" >> "$TSV"
    echo "$nat $off" | awk -v OFS='\t' '{
      sp=($7>0)?$1/$7:0;
      printf "%.0f\t%.0f\t%.0f\t%.0f\t%.3f\t%.0f\t%.0f\t%.0f\t%.0f\t%.3f\t%.3f\t%.4f\t",
        $1*1000,$2*1000,$3*1000,$4*1000,($1>0?($5-$6)/$1:0),
        $7*1000,$8*1000,$9*1000,$10*1000,($7>0?($11-$12)/$7:0),($7>0?$12/$7:0),sp; }' >> "$TSV"
    printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
      "$rd" "$df" "$pb" "$st" "$rw" "$dw" "$pw" "$sw" \
      "$(awk -v a=$rd -v b=$df -v c=$pb -v d=$st 'BEGIN{printf "%.1f",a+b+c+d}')" \
      "$(awk -v w=$iwall 'BEGIN{printf "%.1f",w*1000}')" "$iop" "$ioc" \
      "${consms:-0}" "${uus:-0}" "${sus:-0}" "${rrows:-0}" "${nfin:-0}" "${nexc:-0}" "$live" "$corr" >> "$TSV"
    # MD row (derived split)
    awk -v W=$W -v nat="$nat" -v off="$off" -v rd=$rd -v df=$df -v pb=$pb -v st=$st \
        -v rw=$rw -v dw=$dw -v pw=$pw -v sw=$sw -v consms=${consms:-0} -v live="$live" 'BEGIN{
      split(nat,a," "); split(off,b," ");
      nmed=a[1]*1000;nmin=a[2]*1000;nmax=a[3]*1000;nsd=a[4]*1000;
      omed=b[1]*1000;omin=b[2]*1000;omax=b[3]*1000;osd=b[4]*1000;
      oprod=(b[1]>0)?(b[5]-b[6])/b[1]:0; ocons=(b[1]>0)?b[6]/b[1]:0; sp=(omed>0)?nmed/omed:0;
      tc=rd+df+pb+st; if(tc<=0)tc=1; rdc=oprod*rd/tc; dfc=oprod*df/tc; pbc=oprod*pb/tc;
      tw=rw+dw+pw; if(tw<=0)tw=1; rp=100*rw/tw; dp=100*dw/tw; pp=100*pw/tw;
      printf "| %d | %.0f(%.0f/%.0f,%.0f) | %.0f(%.0f/%.0f,%.0f) | %.2fx | %.2f[%.2f/%.2f/%.2f] | %.0f:%.0f:%.0f(+%.0fms) | %.2f | %.0f | %s |\n",
        W,nmed,nmin,nmax,nsd, omed,omin,omax,osd, sp, oprod,rdc,dfc,pbc, rp,dp,pp,sw, ocons, consms, live;
    }' >> "$MD"
  done
  for t in $TABLES; do "${PSQLA[@]}" -c "SET search_path=pg; ALTER TABLE $t RESET (parallel_workers);" >/dev/null 2>&1; done
done
echo "" >> "$MD"; echo "TSV: $TSV" >> "$MD"
echo "DONE. TSV=$TSV MD=$MD"
