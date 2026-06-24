#!/usr/bin/env bash
# Native-vs-offload W-sweep under a shared cgroup cpu.max cap, for any ClickBench
# query. Mirrors dev/tpch/wsweep.sh but for the single `hits` fact table.
#
# Both the PostgreSQL postmaster tree AND the co-located ClickHouse server are
# placed in ONE cgroup v2 capped to W cores via cpu.max (quota = W*period), so
# native and offload compete for the SAME core budget -- removing any "offload
# used more cores" confound. The measuring shell stays OUTSIDE the capped cgroup;
# cgroups are restored on exit.
#
# Per (query, engine, W): median of N>=5 WARM runs with min/max + stdev, plus
# measured cores from whole-host /proc/stat split into ClickHouse-server vs
# PostgreSQL (host-minus-CH; valid on an idle dedicated host). Eligibility is
# proven per query: the offload plan must contain Custom Scan (ClickHouseShmScan)
# AND the ClickHouse query_log oracle must fire (a streamed_table QueryFinish with
# ShmAdoptedBlocks>=1, correlated by a unique log_comment tag).
#
#   QUERIES="2 8 13" W_LIST="2 4 8 16" N=5 dev/clickbench/wsweep.sh
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
PGDB="${PGDB:-clickbench}"
N="${N:-5}"
W_LIST="${W_LIST:-2 4 8 16}"
QUERIES="${QUERIES:-2 8 13}"
RUN_ID="${RUN_ID:-tpchcb}"
# Offload producer parallelism: PROD workers scan pg.hits and fill SHM; the CH
# consumer runs the GROUP BY/aggregate at max_threads=MT. Both share the W-core
# cgroup cap, so these are parallelism opportunities, not a hard core split.
PROD="${PROD:-}"      # default: W (whole cap to the single-table producer)
MT="${MT:-16}"
QDIR="$HERE/queries"
OUT="${OUT:-$HERE/evidence/phase0/wsweep}"
mkdir -p "$OUT"
# shellcheck disable=SC1090
. "/home/ubuntu/ch-bench/$RUN_ID/manifest.env"   # CH_HOST CH_HTTP_PORT ...
TICK=$(getconf CLK_TCK)
PSQL=(sudo -u postgres psql -d "$PGDB" -X -q -v ON_ERROR_STOP=0)
PSQLA=(sudo -u postgres psql -d "$PGDB" -tAqX -F'|' -v ON_ERROR_STOP=0)
CHHTTP="http://$CH_HOST:$CH_HTTP_PORT/"
chq() { curl -s --max-time 120 "$CHHTTP" --data-binary "$1"; }
# Resolve the LIVE ClickHouse pid from the listening port (manifest pid is stale).
CHPID=$(sudo ss -ltnp 2>/dev/null | grep ":$CH_HTTP_PORT " | grep -oP 'pid=\K[0-9]+' | head -1)
PMPID=$(sudo head -1 /var/lib/postgresql/18/main/postmaster.pid)
CG=/sys/fs/cgroup/pgch_cbwsweep
PERIOD=100000
[ -n "$CHPID" ] && [ -n "$PMPID" ] || { echo "could not resolve CH/PM pids (CH=$CHPID PM=$PMPID)" >&2; exit 1; }
echo "postmaster=$PMPID clickhouse=$CHPID db=$PGDB N=$N W_LIST='$W_LIST' QUERIES='$QUERIES' MT=$MT"

PM_CG=$(sudo cat /proc/$PMPID/cgroup | cut -d: -f3)
CH_CG=$(sudo cat /proc/$CHPID/cgroup | cut -d: -f3)
restore() {
    echo "$PMPID" | sudo tee "/sys/fs/cgroup${PM_CG}/cgroup.procs" >/dev/null 2>&1 || true
    echo "$CHPID" | sudo tee "/sys/fs/cgroup${CH_CG}/cgroup.procs" >/dev/null 2>&1 || true
    "${PSQLA[@]}" -c "SET search_path=pg; ALTER TABLE hits RESET (parallel_workers);" >/dev/null 2>&1 || true
    sudo rmdir "$CG" 2>/dev/null || true
}
trap restore EXIT
grep -qw cpu /sys/fs/cgroup/cgroup.subtree_control || echo +cpu | sudo tee /sys/fs/cgroup/cgroup.subtree_control >/dev/null
sudo mkdir -p "$CG"
echo "$PMPID" | sudo tee "$CG/cgroup.procs" >/dev/null
echo "$CHPID" | sudo tee "$CG/cgroup.procs" >/dev/null

hb(){ awk '/^cpu /{print $2+$3+$4+$7+$8+$9}' /proc/stat; }
cc(){ awk '{s=$0;sub(/^[^)]*\) /,"",s);split(s,f," ");print f[12]+f[13]}' /proc/"$1"/stat 2>/dev/null || echo 0; }

new_tag(){ printf 'cbwsw_%s_%s_%s' "$1" "$$" "${RANDOM}${RANDOM}" | tr -cd 'a-z0-9_'; }

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
offload_set(){ cat <<SQL
LOAD 'pg_clickhouse';
SET search_path=pg;
SET pg_clickhouse.local_ch_server='ch_bench';
SET pg_clickhouse.shm_min_rows=0;
SET pg_clickhouse.session_settings='join_use_nulls 1, group_by_use_nulls 1, final 1, allow_experimental_streamed_table_function 1, max_threads $MT, log_comment $1';
SET pg_clickhouse.enable_shm_offload=on;
SET max_parallel_workers=64; SET max_parallel_workers_per_gather=$MT;
SET statement_timeout='300s';
SQL
}

# One timed run: prints "wall_s host_cpu_s ch_cpu_s". $1 = SET block, $2 = SQL.
measure(){ local h0 k0 s0 s1 h1 k1
  h0=$(hb); k0=$(cc "$CHPID"); s0=$(date +%s.%N)
  printf '%s\n%s;\n' "$1" "$2" | "${PSQL[@]}" >/dev/null 2>&1
  s1=$(date +%s.%N); h1=$(hb); k1=$(cc "$CHPID")
  awk -v h0=$h0 -v h1=$h1 -v k0=$k0 -v k1=$k1 -v s0=$s0 -v s1=$s1 -v t=$TICK \
    'BEGIN{printf "%.4f %.3f %.3f", s1-s0, (h1-h0)/t, (k1-k0)/t}'
}
# median / min / max / stdev of wall (field 1); carries cores of the median run.
stats(){ awk '{w[NR]=$1; ch[NR]=$3; ho[NR]=$2; s+=$1; ss+=$1*$1}
  END{n=NR; if(n==0){print "0 0 0 0 0 0"; exit}
      for(i=1;i<=n;i++)for(j=i+1;j<=n;j++){if(w[j]<w[i]){t=w[i];w[i]=w[j];w[j]=t;t=ch[i];ch[i]=ch[j];ch[j]=t;t=ho[i];ho[i]=ho[j];ho[j]=t}}
      med=w[int((n+1)/2)]; mn=w[1]; mx=w[n]; mean=s/n; sd=sqrt(ss/n-mean*mean);
      printf "%.4f %.4f %.4f %.4f %.3f %.3f", med, mn, mx, sd, ho[int((n+1)/2)], ch[int((n+1)/2)]}'; }

# Eligibility: "blocks read_rows" if genuinely pushed (plan ClickHouseShmScan AND
# query_log oracle fires), else "NO(reason)".
check_eligible(){ local sql="$1" tag; tag=$(new_tag elig)
  local plan; plan=$(printf '%s\nEXPLAIN (COSTS OFF)\n%s;\n' "$(offload_set "$tag")" "$sql" | "${PSQLA[@]}" 2>/dev/null)
  echo "$plan" | grep -q 'ClickHouseShmScan' || { echo "NO(no-customscan)"; return; }
  printf '%s\n%s;\n' "$(offload_set "$tag")" "$sql" | "${PSQL[@]}" >/dev/null 2>&1
  # Poll-retry: query_log QueryFinish is enqueued async, slightly after the query
  # returns -- a single FLUSH+read can race and miss it. Retry up to ~6s.
  local m blk=0
  for _try in $(seq 1 12); do
    chq "SYSTEM FLUSH LOGS" >/dev/null
    m=$(chq "SELECT sum(ProfileEvents['ShmAdoptedBlocks']), sum(read_rows) FROM system.query_log WHERE log_comment='$tag' AND type='QueryFinish' AND positionCaseInsensitive(query,'streamed_table')>0 AND positionCaseInsensitive(query,'query_log')=0")
    blk=$(echo "$m" | cut -f1); blk=${blk:-0}
    [ "${blk:-0}" -ge 1 ] 2>/dev/null && break
    sleep 0.5
  done
  [ "${blk:-0}" -ge 1 ] 2>/dev/null && echo "$m" || echo "NO(no-block)"
}

RESULTS="$OUT/RESULTS.md"
: > "$RESULTS"
{
  echo "# ClickBench native-vs-offload W-sweep under shared cgroup cpu.max cap"
  echo ""
  echo "DB=$PGDB  N=$N warm runs/cell  W in {$W_LIST}  postmaster=$PMPID clickhouse=$CHPID  MT=$MT"
  echo "cap: one cgroup ($CG), both process trees, cpu.max = W*${PERIOD}us. Cores = CPU-s/wall"
  echo "(nat_cores = host-CH; off_cons = CH; off_prod = host-CH). speedup = nat_med/off_med."
  echo ""
} >> "$RESULTS"

for q in $QUERIES; do
  qf="$QDIR/$q.sql"; [ -f "$qf" ] || { echo "Q$q: no file"; continue; }
  # Strip EXPLAIN wrapper + SQL line-comments; drop trailing semicolons. Keep newlines.
  sql="$(sed -e '/^EXPLAIN/d' -e '/^[[:space:]]*--/d' "$qf" | sed 's/;[[:space:]]*$//')"
  elig=$(check_eligible "$sql")
  echo "" | tee -a "$RESULTS"
  echo "## Q$q  (offload eligibility: $elig)" | tee -a "$RESULTS"
  printf '\n| W | nat_med_ms | nat_min/max | nat_sd | nat_cores | off_med_ms | off_min/max | off_sd | off_prod | off_cons | speedup |\n' | tee -a "$RESULTS"
  printf -- '|--:|-----------:|-------------|-------:|----------:|-----------:|-------------|-------:|---------:|---------:|--------:|\n' | tee -a "$RESULTS"
  for W in $W_LIST; do
    echo "$((W*PERIOD)) $PERIOD" | sudo tee "$CG/cpu.max" >/dev/null
    prod=${PROD:-$W}; [ "$prod" -lt 1 ] && prod=1
    # NATIVE: hits parallel_workers = W
    "${PSQLA[@]}" -c "SET search_path=pg; ALTER TABLE hits SET (parallel_workers=$W);" >/dev/null 2>&1
    printf '%s\n%s;\n' "$(native_set $W)" "$sql" | "${PSQL[@]}" >/dev/null 2>&1   # warm
    nat=$(for _ in $(seq 1 "$N"); do measure "$(native_set $W)" "$sql"; echo; done | stats)
    # OFFLOAD: producer parallel_workers = prod
    "${PSQLA[@]}" -c "SET search_path=pg; ALTER TABLE hits SET (parallel_workers=$prod);" >/dev/null 2>&1
    otag=$(new_tag run)
    printf '%s\n%s;\n' "$(offload_set ${otag}w)" "$sql" | "${PSQL[@]}" >/dev/null 2>&1   # warm
    off=$(for _ in $(seq 1 "$N"); do measure "$(offload_set $otag)" "$sql"; echo; done | stats)
    awk -v W=$W -v nat="$nat" -v off="$off" 'BEGIN{
      split(nat,a," "); split(off,b," ");
      nmed=a[1]*1000; nmin=a[2]*1000; nmax=a[3]*1000; nsd=a[4]*1000;
      ncores=(a[1]>0)?(a[5]-a[6])/a[1]:0;
      omed=b[1]*1000; omin=b[2]*1000; omax=b[3]*1000; osd=b[4]*1000;
      oprod=(b[1]>0)?(b[5]-b[6])/b[1]:0; ocons=(b[1]>0)?b[6]/b[1]:0;
      sp=(omed>0)?nmed/omed:0;
      printf "| %s | %.0f | %.0f/%.0f | %.0f | %.2f | %.0f | %.0f/%.0f | %.0f | %.2f | %.2f | %.2fx |\n",
        W, nmed, nmin, nmax, nsd, ncores, omed, omin, omax, osd, oprod, ocons, sp;
    }' | tee -a "$RESULTS"
  done
  "${PSQLA[@]}" -c "SET search_path=pg; ALTER TABLE hits RESET (parallel_workers);" >/dev/null 2>&1
done
echo "" | tee -a "$RESULTS"
echo "results written to $RESULTS"
