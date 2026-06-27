#!/usr/bin/env bash
# Hot/Cold ClickBench-100M — Unit 1 baselines at the FEASIBLE 10M scale (native-PG & full-offload).
#
# native-PG-100M and full-offload-100M are infeasible to measure (PG 100M heap ~70 GiB > free; D-HC-0401).
# This measures the two motivating baselines at 10M (pg.hits), under the SAME shared cgroup cpu cap as
# the 100M merge sweep, so the REPORT can present clearly-labeled LINEAR projections to 100M (never as
# measured 100M numbers, §9.6):
#   native_pg_10m   = the ClickBench query run natively in PG over pg.hits (enable_shm_offload=off), W workers
#   full_offload_10m= the SAME PG query streamed in full to CH via the SHM offload path (offload on)
# Both via the deparser/offload path (the repo's run.sh/wsweep convention), median-of-N wall.
# NOTE: do NOT run concurrently with 06_bench.sh (shared CH/PG). Run after the 100M sweep.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; ROOT="$(cd "$HERE/../../.." && pwd)"
CH_PORT="${CH_PORT:-21002}"
PSQLU="sudo -u postgres psql -d clickbench -X -q -v ON_ERROR_STOP=0 -tA"
N="${N:-5}"; W="${W:-8}"; QDIR="$ROOT/dev/clickbench/queries"
QLIST="${QLIST:-$(grep -P '\tELIGIBLE' "$HERE/templates/eligibility.tsv" | sed 's/^q//;s/\t.*//' | sort -n)}"
OUT="$HERE/results/bench"; mkdir -p "$OUT"; BCELLS="$OUT/baselines10m.tsv"
echo -e "q\tnative_pg_med\tnative_pg_sd\tfull_offload_med\tfull_offload_sd\tnative_min\toffload_min" > "$BCELLS"

# cgroup cap = W cores over PG tree + CH (same as 06_bench)
CG=/sys/fs/cgroup/pgch_hc100m; PERIOD=100000
PMPID=$(sudo head -1 /var/lib/postgresql/18/main/postmaster.pid)
CHPID=$(ss -ltnp 2>/dev/null | grep ":$CH_PORT " | grep -oP 'pid=\K[0-9]+' | head -1)
grep -qw cpu /sys/fs/cgroup/cgroup.subtree_control || echo +cpu | sudo tee /sys/fs/cgroup/cgroup.subtree_control >/dev/null
sudo mkdir -p "$CG"; echo "$PMPID" | sudo tee "$CG/cgroup.procs" >/dev/null; echo "$CHPID" | sudo tee "$CG/cgroup.procs" >/dev/null
echo "$((W*PERIOD)) $PERIOD" | sudo tee "$CG/cpu.max" >/dev/null
restore(){ echo "$PMPID" | sudo tee /sys/fs/cgroup/cgroup.procs >/dev/null 2>&1 || true; echo "$CHPID" | sudo tee /sys/fs/cgroup/cgroup.procs >/dev/null 2>&1 || true; sudo rmdir "$CG" 2>/dev/null || true; }
trap restore EXIT

# set the per-table parallel_workers (the W knob, per the wsweep convention)
$PSQLU -c "ALTER TABLE pg.hits SET (parallel_workers=$W);" >/dev/null 2>&1
mms(){ sort -g | awk '{a[NR]=$1} END{n=NR;if(n==0){print "0 0";exit} m=a[int((n+1)/2)];s=0;ss=0;for(i=1;i<=n;i++){s+=a[i];ss+=a[i]*a[i]}mean=s/n;sd=sqrt((ss/n-mean*mean>0)?ss/n-mean*mean:0);printf "%s %s %.1f",m,a[1],sd}'; }

native_sql(){ printf 'SET search_path=pg; SET pg_clickhouse.enable_shm_offload=off; SET max_parallel_workers=%s; SET max_parallel_workers_per_gather=%s; SET statement_timeout=%s; %s' "$W" "$W" "'600s'" "$1"; }
offload_set(){ printf "LOAD 'pg_clickhouse'; SET search_path=pg; SET pg_clickhouse.local_ch_server='ch_bench'; SET pg_clickhouse.shm_min_rows=0; SET pg_clickhouse.session_settings='join_use_nulls 1, group_by_use_nulls 1, final 1, allow_experimental_streamed_table_function 1, count_distinct_implementation uniqExact'; SET pg_clickhouse.enable_shm_offload=on; SET statement_timeout='600s';"; }

# bare SELECT from the EXPLAIN(ANALYZE)-wrapped query file
bare(){ sed -n '2,$p' "$QDIR/$1.sql"; }
runwall(){ local t0 t1; t0=$(date +%s.%N); printf '%s\n' "$1" | $PSQLU >/dev/null 2>&1; t1=$(date +%s.%N); awk -v a="$t0" -v b="$t1" 'BEGIN{printf "%.0f",(b-a)*1000}'; }

for q in $QLIST; do
  [ -f "$QDIR/$q.sql" ] || continue
  sql="$(bare "$q")"
  read nm nmn nsd <<<"$(for _ in $(seq 1 $N); do runwall "$(native_sql "$sql")"; echo; done | mms)"
  read om omn osd <<<"$(for _ in $(seq 1 $N); do runwall "$(offload_set)
$sql"; echo; done | mms)"
  echo -e "${q}\t${nm}\t${nsd}\t${om}\t${osd}\t${nmn}\t${omn}" | tee -a "$BCELLS"
done
echo "== baselines done: $BCELLS =="
