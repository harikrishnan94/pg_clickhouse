#!/usr/bin/env bash
# Hot/Cold ClickBench-100M — Unit 1 iter-2b: PARALLEL hot producer (D-HC-0405 revisit).
#
# iter-1 found the SINGLE-threaded hot producer (~2.6M rows/s) is the bottleneck (merge_ch dominated by
# the hot stream for fast queries; the f=10% win vs full-offload vanished). Here P disjoint hot
# sub-tables are streamed by P concurrent producers into P rings, unioned with cold in one CH query
# (mk_merge_sql mode p<f>:parP). Tests whether parallel hot shrinks merge_ch -> more queries hide.
#
# SETUP (idempotent): split clickbench.hits_hot_p<f> into P parts by cityHash64(WatchID,EventTime,UserID)%P,
# COPY each to PG pg.hits_hot_p<f>_<w>. BENCH: single-hot merge (p<f>) vs parallel-hot (p<f>:parP), N runs,
# CH query_duration_ms median. cgroup cap = W cores (shared PG+CH). Run AFTER the main sweep (shares CH/PG).
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; ROOT="$(cd "$HERE/../../.." && pwd)"
CH_PORT="${CH_PORT:-21002}"; CH="curl -s 127.0.0.1:${CH_PORT}/"
SET_BASE="allow_experimental_streamed_table_function=1&join_use_nulls=1&group_by_use_nulls=1&final=1&count_distinct_implementation=uniqExact&max_bytes_before_external_group_by=4000000000&max_bytes_before_external_sort=4000000000&max_execution_time=600"
PSQLU="sudo -u postgres psql -d clickbench -X -q -v ON_ERROR_STOP=0"
N="${N:-5}"; W="${W:-8}"; P="${P:-8}"; FRACS="${FRACS:-p01 p10}"; QLIST="${QLIST:-29 33 19 17 18 13 22 8}"
OUT="$HERE/results/bench"; mkdir -p "$OUT"; PCELLS="$OUT/parallel_hot.tsv"
[ -s "$PCELLS" ] || echo -e "f\tP\tq\tcold_ch\thot1_ch\tmerge1_ch\thotP_ch\tmergeP_ch\tspeedup_merge" > "$PCELLS"
NF_rows(){ case "$1" in p01) echo 1000000;; p05) echo 5000000;; p10) echo 10000000;; esac; }

setup_split(){ local f="$1" w want got
  want=$(NF_rows "$f")
  for w in $(seq 0 $((P-1))); do
    if [ "$(echo "EXISTS clickbench.hits_hot_${f}_${w}" | $CH --data-binary @- 2>/dev/null)" != "1" ]; then
      echo "  [setup] CH split hits_hot_${f}_${w} ..."
      echo "CREATE TABLE clickbench.hits_hot_${f}_${w} ENGINE=MergeTree ORDER BY tuple() AS SELECT * FROM clickbench.hits_hot_${f} WHERE cityHash64(WatchID,EventTime,UserID) % ${P} = ${w}" | $CH --data-binary @-
    fi
    if [ "$($PSQLU -tAc "select to_regclass('pg.hits_hot_${f}_${w}') is not null")" != "t" ]; then
      echo "  [setup] PG copy hits_hot_${f}_${w} ..."
      $PSQLU >/dev/null 2>&1 <<SQL
SET search_path=pg; CREATE TABLE pg.hits_hot_${f}_${w} (LIKE pg.hits);
COPY pg.hits_hot_${f}_${w} FROM PROGRAM 'curl -s --data-binary "SELECT * FROM clickbench.hits_hot_${f}_${w} FORMAT TabSeparated" http://127.0.0.1:${CH_PORT}/';
SQL
    fi
  done
  # verify disjoint+total: sum of parts == N(f)
  got=$($PSQLU -tAc "select $(for w in $(seq 0 $((P-1))); do printf '(select count(*) from pg.hits_hot_%s_%s)+' "$f" "$w"; done)0")
  [ "$got" = "$want" ] && echo "  [setup] $f: P=$P parts sum=$got == N OK" || { echo "  [setup] $f FAIL sum=$got != $want"; exit 1; }
}

CG=/sys/fs/cgroup/pgch_hc100m; PERIOD=100000
PMPID=$(sudo head -1 /var/lib/postgresql/18/main/postmaster.pid); CHPID=$(ss -ltnp|grep ":$CH_PORT "|grep -oP 'pid=\K[0-9]+'|head -1)
grep -qw cpu /sys/fs/cgroup/cgroup.subtree_control || echo +cpu|sudo tee /sys/fs/cgroup/cgroup.subtree_control>/dev/null
sudo mkdir -p "$CG"; echo "$PMPID"|sudo tee "$CG/cgroup.procs">/dev/null; echo "$CHPID"|sudo tee "$CG/cgroup.procs">/dev/null
echo "$((W*PERIOD)) $PERIOD"|sudo tee "$CG/cpu.max">/dev/null
restore(){ echo "$PMPID"|sudo tee /sys/fs/cgroup/cgroup.procs>/dev/null 2>&1||true; echo "$CHPID"|sudo tee /sys/fs/cgroup/cgroup.procs>/dev/null 2>&1||true; sudo rmdir "$CG" 2>/dev/null||true; }
trap restore EXIT
mms(){ sort -g|awk '{a[NR]=$1}END{n=NR;print (n? a[int((n+1)/2)] : 0)}'; }
chdur(){ local tb t n; for t in $(seq 1 20); do echo "SYSTEM FLUSH LOGS"|$CH --data-binary @->/dev/null 2>&1
  n=$(echo "SELECT count() FROM system.query_log WHERE log_comment LIKE '$1%' AND type='QueryFinish'"|$CH --data-binary @- 2>/dev/null); [ "${n:-0}" -ge "$2" ]&&break; sleep 0.5; done
  echo "SELECT query_duration_ms FROM system.query_log WHERE log_comment LIKE '$1%' AND type='QueryFinish' ORDER BY event_time_microseconds"|$CH --data-binary @- 2>/dev/null|mms; }

# launch_n: $1=f $2=n(1 or P) -> launches producers for rings pgch_hot_<f>[_w], sets PIDS[]
launch_n(){ local f="$1" n="$2" w; PIDS=(); local tbl ring
  for w in $(seq 0 $((n-1))); do
    if [ "$n" = 1 ]; then tbl="pg.hits_hot_${f}"; ring="pgch_hot_${f}"; else tbl="pg.hits_hot_${f}_${w}"; ring="pgch_hot_${f}_${w}"; fi
    ( $PSQLU -tA -c "LOAD 'pg_clickhouse'; SET search_path=pg,public; SET statement_timeout='600s'; SELECT clickhouse_stream_relation('${tbl}'::regclass,'${ring}',65536);" >"/tmp/ph_${f}_${w}.out" 2>&1 ) & PIDS+=($!)
    local i; for i in $(seq 1 600); do [ -S "/tmp/clickhouse_shm_${ring}.sock" ]&&break; sleep 0.02; done
  done; }
runmerge(){ local f="$1" sql="$2" n="$3" tag="$4" t0 t1; launch_n "$f" "$n"
  printf '%s' "$sql"|curl -s "127.0.0.1:${CH_PORT}/?${SET_BASE}&max_threads=${W}&log_comment=${tag}" --data-binary @->/dev/null 2>&1
  for pid in "${PIDS[@]}"; do grep -q Exception "/tmp/ph_${f}_"*.out 2>/dev/null && $PSQLU -tAc "SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE query LIKE '%clickhouse_stream_relation%' AND pid<>pg_backend_pid();">/dev/null 2>&1; done
  for pid in "${PIDS[@]}"; do wait "$pid" 2>/dev/null; done; }

for f in $FRACS; do echo "== fraction $f =="; setup_split "$f"
  for q in $QLIST; do
    [ -f "$HERE/templates/q${q}.ch.sql" ] || continue
    cold=$(awk -F'\t' -v q="$q" -v f="$f" '$1==f && $3==q{print $5}' "$OUT/cells.tsv")
    s1="bphs1_${f}_q${q}_$(date +%s%N)"; sP="bphsP_${f}_q${q}_$(date +%s%N)"
    for _ in $(seq 1 $N); do runmerge "$f" "$(python3 "$HERE/mk_merge_sql.py" "$HERE/templates/q${q}.ch.sql" "$f")" 1 "${s1}_$RANDOM"; done
    for _ in $(seq 1 $N); do runmerge "$f" "$(python3 "$HERE/mk_merge_sql.py" "$HERE/templates/q${q}.ch.sql" "${f}:par${P}")" "$P" "${sP}_$RANDOM"; done
    h1=$(chdur "bphs1_${f}_q${q}_" "$N"); hP=$(chdur "bphsP_${f}_q${q}_" "$N")
    sp=$(awk -v a="$h1" -v b="$hP" 'BEGIN{print (b>0)?a/b:0}')
    echo -e "${f}\t${P}\t${q}\t${cold}\t-\t${h1}\t-\t${hP}\t${sp}" | tee -a "$PCELLS"
  done
done
echo "== parallel-hot done: $PCELLS =="
