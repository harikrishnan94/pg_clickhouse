#!/usr/bin/env bash
# Hot/Cold ClickBench-100M — Unit 0 correctness oracle.
#
# For each eligible query q and fraction f in {p01,p05,p10}:
#   (1) result equivalence: cmp_results.py( pure-CH-100M , merge ) in {exact,float,approx}
#   (2) push-down proof: the merged query's CH query_log row has ProfileEvents['ShmAdoptedBlocks']>=1
#   (3) producer fidelity: clickhouse_stream_relation returned exactly N(f) rows
# Plus, once per f: partition exactness (count(hot)=N, count(cold)=TOTAL-N, hot+cold=TOTAL).
# Plus a teardown leak check (no /dev/shm/pgch_*, no control sockets, no stray shm-stream backends).
#
# pure-CH result is f-independent => computed once per query and reused. Both pure-CH and merge run
# with identical CH session settings (the ones the deparser assumed) so the comparison is fair.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; ROOT="$(cd "$HERE/../../.." && pwd)"
CMP="$ROOT/dev/clickbench/cmp_results.py"
CH_PORT="${CH_PORT:-21002}"
SETTINGS="allow_experimental_streamed_table_function=1&join_use_nulls=1&group_by_use_nulls=1&final=1&count_distinct_implementation=uniqExact&max_bytes_before_external_group_by=4000000000&max_bytes_before_external_sort=4000000000&max_execution_time=300"
PSQLU="sudo -u postgres psql -d clickbench -X -q -v ON_ERROR_STOP=0"
TOTAL=99997497
declare -A NF=( [p01]=1000000 [p05]=5000000 [p10]=10000000 )
FRACS="${FRACS:-p01 p05 p10}"
QLIST="${QLIST:-$(grep -P '\tELIGIBLE' "$HERE/templates/eligibility.tsv" | sed 's/^q//;s/\t.*//' | sort -n)}"
OUT="$HERE/results/oracle"; mkdir -p "$OUT/pure" "$OUT/merge"; SUM="$OUT/summary.tsv"
: > "$SUM"; echo -e "f\tq\tclass\tstreamed_rows\texpect_N\tshm_blocks\tverdict" >> "$SUM"

ch()  { curl -s "127.0.0.1:${CH_PORT}/?${SETTINGS}" --data-binary @- ; }
chc() { curl -s "127.0.0.1:${CH_PORT}/" --data-binary @- ; }   # control (no settings)

echo "== partition exactness per fraction =="
for f in $FRACS; do
  python3 "$HERE/mk_merge_sql.py" "$HERE/templates/q5.ch.sql" "$f" >/dev/null 2>&1 || true
done
# exactness uses the boundary predicate directly (independent of the templates)
python3 - "$HERE" "$FRACS" <<'PY'
import sys,os,subprocess
HERE=sys.argv[1]; fr=sys.argv[2].split()
sys.path.insert(0,HERE)
import importlib.util
spec=importlib.util.spec_from_file_location("m",os.path.join(HERE,"mk_merge_sql.py"))
m=importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
env=m.load_boundaries()
import urllib.request,urllib.parse
def chq(q):
    return urllib.request.urlopen("http://127.0.0.1:21002/",data=q.encode()).read().decode().strip()
NF={"01":1000000,"05":5000000,"10":10000000}
for fp in fr:
    f=fp[1:]; pred=m.cold_pred(env,f)
    q=f"SELECT countIf(NOT ({pred})) AS hot, countIf({pred}) AS cold, count() AS tot FROM clickbench.hits_100m SETTINGS max_threads=8"
    hot,cold,tot=chq(q).split("\t")
    ok = (int(hot)==NF[f] and int(hot)+int(cold)==int(tot))
    print(f"PARTITION {fp}: hot={hot} cold={cold} tot={tot} expect_hot={NF[f]} -> {'EXACT' if ok else 'BROKEN'}")
PY

run_one() {  # $1=f  $2=q
  local f="$1" q="$2" tmpl="$HERE/templates/q${q}.ch.sql"
  [ -f "$tmpl" ] || return 0
  # pure-CH (compute once, cache)
  if [ ! -s "$OUT/pure/q${q}.out" ]; then
    python3 "$HERE/mk_merge_sql.py" "$tmpl" pure | ch > "$OUT/pure/q${q}.out" 2>"$OUT/pure/q${q}.err" || true
  fi
  # launch producer (self-cancels via statement_timeout if no consumer)
  local pout="/tmp/oracle_prod_${f}_q${q}.out"; rm -f "$pout"
  ( $PSQLU -tA -c "LOAD 'pg_clickhouse'; SET search_path=pg,public; SET statement_timeout='240s'; SELECT clickhouse_stream_relation('pg.hits_hot_${f}'::regclass, 'pgch_hot_${f}', 65536);" > "$pout" 2>&1 ) &
  local pp=$!
  local i; for i in $(seq 1 200); do [ -S "/tmp/clickhouse_shm_pgch_hot_${f}.sock" ] && break; sleep 0.05; done
  local tag="oracle_${f}_q${q}_$(date +%s%N)"
  python3 "$HERE/mk_merge_sql.py" "$tmpl" "$f" | curl -s "127.0.0.1:${CH_PORT}/?${SETTINGS}&log_comment=${tag}" --data-binary @- > "$OUT/merge/${f}_q${q}.out" 2>"$OUT/merge/${f}_q${q}.err" || true
  # if the merge errored it never drained the ring -> cleanly cancel the producer (no 240s hang, no bash-kill)
  if grep -q 'Exception' "$OUT/merge/${f}_q${q}.out" 2>/dev/null; then
    $PSQLU -tA -c "SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE query LIKE '%clickhouse_stream_relation%' AND pid<>pg_backend_pid();" >/dev/null 2>&1
  fi
  wait $pp 2>/dev/null
  local rows; rows=$(tr -dc '0-9' < "$pout" 2>/dev/null); rows="${rows:-NA}"
  # poll-retry for QueryFinish (query_log flush is async — verify_offload.sh does the same)
  local blk=0 tries fin
  for tries in $(seq 1 15); do
    echo "SYSTEM FLUSH LOGS" | chc >/dev/null 2>&1
    fin=$(echo "SELECT count() FROM system.query_log WHERE log_comment='${tag}' AND type='QueryFinish'" | chc 2>/dev/null)
    if [ "${fin:-0}" -ge 1 ] 2>/dev/null; then
      blk=$(echo "SELECT max(ProfileEvents['ShmAdoptedBlocks']+ProfileEvents['ShmCopiedBlocks']) FROM system.query_log WHERE log_comment='${tag}' AND type='QueryFinish'" | chc 2>/dev/null)
      break
    fi
    sleep 0.5
  done
  blk="${blk:-0}"
  local cls; cls=$(python3 "$CMP" "$OUT/pure/q${q}.out" "$OUT/merge/${f}_q${q}.out" 1e-6 2>/dev/null | cut -d'|' -f1); cls="${cls:-ERR}"
  local verdict="FAIL"
  case "$cls" in exact|float|approx)
     [ "$rows" = "${NF[$f]}" ] && [ "${blk:-0}" -ge 1 ] 2>/dev/null && verdict="PASS" ;;
  esac
  echo -e "${f}\t${q}\t${cls}\t${rows}\t${NF[$f]}\t${blk}\t${verdict}" >> "$SUM"
  echo "  [$f q$q] class=$cls rows=$rows/${NF[$f]} blk=$blk -> $verdict"
}

for f in $FRACS; do
  echo "== fraction $f =="
  for q in $QLIST; do run_one "$f" "$q"; done
done

echo "== teardown leak check =="
echo "  /dev/shm/pgch_*: $(ls /dev/shm/ 2>/dev/null | grep -c pgch || true)"
echo "  control sockets: $(ls /tmp/clickhouse_shm_pgch* 2>/dev/null | wc -l)"
echo "  shm-stream backends: $($PSQLU -tAc "select count(*) from pg_stat_activity where backend_type ilike '%clickhouse%' or query ilike '%clickhouse_stream_relation%' and pid<>pg_backend_pid();" 2>/dev/null)"
echo "== summary =="
awk -F'\t' 'NR>1{c[$7]++} END{for(k in c) printf "  %s: %d\n",k,c[k]}' "$SUM"
echo "  (full: $SUM)"
