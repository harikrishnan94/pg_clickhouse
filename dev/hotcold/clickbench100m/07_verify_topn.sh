#!/usr/bin/env bash
# Hot/Cold ClickBench-100M — Unit 0: top-N DIFF root-cause via deterministic tiebreak.
#
# The 14 DIFF cells are all LIMIT/top-N queries whose LIMIT boundary is ambiguous (tied rows ->
# non-deterministic in BOTH engines; the failures are inconsistent across fractions, the signature
# of tie reshuffle, not a systematic bug). Per the repo convention (dev/clickbench/tiebreak_check.sh)
# and task §7, a genuine tie deviation collapses to `exact` once a TOTAL order is imposed; a real
# value bug survives as DIFF/approx. Here we re-capture the deparser-faithful CH body for the
# TIEBROKEN PG query, then compare pure-CH-100M vs merge for all three fractions. PASS == exact.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; ROOT="$(cd "$HERE/../../.." && pwd)"
CMP="$ROOT/dev/clickbench/cmp_results.py"; CH_PORT="${CH_PORT:-21002}"
SETTINGS="allow_experimental_streamed_table_function=1&join_use_nulls=1&group_by_use_nulls=1&final=1&count_distinct_implementation=uniqExact&max_bytes_before_external_group_by=4000000000&max_bytes_before_external_sort=4000000000&max_execution_time=300"
PSQLU="sudo -u postgres psql -d clickbench -X -q -v ON_ERROR_STOP=0"
OUT="$HERE/results/topn"; mkdir -p "$OUT/tmpl" "$OUT/pure" "$OUT/merge"; SUM="$OUT/summary.tsv"
echo -e "q\tf\tclass\tverdict" > "$SUM"
ch()  { curl -s "127.0.0.1:${CH_PORT}/?${SETTINGS}" --data-binary @- ; }
chc() { curl -s "127.0.0.1:${CH_PORT}/" --data-binary @- ; }

# Tiebroken PG queries (deterministic total order appended). WatchID makes EventTime-ties unique.
declare -A Q
Q[18]="SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC, UserID, SearchPhrase LIMIT 10"
Q[25]="SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase, WatchID LIMIT 10"
Q[31]="SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, ClientIP ORDER BY c DESC, SearchEngineID, ClientIP LIMIT 10"
Q[32]="SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY WatchID, ClientIP ORDER BY c DESC, WatchID, ClientIP LIMIT 10"
Q[39]="SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND IsLink <> 0 AND IsDownload = 0 GROUP BY URL ORDER BY PageViews DESC, URL LIMIT 10 OFFSET 1000"
Q[40]="SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END AS Src, URL AS Dst, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst ORDER BY PageViews DESC, TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst LIMIT 10 OFFSET 1000"

$PSQLU -c "ALTER TABLE pg.hits SET (parallel_workers=0);" >/dev/null 2>&1
for q in 18 25 31 32 39 40; do
  # 1) capture deparser-faithful CH body for the tiebroken query
  tag="tbcap_q${q}_$(date +%s%N)"; echo "SYSTEM FLUSH LOGS" | chc >/dev/null 2>&1
  $PSQLU -tA >/dev/null 2>&1 <<SQL || true
LOAD 'pg_clickhouse'; SET search_path=pg,public;
SET pg_clickhouse.local_ch_server='ch_bench'; SET pg_clickhouse.shm_min_rows=0;
SET pg_clickhouse.session_settings='join_use_nulls 1, group_by_use_nulls 1, final 1, allow_experimental_streamed_table_function 1, count_distinct_implementation uniqExact, log_comment ${tag}';
SET pg_clickhouse.enable_shm_offload=on; SET statement_timeout='120s';
EXPLAIN (ANALYZE, COSTS) ${Q[$q]};
SQL
  sleep 1; echo "SYSTEM FLUSH LOGS" | chc >/dev/null 2>&1; sleep 0.5
  echo "SELECT replaceRegexpAll(query,'\\\\s+',' ') FROM system.query_log WHERE log_comment='${tag}' AND type='QueryFinish' AND positionCaseInsensitive(query,'streamed_table')>0 AND positionCaseInsensitive(query,'query_log')=0 ORDER BY event_time_microseconds DESC LIMIT 1 FORMAT TSVRaw" | chc > "$OUT/tmpl/q${q}.ch.sql"
  ncalls=$(grep -o 'streamed_table(' "$OUT/tmpl/q${q}.ch.sql" | wc -l) || true
  [ "$ncalls" = "1" ] || { echo -e "q${q}\t-\tCAPTURE_FAIL($ncalls st)\tFAIL" | tee -a "$SUM"; continue; }
  # 2) pure-CH once
  python3 "$HERE/mk_merge_sql.py" "$OUT/tmpl/q${q}.ch.sql" pure | ch > "$OUT/pure/q${q}.out" 2>&1
  # 3) merge per fraction
  for f in p01 p05 p10; do
    rm -f "/tmp/tb_prod_${f}.out"
    ( $PSQLU -tA -c "LOAD 'pg_clickhouse'; SET search_path=pg,public; SET statement_timeout='240s'; SELECT clickhouse_stream_relation('pg.hits_hot_${f}'::regclass,'pgch_hot_${f}',65536);" >"/tmp/tb_prod_${f}.out" 2>&1 ) & pp=$!
    for i in $(seq 1 300); do [ -S "/tmp/clickhouse_shm_pgch_${f}.sock" ] && break; sleep 0.03; done
    python3 "$HERE/mk_merge_sql.py" "$OUT/tmpl/q${q}.ch.sql" "$f" | ch > "$OUT/merge/q${q}_${f}.out" 2>&1
    grep -q 'Exception' "$OUT/merge/q${q}_${f}.out" && $PSQLU -tAc "SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE query LIKE '%clickhouse_stream_relation%' AND pid<>pg_backend_pid();" >/dev/null 2>&1
    wait $pp 2>/dev/null
    cls=$(python3 "$CMP" "$OUT/pure/q${q}.out" "$OUT/merge/q${q}_${f}.out" 1e-6 2>/dev/null | cut -d'|' -f1); cls="${cls:-ERR}"
    v="FAIL"; case "$cls" in exact|float|approx) v="PASS";; esac
    echo -e "q${q}\t${f}\t${cls}\t${v}" | tee -a "$SUM"
  done
done
echo "== topn verify summary =="; awk -F'\t' 'NR>1{c[$4]++} END{for(k in c) printf "  %s: %d\n",k,c[k]}' "$SUM"