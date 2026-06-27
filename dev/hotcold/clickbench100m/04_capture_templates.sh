#!/usr/bin/env bash
# Hot/Cold ClickBench-100M — Unit 0 step 4: capture per-query deparsed CH bodies (templates).
#
# Runs each ClickBench PG query (queries/<q>.sql, EXPLAIN(ANALYZE) -> executes + offloads at 10M)
# with parallel_workers=0 (single stream => exactly one streamed_table() call) and grabs the CH
# query ClickHouse actually executed from system.query_log (matched by a unique log_comment).
# That CH query is the deparser-faithful body; the merge/pure-CH harness later swaps its single
# streamed_table source subquery for hits_dt64 (pure-CH) or the inline hot UNION cold (merge).
# Output: templates/q<q>.ch.sql  (raw deparsed CH query, one line). Queries with no streamed_table
# (e.g. Q1 COUNT(*) which references no column) are recorded as INELIGIBLE.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; ROOT="$(cd "$HERE/../../.." && pwd)"
QDIR="$ROOT/dev/clickbench/queries"; OUT="$HERE/templates"; mkdir -p "$OUT"
CH_PORT="${CH_PORT:-21002}"; CH="curl -s 127.0.0.1:${CH_PORT}/"
PSQLU="sudo -u postgres psql -d clickbench -X -q -v ON_ERROR_STOP=0"

$PSQLU -c "ALTER TABLE pg.hits SET (parallel_workers=0);" >/dev/null 2>&1
: > "$OUT/eligibility.tsv"
for q in $(seq 1 43); do
    [ -f "$QDIR/$q.sql" ] || continue
    tag="dcap_q${q}_$(date +%s%N)"
    echo "SYSTEM FLUSH LOGS" | $CH --data-binary @- >/dev/null 2>&1
    $PSQLU -tA >/dev/null 2>&1 <<SQL || true
LOAD 'pg_clickhouse'; SET search_path=pg,public;
SET pg_clickhouse.local_ch_server='ch_bench'; SET pg_clickhouse.shm_min_rows=0;
SET pg_clickhouse.session_settings='join_use_nulls 1, group_by_use_nulls 1, final 1, allow_experimental_streamed_table_function 1, count_distinct_implementation uniqExact, log_comment ${tag}';
SET pg_clickhouse.enable_shm_offload=on;
SET statement_timeout='120s';
$(cat "$QDIR/$q.sql")
SQL
    sleep 1; echo "SYSTEM FLUSH LOGS" | $CH --data-binary @- >/dev/null 2>&1; sleep 0.5
    ch_q=$(echo "SELECT replaceRegexpAll(query,'\\\\s+',' ') FROM system.query_log WHERE log_comment='${tag}' AND type='QueryFinish' AND positionCaseInsensitive(query,'streamed_table')>0 AND positionCaseInsensitive(query,'query_log')=0 ORDER BY event_time_microseconds DESC LIMIT 1 FORMAT TSVRaw" | $CH --data-binary @- 2>/dev/null)
    ncalls=$(printf '%s' "$ch_q" | grep -o 'streamed_table(' | wc -l) || true
    if [ -n "$ch_q" ] && [ "$ncalls" = "1" ]; then
        printf '%s\n' "$ch_q" > "$OUT/q${q}.ch.sql"
        printf 'q%s\tELIGIBLE\t%s streamed_table\n' "$q" "$ncalls" | tee -a "$OUT/eligibility.tsv"
    elif [ -n "$ch_q" ] && [ "$ncalls" -gt 1 ]; then
        printf '%s\n' "$ch_q" > "$OUT/q${q}.ch.sql"
        printf 'q%s\tELIGIBLE_MULTI\t%s streamed_table (tokenizer must handle)\n' "$q" "$ncalls" | tee -a "$OUT/eligibility.tsv"
    else
        printf 'q%s\tINELIGIBLE\tno streamed_table in CH query_log (declined offload)\n' "$q" | tee -a "$OUT/eligibility.tsv"
    fi
done
echo "=== eligibility summary ==="; cut -f2 "$OUT/eligibility.tsv" | sort | uniq -c
