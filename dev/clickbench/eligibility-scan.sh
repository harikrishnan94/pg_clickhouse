#!/usr/bin/env bash
# Phase-0 eligibility + fidelity scan for the 43 ClickBench queries.
#
# ClickBench is single-table (no joins): the heavy fragment is the
# aggregate / GROUP BY / HAVING (+ top-N) for aggregate queries, or the
# filtered scan (+ optional ORDER BY/LIMIT) for the non-aggregate projections
# (Q20, Q24-27). For each query it records, from INDEPENDENT sources:
#
#   1. Correctness/fidelity: run with offload OFF (native, tuned) and ON; capture
#      both result sets and quantify any deviation with cmp_results.py -- exact /
#      float(avg->Float64, bounded by TOL) / approx(count(DISTINCT) integer error)
#      / DIFF (structural; flagged for manual classification incl. LIMIT ties).
#   2. ClickHouse query_log oracle (authority): correlate by a unique per-query
#      log_comment tag -> dispatched streamed_table() SQL, read_rows,
#      ProfileEvents['ShmAdoptedBlocks'], and how count(DISTINCT) deparsed
#      (uniq / uniqExact / count(DISTINCT)).
#   3. PostgreSQL plan (offload ON, EXPLAIN VERBOSE COSTS OFF): how many
#      Custom Scan (ClickHouseShmScan) nodes, and whether a residual Aggregate /
#      Sort / Limit still sits in PG above the topmost CustomScan (i.e. the heavy
#      fragment / the top-N was NOT pushed).
#
# Verdict:
#   fully     - oracle fires (ShmAdoptedBlocks>=1) AND CH SQL carries the heavy op
#               (GROUP BY/aggregate for agg queries; WHERE for projections) AND PG
#               has no residual Aggregate above the CustomScan.
#   scan_only - a ClickHouseShmScan fires but PG still does the aggregate.
#   none      - no ClickHouseShmScan / no streamed_table query in the log.
#
# Raw artifacts land in $OUT (default dev/clickbench/evidence/phase0). A markdown
# summary table is printed and written to $OUT/SUMMARY.md.
#
#   PGDB=clickbench dev/clickbench/eligibility-scan.sh [q ...]   # default 1..43
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1090
. "$HERE/../bench/bench-common.sh"          # chq, shm_set_block, manifest vars

PGDB="${PGDB:-clickbench}"
QDIR="$HERE/queries"
OUT="${OUT:-$HERE/evidence/phase0}"
TOL="${TOL:-1e-6}"
CMP="$HERE/cmp_results.py"
mkdir -p "$OUT"
QUERIES=("$@"); [ ${#QUERIES[@]} -gt 0 ] || QUERIES=($(seq 1 43))

# psql as the postgres OS user; pipe-separated, NULL->NULL, do NOT stop on error
# (we want to capture partial output + the error text for the ledger).
PSQL=(sudo -u postgres psql -d "$PGDB" -tAqX -F'|' -v ON_ERROR_STOP=0 -P null=NULL)

# Native baseline tuning (correctness baseline; the latency benchmark is wsweep.sh).
NATIVE_TUNE="SET search_path=pg;
SET pg_clickhouse.enable_shm_offload=off;
SET max_parallel_workers=16;
SET max_parallel_workers_per_gather=8;
SET parallel_leader_participation=on;
SET min_parallel_table_scan_size=0;
SET work_mem='2GB';
SET hash_mem_multiplier=4;
SET jit=on;
SET statement_timeout='600s';"

# Strip the EXPLAIN(ANALYZE) wrapper -> the runnable SELECT.
runnable() { sed '/^EXPLAIN/d' "$QDIR/$1.sql"; }
# Plan-only EXPLAIN (no execution) for residual-op classification.
plansql()  { sed 's/^EXPLAIN.*/EXPLAIN (VERBOSE, COSTS OFF)/' "$QDIR/$1.sql"; }
# Does the query contain an aggregate / GROUP BY?
q_has_agg()  { grep -qiE 'sum\(|avg\(|count\(|min\(|max\(|group[[:space:]]+by' "$QDIR/$1.sql"; }

summary="$OUT/SUMMARY.md"
: > "$summary"
printf '| Q | verdict | fidelity | shmscan | strmd | ShmBlk | read_rows | pg_agg | pg_sort | pg_lim | ch_grpby | ch_ord | ch_lim | ch_distinct | rows off/on |\n' >> "$summary"
printf '|--:|---------|----------|--------:|------:|-------:|----------:|-------:|--------:|-------:|:--------:|:------:|:------:|:-----------:|------------|\n' >> "$summary"

echo "=== Phase-0 ClickBench eligibility scan: db=$PGDB  CH=$CH_HTTP  out=$OUT ==="

for q in "${QUERIES[@]}"; do
    qf="$QDIR/$q.sql"; [ -f "$qf" ] || { echo "Q$q: no file"; continue; }
    tag="$(new_tag elig_q${q})"
    rsql="$(runnable "$q")"
    psql_plan="$(plansql "$q")"

    # --- OFF baseline (native, tuned) ---
    printf '%s\n%s\n' "$NATIVE_TUNE" "$rsql" \
        | "${PSQL[@]}" > "$OUT/q${q}.off.out" 2> "$OUT/q${q}.off.err"

    # --- ON plan (cheap, no execution): residual PG-op classification ---
    printf '%s\nSET search_path=pg;\n%s\n' "$(shm_set_block "$tag")" "$psql_plan" \
        | "${PSQL[@]}" > "$OUT/q${q}.plan.txt" 2> "$OUT/q${q}.plan.err"

    # --- ON run (executes -> oracle + result) ---
    chq "SYSTEM FLUSH LOGS" >/dev/null 2>&1
    printf '%s\nSET search_path=pg;\nSET statement_timeout=%s;\n%s\n' \
        "$(shm_set_block "$tag")" "'600s'" "$rsql" \
        | "${PSQL[@]}" > "$OUT/q${q}.on.out" 2> "$OUT/q${q}.on.err"
    chq "SYSTEM FLUSH LOGS" >/dev/null 2>&1

    # --- query_log oracle (by tag) ---
    # Poll-retry: ClickHouse enqueues the QueryFinish row into the async query_log
    # buffer slightly AFTER the pipeline completes, so a single FLUSH+read right
    # after the PG query returns can race and miss it (observed false-negatives on
    # large-grouped-result queries Q18/37/40). Retry until the streamed_table entry
    # for this tag appears (or give up after ~6s -> genuine non-offload).
    strmd_q=0
    for _try in $(seq 1 12); do
        chq "SYSTEM FLUSH LOGS" >/dev/null 2>&1
        strmd_q=$(chq "SELECT count() FROM system.query_log
                       WHERE log_comment='$tag' AND type='QueryFinish'
                         AND positionCaseInsensitive(query,'streamed_table')>0
                         AND positionCaseInsensitive(query,'query_log')=0")
        [ "${strmd_q:-0}" -ge 1 ] 2>/dev/null && break
        sleep 0.5
    done
    # Fallback: a streamed_table query that ADOPTED blocks but logged as
    # ExceptionWhileProcessing because PG's LIMIT(-without-ORDER-BY) closed the
    # socket early (Code 210 broken pipe) still PROVES the heavy fragment ran in
    # CH (Q18). Capture it as a cancelled-but-adopted offload.
    cxl_blk=0
    qtype="QueryFinish"
    if [ "${strmd_q:-0}" -lt 1 ] 2>/dev/null; then
        cxl_blk=$(chq "SELECT sum(ProfileEvents['ShmAdoptedBlocks']) FROM system.query_log
                       WHERE log_comment='$tag' AND type='ExceptionWhileProcessing'
                         AND positionCaseInsensitive(query,'streamed_table')>0
                         AND positionCaseInsensitive(query,'query_log')=0")
        cxl_blk=${cxl_blk:-0}
        # When the only streamed_table evidence is a client-cancelled (Code 210)
        # row, read its metrics/SQL from the Exception row so the table is
        # self-consistent (Q18).
        [ "${cxl_blk:-0}" -ge 1 ] 2>/dev/null && qtype="ExceptionWhileProcessing"
    fi
    read_rows=$(chq "SELECT sum(read_rows) FROM system.query_log
                   WHERE log_comment='$tag' AND type='$qtype'
                     AND positionCaseInsensitive(query,'streamed_table')>0
                     AND positionCaseInsensitive(query,'query_log')=0")
    shm_blocks=$(chq "SELECT sum(ProfileEvents['ShmAdoptedBlocks']) FROM system.query_log
                   WHERE log_comment='$tag' AND type='$qtype'
                     AND positionCaseInsensitive(query,'streamed_table')>0
                     AND positionCaseInsensitive(query,'query_log')=0")
    chq "SELECT replaceRegexpAll(query,'\\\\s+',' ') FROM system.query_log
         WHERE log_comment='$tag' AND type='$qtype'
           AND positionCaseInsensitive(query,'streamed_table')>0
           AND positionCaseInsensitive(query,'query_log')=0
         ORDER BY event_time_microseconds DESC LIMIT 5" > "$OUT/q${q}.chsql.txt"
    chsql="$(cat "$OUT/q${q}.chsql.txt")"

    # --- PG plan signals (grep -c prints 0 + exits 1 on no-match) ---
    shmscan=$(grep -c 'ClickHouseShmScan' "$OUT/q${q}.plan.txt" 2>/dev/null); shmscan=${shmscan:-0}
    pg_agg=$(grep -cE 'Aggregate|GroupAggregate|HashAggregate' "$OUT/q${q}.plan.txt" 2>/dev/null); pg_agg=${pg_agg:-0}
    pg_sort=$(grep -c 'Sort Key:' "$OUT/q${q}.plan.txt" 2>/dev/null); pg_sort=${pg_sort:-0}
    pg_lim=$(grep -cwE 'Limit' "$OUT/q${q}.plan.txt" 2>/dev/null); pg_lim=${pg_lim:-0}

    # --- CH SQL signals ---
    ch_grpby=no; echo "$chsql" | grep -qiE 'group by'        && ch_grpby=yes
    ch_ord=no;   echo "$chsql" | grep -qiE 'order by'        && ch_ord=yes
    ch_lim=no;   echo "$chsql" | grep -qiE '\blimit\b'       && ch_lim=yes
    ch_distinct="-"
    echo "$chsql" | grep -qiE 'uniqExact'                    && ch_distinct="uniqExact"
    [ "$ch_distinct" = "-" ] && { echo "$chsql" | grep -qiE 'count\(distinct|countDistinct'  && ch_distinct="count(DISTINCT)"; }
    [ "$ch_distinct" = "-" ] && { echo "$chsql" | grep -qiE '\buniq\(' && ch_distinct="uniq(APPROX)"; }

    # --- fidelity (numeric-aware) ---
    fid=$("$CMP" "$OUT/q${q}.off.out" "$OUT/q${q}.on.out" "$TOL" 2>/dev/null)
    fclass=$(echo "$fid"  | cut -d'|' -f1)
    roff=$(echo "$fid"    | cut -d'|' -f2)
    ron=$(echo "$fid"     | cut -d'|' -f3)
    maxabs=$(echo "$fid"  | cut -d'|' -f4)
    maxrel=$(echo "$fid"  | cut -d'|' -f5)
    fdetail=$(echo "$fid" | cut -d'|' -f8)
    echo "$fid" > "$OUT/q${q}.fid.txt"

    # --- verdict ---
    verdict="none"
    if [ "${shm_blocks:-0}" -ge 1 ] 2>/dev/null && [ "${strmd_q:-0}" -ge 1 ] 2>/dev/null; then
        if q_has_agg "$q"; then
            if { [ "$ch_grpby" = yes ] || echo "$chsql" | grep -qiE 'sum\(|avg\(|count\(|min\(|max\(|uniq'; } \
               && [ "${pg_agg:-0}" -eq 0 ]; then
                verdict="fully"
            else
                verdict="scan_only"
            fi
        else
            # non-aggregate projection: heavy op is the filtered scan (+ topN)
            if echo "$chsql" | grep -qiE 'where|order by|limit'; then
                verdict="fully"
            else
                verdict="scan_only"
            fi
        fi
    elif [ "${cxl_blk:-0}" -ge 1 ] 2>/dev/null; then
        # heavy fragment ran in CH (blocks adopted) but PG cancelled the stream
        # early (LIMIT without ORDER BY -> broken pipe). Effectively offloaded.
        verdict="fully(cxl)"
    elif [ "${shmscan:-0}" -ge 1 ]; then
        verdict="scan_only?"
    fi

    printf '| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s/%s |\n' \
        "$q" "$verdict" "$fclass" "$shmscan" "${strmd_q:-0}" "${shm_blocks:-0}" "${read_rows:-0}" \
        "$pg_agg" "$pg_sort" "$pg_lim" "$ch_grpby" "$ch_ord" "$ch_lim" "$ch_distinct" "$roff" "$ron" >> "$summary"
    printf 'Q%-2s %-10s fid=%-8s shmscan=%s strmd=%s blk=%s rows=%s/%s pg_agg=%s pg_sort=%s ch[g=%s o=%s l=%s] dist=%s %s\n' \
        "$q" "$verdict" "$fclass" "$shmscan" "${strmd_q:-0}" "${shm_blocks:-0}" "$roff" "$ron" \
        "$pg_agg" "$pg_sort" "$ch_grpby" "$ch_ord" "$ch_lim" "$ch_distinct" "$fdetail"
done

echo ""
echo "=== SUMMARY ($summary) ==="
cat "$summary"
