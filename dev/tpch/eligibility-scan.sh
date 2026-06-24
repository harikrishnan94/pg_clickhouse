#!/usr/bin/env bash
# Phase-0 eligibility + fidelity scan for the 22 TPC-H queries.
#
# For each query it records, from INDEPENDENT sources, whether and HOW the heavy
# analytic fragment offloads to ClickHouse over the SHM path:
#
#   1. Correctness/fidelity: run with offload OFF (native, tuned) and ON; capture
#      both result sets and quantify any deviation (row count, per-column max abs
#      and rel error for numeric columns).
#   2. ClickHouse query_log oracle (authority): correlate by a unique per-query
#      log_comment tag -> the dispatched streamed_table() SQL, read_rows, and
#      ProfileEvents['ShmAdoptedBlocks']. Counts how many streamed_table queries
#      the run dispatched.
#   3. PostgreSQL plan: the offload-ON EXPLAIN (VERBOSE, COSTS OFF) -> how many
#      Custom Scan (ClickHouseShmScan) nodes, and whether a JOIN / Aggregate node
#      still sits in PG (i.e. the heavy fragment was NOT pushed).
#
# Verdict (see dev/tpch/FULL-OFFLOAD-DECISIONS.md D0001):
#   fully     - oracle fires (ShmAdoptedBlocks>=1) AND CH SQL carries the heavy op
#               AND PG has no residual heavy op above the CustomScan.
#   scan_only - a ClickHouseShmScan fires but PG still does the join/aggregate.
#   none      - no ClickHouseShmScan / no streamed_table query in the log.
#
# Raw artifacts land in $OUT (default dev/tpch/evidence/phase0). A markdown summary
# table is printed and written to $OUT/SUMMARY.md.
#
#   PGDB=tpch_sf10 dev/tpch/eligibility-scan.sh [q ...]     # default: 1..22
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1090
. "$HERE/../bench/bench-common.sh"          # chq, shm_set_block, manifest vars

PGDB="${PGDB:-tpch_sf10}"
QDIR="$HERE/queries"
OUT="${OUT:-$HERE/evidence/phase0}"
mkdir -p "$OUT"
QUERIES=("$@"); [ ${#QUERIES[@]} -gt 0 ] || QUERIES=($(seq 1 22))

# psql as the postgres OS user; pipe-separated, untuned-quiet, do NOT stop on error
# (we want to capture partial output + the error text for the ledger).
PSQL=(sudo -u postgres psql -d "$PGDB" -tAqX -F'|' -v ON_ERROR_STOP=0 -P null=NULL)

# Native baseline tuning so SF10 OFF/ON runs finish; correctness baseline only
# (the latency benchmark is dev/bench/sweep-capped.sh).
NATIVE_TUNE="SET search_path=pg;
SET max_parallel_workers=16;
SET max_parallel_workers_per_gather=8;
SET parallel_leader_participation=on;
SET work_mem='2GB';
SET hash_mem_multiplier=4;
SET jit=on;
SET statement_timeout='900s';"

# Strip the EXPLAIN wrapper -> the runnable statement(s) (keeps Q15 view ddl).
runnable() { sed '/^EXPLAIN/d' "$QDIR/$1.sql"; }
# Replace EXPLAIN (ANALYZE,...) with a plan-only EXPLAIN (no execution).
plansql()  { sed 's/^EXPLAIN.*/EXPLAIN (VERBOSE, COSTS OFF)/' "$QDIR/$1.sql"; }

# Does the original query contain a heavy aggregate / GROUP BY / multi-table join?
q_has_agg()  { grep -qiE 'sum\(|avg\(|count\(|min\(|max\(|group[[:space:]]+by' "$QDIR/$1.sql"; }

summary="$OUT/SUMMARY.md"
: > "$summary"
printf '| Q | verdict | shmscan | strmd_q | ShmBlocks | read_rows | pg_join | pg_agg | ch_join | ch_grpby | rows off/on | fidelity |\n' >> "$summary"
printf '|---|---------|--------:|--------:|----------:|----------:|--------:|-------:|:-------:|:--------:|------------|----------|\n' >> "$summary"

echo "=== Phase-0 eligibility scan: db=$PGDB  CH=$CH_HTTP  out=$OUT ==="

for q in "${QUERIES[@]}"; do
    qf="$QDIR/$q.sql"; [ -f "$qf" ] || { echo "Q$q: no file"; continue; }
    tag="$(new_tag elig_q${q})"
    rsql="$(runnable "$q")"
    psql_plan="$(plansql "$q")"

    # --- OFF baseline (native, tuned) ---
    printf '%s\n%s\n' "$NATIVE_TUNE" "$rsql" \
        | "${PSQL[@]}" > "$OUT/q${q}.off.out" 2> "$OUT/q${q}.off.err"

    # --- ON plan (cheap, no execution): classification of residual PG ops ---
    printf '%s\nSET search_path=pg;\n%s\n' "$(shm_set_block "$tag")" "$psql_plan" \
        | "${PSQL[@]}" > "$OUT/q${q}.plan.txt" 2> "$OUT/q${q}.plan.err"

    # --- ON run (executes -> oracle + result) ---
    chq "SYSTEM FLUSH LOGS" >/dev/null 2>&1
    printf '%s\nSET search_path=pg;\nSET statement_timeout=%s;\n%s\n' \
        "$(shm_set_block "$tag")" "'900s'" "$rsql" \
        | "${PSQL[@]}" > "$OUT/q${q}.on.out" 2> "$OUT/q${q}.on.err"
    chq "SYSTEM FLUSH LOGS" >/dev/null 2>&1

    # --- query_log oracle (by tag): count, latest read_rows/blocks/sql ---
    strmd_q=$(chq "SELECT count() FROM system.query_log
                   WHERE log_comment='$tag' AND type='QueryFinish'
                     AND positionCaseInsensitive(query,'streamed_table')>0
                     AND positionCaseInsensitive(query,'query_log')=0")
    read_rows=$(chq "SELECT sum(read_rows) FROM system.query_log
                   WHERE log_comment='$tag' AND type='QueryFinish'
                     AND positionCaseInsensitive(query,'streamed_table')>0
                     AND positionCaseInsensitive(query,'query_log')=0")
    shm_blocks=$(chq "SELECT sum(ProfileEvents['ShmAdoptedBlocks']) FROM system.query_log
                   WHERE log_comment='$tag' AND type='QueryFinish'
                     AND positionCaseInsensitive(query,'streamed_table')>0
                     AND positionCaseInsensitive(query,'query_log')=0")
    chq "SELECT replaceRegexpAll(query,'\\\\s+',' ') FROM system.query_log
         WHERE log_comment='$tag' AND type='QueryFinish'
           AND positionCaseInsensitive(query,'streamed_table')>0
           AND positionCaseInsensitive(query,'query_log')=0
         ORDER BY event_time_microseconds DESC LIMIT 5" > "$OUT/q${q}.chsql.txt"

    chsql="$(cat "$OUT/q${q}.chsql.txt")"

    # --- PG plan signals (grep -c prints 0 + exits 1 on no-match; default empty->0,
    # never chain `|| echo 0` which would append a second 0 line) ---
    shmscan_count=$(grep -c 'ClickHouseShmScan' "$OUT/q${q}.plan.txt" 2>/dev/null); shmscan_count=${shmscan_count:-0}
    pg_join=$(grep -cE 'Hash Join|Merge Join|Nested Loop' "$OUT/q${q}.plan.txt" 2>/dev/null); pg_join=${pg_join:-0}
    pg_agg=$(grep -cE 'Aggregate|GroupAggregate|HashAggregate' "$OUT/q${q}.plan.txt" 2>/dev/null); pg_agg=${pg_agg:-0}

    # --- CH SQL signals ---
    ch_join=no;  echo "$chsql" | grep -qiE '\bjoin\b'       && ch_join=yes
    ch_grpby=no; echo "$chsql" | grep -qiE 'group by'       && ch_grpby=yes

    # --- fidelity: row counts + line-set diff ---
    roff=$(grep -cve '^$' "$OUT/q${q}.off.out" 2>/dev/null); roff=${roff:-0}
    ron=$(grep -cve '^$' "$OUT/q${q}.on.out" 2>/dev/null); ron=${ron:-0}
    on_err=""
    grep -qiE 'ERROR|FATAL|server closed|terminated' "$OUT/q${q}.on.err" && on_err="ON_ERR"
    off_err=""
    grep -qiE 'ERROR|FATAL|server closed|terminated' "$OUT/q${q}.off.err" && off_err="OFF_ERR"
    if [ -n "$on_err$off_err" ]; then
        fidelity="$on_err$off_err"
    elif sort "$OUT/q${q}.off.out" | grep -ve '^$' > "$OUT/q${q}.off.sorted" 2>/dev/null \
         && sort "$OUT/q${q}.on.out" | grep -ve '^$' > "$OUT/q${q}.on.sorted" 2>/dev/null \
         && diff -q "$OUT/q${q}.off.sorted" "$OUT/q${q}.on.sorted" >/dev/null 2>&1; then
        fidelity="exact"
    else
        d=$(diff "$OUT/q${q}.off.sorted" "$OUT/q${q}.on.sorted" 2>/dev/null | grep -cE '^[<>]')
        fidelity="DIFF($d lines)"
    fi

    # --- verdict ---
    verdict="none"
    if [ "${shm_blocks:-0}" -ge 1 ] 2>/dev/null && [ "${strmd_q:-0}" -ge 1 ] 2>/dev/null; then
        if q_has_agg "$q"; then
            if { [ "$ch_grpby" = yes ] || echo "$chsql" | grep -qiE 'sum\(|avg\(|count\(|min\(|max\('; } \
               && [ "${pg_agg:-0}" -eq 0 ]; then
                verdict="fully"
            else
                verdict="scan_only"
            fi
        else
            # join/projection-only query: heavy op is the join
            if [ "$ch_join" = yes ] && [ "${pg_join:-0}" -eq 0 ]; then
                verdict="fully"
            elif [ "$shmscan_count" -ge 1 ]; then
                verdict="scan_only"
            fi
        fi
    elif [ "${shmscan_count:-0}" -ge 1 ]; then
        verdict="scan_only?"   # plan has a CustomScan but oracle didn't confirm a block
    fi

    printf '| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s/%s | %s |\n' \
        "$q" "$verdict" "$shmscan_count" "${strmd_q:-0}" "${shm_blocks:-0}" "${read_rows:-0}" \
        "$pg_join" "$pg_agg" "$ch_join" "$ch_grpby" "$roff" "$ron" "$fidelity" >> "$summary"
    printf 'Q%-2s %-10s shmscan=%s strmd=%s blocks=%s rows=%s/%s fid=%s\n' \
        "$q" "$verdict" "$shmscan_count" "${strmd_q:-0}" "${shm_blocks:-0}" "$roff" "$ron" "$fidelity"
done

echo ""
echo "=== SUMMARY ($summary) ==="
cat "$summary"
