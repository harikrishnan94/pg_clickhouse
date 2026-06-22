#!/usr/bin/env bash
# TPC-H SF=10 full matrix: native PostgreSQL vs pg_clickhouse FDW pushdown vs
# pg_clickhouse SHM offload. Each queries/<i>.sql is an EXPLAIN (ANALYZE, COSTS)
# run 3x per mode; the averaged Execution Time is reported. The "offloaded?"
# column is the per-session SHOW pg_clickhouse.last_query_used_clickhouse for the
# SHM mode (so it is correct under concurrency, not a query_log recency guess).
#
# Most TPC-H queries are multi-table joins and only Q1/Q6 fully offload via SHM;
# the rest will show offloaded = (blank). That is expected -- see README.
#
# Requires the dedicated server to be running (RUN_ID); reads its manifest.
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
. "$HERE/../bench/bench-common.sh"
PG_DB="${PG_DB:-tpch_sf10}"
TIMEOUT_MS="${TIMEOUT_MS:-120000}"
mkdir -p result

# Average "Execution Time: N ms" over 3 EXPLAIN ANALYZE runs of $sqlfile, with
# the given prelude (SET statements) prepended. Echoes "avg_ms|offload_flag".
run_query() {
    local mode="$1" sqlfile="$2" prelude="$3" i out times=() flag="" ms
    local tmp; tmp="$(mktemp)"
    for i in 1 2 3; do
        out="result/${mode}$(basename "$sqlfile" .sql).$i"
        {
            printf '%s\n' "$prelude"
            cat "$sqlfile"
            [ "$mode" = shm ] && echo ";SHOW pg_clickhouse.last_query_used_clickhouse;"
        } > "$tmp"
        sudo -u "$PG_SUPER" psql -d "$PG_DB" -qAXt -v ON_ERROR_STOP=0 \
            -c "SET statement_timeout=$TIMEOUT_MS" -f "$tmp" > "$out" 2>&1
        ms=$(grep -i 'Execution Time' "$out" | grep -Eo '[0-9]+\.[0-9]+' | head -n1)
        [ -n "$ms" ] && times+=("$ms")
        [ "$mode" = shm ] && flag=$(grep -E '^(on|off|t|f|true|false)$' "$out" | tail -n1)
    done
    rm -f "$tmp"
    local avg="-"
    if [ "${#times[@]}" -gt 0 ]; then
        avg=$(sudo -u "$PG_SUPER" psql -d "$PG_DB" -tAqX -c \
          "SELECT round(avg(x))||' ms' FROM unnest(ARRAY[$(IFS=,; echo "${times[*]}")]) x")
    fi
    echo "$avg|$flag"
}

SHM_PRELUDE="SET search_path=pg; $(shm_set_block "RUNTAG")"

echo "Running TPC-H SF=10 matrix (RUN_ID=$RUN_ID, http=$CH_HTTP_PORT). This runs 22 queries x 3 modes x 3..."
timed_lock_acquire
declare -A NAT FDW SHM OFF
for i in $(seq 1 22); do
    f="queries/$i.sql"
    printf 'Q%-2s ' "$i"
    NAT[$i]=$(run_query native "$f" "SET search_path=pg; SET pg_clickhouse.enable_shm_offload=off;" | cut -d'|' -f1); printf 'n'
    FDW[$i]=$(run_query fdw    "$f" "SET search_path=ch;"                                            | cut -d'|' -f1); printf 'f'
    r=$(run_query shm "$f" "SET search_path=pg; $(shm_set_block "$(new_tag tpch_run_q$i)")"); printf 's\n'
    SHM[$i]=$(echo "$r" | cut -d'|' -f1)
    fl=$(echo "$r" | cut -d'|' -f2)
    case "$fl" in on|t|true) OFF[$i]='✔︎';; *) OFF[$i]=' ';; esac
done
timed_lock_release

{
  echo ""
  echo "|    Query   | PostgreSQL |  FDW pushdown | SHM offload | offloaded? |"
  echo "| ----------:| ----------:| -------------:| -----------:|:----------:|"
  for i in $(seq 1 22); do
    printf "| %10s | %10s | %13s | %11s |     %s     |\n" "[Query $i]" "${NAT[$i]:-?}" "${FDW[$i]:-?}" "${SHM[$i]:-?}" "${OFF[$i]:- }"
  done
} | tee result/RESULTS.md
