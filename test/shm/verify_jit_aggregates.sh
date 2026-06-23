#!/usr/bin/env bash
# Regression test for the JIT tuple-deform corruption in the SHM base-scan
# offload (fixed by giving the base CustomScan an explicit custom_scan_tlist).
#
# The bug: a base offload scan streams only the projected columns but described
# its scan tuple with the whole heap relation's descriptor. Un-projected columns
# were left as phantom NULLs; when such a column is declared NOT NULL, JIT's
# slot_compile_deform treats it as guaranteed-present and skips its null-bitmap
# check, then reads every following column at the wrong offset -- silently
# returning garbage (e.g. sum -> 4.7e25, a float -> NaN, a date -> year 5560075).
# Interpreted deform reads the bitmap and is unaffected, so the corruption only
# appeared once JIT engaged (above its cost threshold) -- which is why it showed
# up at 4+ aggregates / large scans but not in small smoke tests.
#
# Each case forces JIT on (jit_above_cost=0) on BOTH sides so the offloaded
# JIT-deformed result is compared against the non-offloaded result deterministically,
# regardless of data size. A passing case proves off==on AND that the scan really
# offloaded (last_query_used_clickhouse=on). The mixed-aggregate cases (count +
# sum(int8->numeric) + sum(float8) + min/max(date)) decline the grouped push-down
# on the numeric output, so PostgreSQL aggregates the streamed base-scan rows --
# exactly the path the bug corrupted.
#
# Requires the ch_bench server: RUN_ID=tpchcb dev/bench/ch-bench-server.sh start
# Env: PGDB (tpch_sf10), CH_SERVER (ch_bench)
set -uo pipefail

PGDB="${PGDB:-tpch_sf10}"
CH_SERVER="${CH_SERVER:-ch_bench}"
PASS=0
FAIL=0
PSQL=(sudo -u postgres psql -d "$PGDB" -tAqX -v ON_ERROR_STOP=1)

# Force JIT on at zero cost so the JIT deform path is exercised on every query,
# independent of table size -- this is what makes the regression deterministic.
JIT="SET jit = on;
SET jit_above_cost = 0;
SET jit_optimize_above_cost = 0;
SET jit_inline_above_cost = 0;"

OFFLOAD_OFF="${JIT}
SET pg_clickhouse.enable_shm_offload = off;"

OFFLOAD_ON="LOAD 'pg_clickhouse';
SET pg_clickhouse.local_ch_server = '${CH_SERVER}';
SET pg_clickhouse.shm_min_rows = 0;
SET pg_clickhouse.session_settings = 'allow_experimental_streamed_table_function 1, max_threads 1';
${JIT}
SET pg_clickhouse.enable_shm_offload = on;"

# $1 = label, $2 = query
check() {
  local label="$1" q="$2" off on used
  off="$(printf '%s\n%s\n' "$OFFLOAD_OFF" "$q" | "${PSQL[@]}")"
  on="$(printf '%s\n%s\n' "$OFFLOAD_ON" "$q" | "${PSQL[@]}")"
  used="$(printf '%s\n%s\nSHOW pg_clickhouse.last_query_used_clickhouse;\n' "$OFFLOAD_ON" "$q" | "${PSQL[@]}" | tail -1)"
  if [ "$off" = "$on" ] && [ "$used" = "on" ]; then
    PASS=$((PASS+1)); printf '  PASS  %-24s off==on, offloaded  [%s]\n' "$label" "$on"
  else
    FAIL=$((FAIL+1)); printf '  FAIL  %-24s off=[%s] on=[%s] offloaded=%s\n' "$label" "$off" "$on" "$used"
  fi
}

echo "=== JIT base-scan deform correctness (db=${PGDB}, jit forced on) ==="

# --- t4: leading NOT NULL column (id) is NOT projected -> phantom NULL on a
#     NOT NULL attribute, the exact shape that broke JIT deform. ---
"${PSQL[@]}" -c "DROP TABLE IF EXISTS jit_t4;" >/dev/null
"${PSQL[@]}" -c "CREATE TABLE jit_t4 (id bigint NOT NULL, a bigint NOT NULL, b float8 NOT NULL, d date NOT NULL);" >/dev/null
"${PSQL[@]}" -c "INSERT INTO jit_t4 SELECT g, g, g*1.5, date '2000-01-01'+(g%3000) FROM generate_series(1,200000) g;" >/dev/null
"${PSQL[@]}" -c "ANALYZE jit_t4;" >/dev/null
# --- Row-projection cases: an offloaded base scan feeding a PostgreSQL consumer
#     (md5 over string_agg) ALWAYS exercises the base-scan JIT deform, regardless
#     of whether an aggregate would otherwise push down. These are the primary,
#     deterministic guards for the deform-offset corruption. Each fences the scan
#     in a subquery so only the projection -- not the aggregate -- is offloaded.
check "rows_abd"           "SELECT md5(string_agg(t::text, ';' ORDER BY a)) FROM (SELECT a, b, d FROM jit_t4 WHERE a BETWEEN 1 AND 50000) t(a,b,d);"
check "rows_only_d"        "SELECT md5(string_agg(t::text, ';' ORDER BY d)) FROM (SELECT d FROM jit_t4 WHERE a BETWEEN 1 AND 50000) t(d);"
check "rows_only_b"        "SELECT md5(string_agg(t::text, ';' ORDER BY b)) FROM (SELECT b FROM jit_t4 WHERE a BETWEEN 1 AND 50000) t(b);"
check "rows_bd"            "SELECT md5(string_agg(t::text, ';' ORDER BY b)) FROM (SELECT b, d FROM jit_t4 WHERE a BETWEEN 1 AND 50000) t(b,d);"

# --- Mixed-aggregate cases (the user-reported shape). A CTE MATERIALIZED fence
#     forces PostgreSQL to aggregate the offloaded base-scan rows under JIT, which
#     is exactly the path the bug corrupted -- deterministically, independent of
#     whether the bare query would decline or push down the grouped aggregate.
#     count(*) + sum(int8->numeric) + sum(float8) + min/max(date) = 4+ mixed types.
check "agg_count_4"        "WITH s AS MATERIALIZED (SELECT a, b, d FROM jit_t4 WHERE a >= 0) SELECT count(*), sum(a), sum(b), max(d) FROM s;"
check "agg_dates_4"        "WITH s AS MATERIALIZED (SELECT a, b, d FROM jit_t4 WHERE a >= 0) SELECT sum(a), sum(b), max(d), min(d) FROM s;"
check "agg_nodate_4"       "WITH s AS MATERIALIZED (SELECT a, b, d FROM jit_t4 WHERE a >= 0) SELECT sum(a), sum(b), min(a), max(a) FROM s;"
check "agg_only_d_3"       "WITH s AS MATERIALIZED (SELECT a, d FROM jit_t4 WHERE a >= 0) SELECT count(*), max(d), min(d) FROM s;"

# --- The bare query too (real planner path; on the shipped build the numeric
#     output declines the grouped push-down, so PostgreSQL aggregates the base scan). ---
check "bare_count_4"       "SELECT count(*), sum(a), sum(b), max(d) FROM jit_t4 WHERE a >= 0;"
check "bare_3agg"          "SELECT sum(a), sum(b), max(d) FROM jit_t4 WHERE a >= 0;"

# --- Control: project id too (all columns streamed, no phantom NULL). ---
check "ctl_proj_id"        "SELECT count(*), sum(a), sum(b), max(d), max(id) FROM jit_t4 WHERE a >= 0;"

# --- mid: the un-projected NOT NULL column sits between projected columns, so a
#     wrong offset corrupts the columns that follow it. ---
"${PSQL[@]}" -c "DROP TABLE IF EXISTS jit_mid;" >/dev/null
"${PSQL[@]}" -c "CREATE TABLE jit_mid (a bigint NOT NULL, skip bigint NOT NULL, b float8 NOT NULL, d date NOT NULL);" >/dev/null
"${PSQL[@]}" -c "INSERT INTO jit_mid SELECT g, g*7, g*1.5, date '2000-01-01'+(g%3000) FROM generate_series(1,200000) g;" >/dev/null
"${PSQL[@]}" -c "ANALYZE jit_mid;" >/dev/null
# Projects a, b, d; 'skip' (NOT NULL, attno 2) is the phantom NULL between them.
check "mid_rows"           "SELECT md5(string_agg(t::text, ';' ORDER BY a)) FROM (SELECT a, b, d FROM jit_mid WHERE a BETWEEN 1 AND 50000) t(a,b,d);"
check "mid_agg_4"          "WITH s AS MATERIALIZED (SELECT a, b, d FROM jit_mid WHERE a >= 0) SELECT count(*), sum(a), sum(b), max(d) FROM s;"
check "mid_agg_dates"      "WITH s AS MATERIALIZED (SELECT a, b, d FROM jit_mid WHERE a >= 0) SELECT sum(a), sum(b), max(d), min(d) FROM s;"

"${PSQL[@]}" -c "DROP TABLE IF EXISTS jit_t4; DROP TABLE IF EXISTS jit_mid;" >/dev/null

echo "--------------------------------------------------------"
echo "PASS=$PASS FAIL=$FAIL"
[ "$FAIL" -eq 0 ]
