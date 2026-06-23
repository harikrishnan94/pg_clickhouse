#!/usr/bin/env bash
# Correctness for the column-major (struct-of-arrays) batch deform
# (pg_clickhouse.shm_columnar_deform). For each case the result with offload ON
# (column-major reader) must equal the baseline with offload OFF, and the offload
# must actually engage (last_query_used_clickhouse = on).
#
# Cases:
#   vnn  - nullable NON-projected column (~1/3 NULL) between two projected NOT NULL
#          columns: pages with a NULL row split into group B (row-major), the rest
#          group A (column-major); the projected column AFTER the NULL must read
#          correctly on both. Keystone for the visibility splice.
#   svar - projected String column (incl. empty strings): group A string SoA fill.
#   bigi - > rows_per_block (65536) rows: group A crossing the block-flush boundary.
#
# Requires the ch_bench server: RUN_ID=tpchcb dev/bench/ch-bench-server.sh start
# Env: PGDB (tpch_sf10), CH_SERVER (ch_bench)
set -uo pipefail

PGDB="${PGDB:-tpch_sf10}"
CH_SERVER="${CH_SERVER:-ch_bench}"
PASS=0
FAIL=0
PSQL=(sudo -u postgres psql -d "$PGDB" -tAqX -v ON_ERROR_STOP=1)

OFFLOAD_ON="LOAD 'pg_clickhouse';
SET pg_clickhouse.local_ch_server = '${CH_SERVER}';
SET pg_clickhouse.shm_min_rows = 0;
SET pg_clickhouse.session_settings = 'allow_experimental_streamed_table_function 1, max_threads 1';
SET pg_clickhouse.shm_vectorized_reader = on;
SET pg_clickhouse.shm_columnar_deform = on;
SET pg_clickhouse.enable_shm_offload = on;"

# $1 = label, $2 = query
check() {
  local label="$1" q="$2" off on used
  off="$("${PSQL[@]}" -c "SET pg_clickhouse.enable_shm_offload = off; $q")"
  on="$(printf '%s\n%s\n' "$OFFLOAD_ON" "$q" | "${PSQL[@]}")"
  used="$(printf '%s\n%s\nSHOW pg_clickhouse.last_query_used_clickhouse;\n' "$OFFLOAD_ON" "$q" | "${PSQL[@]}" | tail -1)"
  if [ "$off" = "$on" ] && [ "$used" = "on" ]; then
    PASS=$((PASS+1)); printf '  PASS  %-22s off==on, offloaded  [%s]\n' "$label" "$on"
  else
    FAIL=$((FAIL+1)); printf '  FAIL  %-22s off=[%s] on=[%s] offloaded=%s\n' "$label" "$off" "$on" "$used"
  fi
}

echo "=== column-major deform correctness (db=${PGDB}) ==="

# --- vnn: nullable non-projected column forces per-page A/B split ---
"${PSQL[@]}" -c "DROP TABLE IF EXISTS vnn;" >/dev/null
"${PSQL[@]}" -c "CREATE TABLE vnn (k bigint NOT NULL, opt text, payload bigint NOT NULL);" >/dev/null
"${PSQL[@]}" -c "INSERT INTO vnn SELECT g, CASE WHEN g % 3 = 0 THEN NULL ELSE 'x'||g END, g*2 FROM generate_series(1,20000) g;" >/dev/null
"${PSQL[@]}" -c "ANALYZE vnn;" >/dev/null
# Query references only k and payload (both NOT NULL); opt (nullable, ~1/3 NULL) is
# non-projected. payload is attno 3, after the nullable opt (attno 2).
check "vnn_split"          "SELECT count(*), sum(k), sum(payload), min(payload), max(payload) FROM vnn;"
check "vnn_split_filter"   "SELECT count(*), sum(payload) FROM vnn WHERE k > 5000;"

# --- svar: projected String column, incl. empty strings (group A SoA) ---
"${PSQL[@]}" -c "DROP TABLE IF EXISTS svar;" >/dev/null
"${PSQL[@]}" -c "CREATE TABLE svar (id bigint NOT NULL, s text NOT NULL);" >/dev/null
"${PSQL[@]}" -c "INSERT INTO svar SELECT g, CASE WHEN g % 4 = 0 THEN '' ELSE repeat('z', (g % 37)+1) END FROM generate_series(1,20000) g;" >/dev/null
"${PSQL[@]}" -c "ANALYZE svar;" >/dev/null
check "svar_string"        "SELECT count(*), sum(id), max(s), min(s) FROM svar;"
check "svar_groupby"       "SELECT s, count(*) FROM svar GROUP BY s ORDER BY s LIMIT 5;"

# --- bigi: > rows_per_block, all NOT NULL (group A multi-page + flush straddle) ---
"${PSQL[@]}" -c "DROP TABLE IF EXISTS bigi;" >/dev/null
"${PSQL[@]}" -c "CREATE TABLE bigi (id bigint NOT NULL, v bigint NOT NULL, s text NOT NULL);" >/dev/null
"${PSQL[@]}" -c "INSERT INTO bigi SELECT g, g*2, 'r'||(g%97) FROM generate_series(1,200000) g;" >/dev/null
"${PSQL[@]}" -c "ANALYZE bigi;" >/dev/null
check "bigi_straddle"      "SELECT count(*), sum(id), sum(v), max(s), min(s) FROM bigi;"

# ===== group B (NULL-aware column-major C++ driver) =====================
# All projected columns are NOT NULL; NULLs live only in non-projected columns.

# --- bnull_allpages: nullable non-projected col NULL in ~1/2 of rows so EVERY
#     page is group B; projected col (v) comes AFTER the nullable col (offset shift). ---
"${PSQL[@]}" -c "DROP TABLE IF EXISTS bnull;" >/dev/null
"${PSQL[@]}" -c "CREATE TABLE bnull (k bigint NOT NULL, opt bigint, v bigint NOT NULL);" >/dev/null
"${PSQL[@]}" -c "INSERT INTO bnull SELECT g, CASE WHEN g % 2 = 0 THEN NULL ELSE g END, g*3 FROM generate_series(1,20000) g;" >/dev/null
"${PSQL[@]}" -c "ANALYZE bnull;" >/dev/null
check "bnull_allpages"     "SELECT count(*), sum(k), sum(v), min(v), max(v) FROM bnull;"
check "bnull_filter"       "SELECT count(*), sum(v) FROM bnull WHERE k > 7500;"

# --- nullbefore_fixed: nullable FIXED col before projected cols => prefix_len_b = 0. ---
"${PSQL[@]}" -c "DROP TABLE IF EXISTS nbf;" >/dev/null
"${PSQL[@]}" -c "CREATE TABLE nbf (opt int, a bigint NOT NULL, b bigint NOT NULL);" >/dev/null
"${PSQL[@]}" -c "INSERT INTO nbf SELECT CASE WHEN g % 3 = 0 THEN NULL ELSE g END, g, g*2 FROM generate_series(1,20000) g;" >/dev/null
"${PSQL[@]}" -c "ANALYZE nbf;" >/dev/null
check "nullbefore_fixed"   "SELECT count(*), sum(a), sum(b) FROM nbf;"

# --- nullbefore_var: nullable VARLENA before projected cols (nullable varlena advance). ---
"${PSQL[@]}" -c "DROP TABLE IF EXISTS nbv;" >/dev/null
"${PSQL[@]}" -c "CREATE TABLE nbv (opt text, a bigint NOT NULL, b bigint NOT NULL);" >/dev/null
"${PSQL[@]}" -c "INSERT INTO nbv SELECT CASE WHEN g % 3 = 0 THEN NULL ELSE 'o'||g END, g, g*2 FROM generate_series(1,20000) g;" >/dev/null
"${PSQL[@]}" -c "ANALYZE nbv;" >/dev/null
check "nullbefore_var"     "SELECT count(*), sum(a), sum(b) FROM nbv;"

# --- projafternull: keystone -- project a fixed col AND a string col both AFTER a
#     nullable varlena col (per-row offset shift on multiple projected cols). ---
"${PSQL[@]}" -c "DROP TABLE IF EXISTS pan;" >/dev/null
"${PSQL[@]}" -c "CREATE TABLE pan (k bigint NOT NULL, opt text, payload bigint NOT NULL, tail text NOT NULL);" >/dev/null
"${PSQL[@]}" -c "INSERT INTO pan SELECT g, CASE WHEN g % 3 = 0 THEN NULL ELSE 'x'||g END, g*5, 't'||(g%53) FROM generate_series(1,20000) g;" >/dev/null
"${PSQL[@]}" -c "ANALYZE pan;" >/dev/null
check "projafternull"      "SELECT count(*), sum(k), sum(payload), max(tail), min(tail) FROM pan;"

# --- strnull_mix: project a string col after a nullable varlena; strings incl. empty. ---
"${PSQL[@]}" -c "DROP TABLE IF EXISTS snm;" >/dev/null
"${PSQL[@]}" -c "CREATE TABLE snm (id bigint NOT NULL, opt text, s text NOT NULL);" >/dev/null
"${PSQL[@]}" -c "INSERT INTO snm SELECT g, CASE WHEN g % 3 = 0 THEN NULL ELSE 'o'||g END, CASE WHEN g % 5 = 0 THEN '' ELSE repeat('s',(g%29)+1) END FROM generate_series(1,20000) g;" >/dev/null
"${PSQL[@]}" -c "ANALYZE snm;" >/dev/null
check "strnull_mix"        "SELECT count(*), sum(id), max(s), min(s) FROM snm;"

# --- bnull_big: > rows_per_block group B (flush-boundary split inside the nullable driver). ---
"${PSQL[@]}" -c "DROP TABLE IF EXISTS bnb;" >/dev/null
"${PSQL[@]}" -c "CREATE TABLE bnb (id bigint NOT NULL, opt bigint, v bigint NOT NULL);" >/dev/null
"${PSQL[@]}" -c "INSERT INTO bnb SELECT g, CASE WHEN g % 2 = 0 THEN NULL ELSE g END, g*2 FROM generate_series(1,200000) g;" >/dev/null
"${PSQL[@]}" -c "ANALYZE bnb;" >/dev/null
check "bnull_big"          "SELECT count(*), sum(id), sum(v) FROM bnb;"

"${PSQL[@]}" -c "DROP TABLE IF EXISTS vnn; DROP TABLE IF EXISTS svar; DROP TABLE IF EXISTS bigi; DROP TABLE IF EXISTS bnull; DROP TABLE IF EXISTS nbf; DROP TABLE IF EXISTS nbv; DROP TABLE IF EXISTS pan; DROP TABLE IF EXISTS snm; DROP TABLE IF EXISTS bnb;" >/dev/null

echo "================================================================"
echo "PASS=${PASS}  FAIL=${FAIL}"
[ "$FAIL" -eq 0 ] && echo "ALL COLUMNAR CHECKS PASSED" || { echo "COLUMNAR CHECKS FAILED"; exit 1; }
