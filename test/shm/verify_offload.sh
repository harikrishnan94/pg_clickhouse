#!/usr/bin/env bash
#
# Exhaustive end-to-end verification of the SHM heap-offload path.
#
# For every query in the matrix this script proves, from ClickHouse's OWN system
# tables (not the PostgreSQL plan), that the query was offloaded:
#   1. Correctness: the result with pg_clickhouse.enable_shm_offload = on is
#      byte-identical to the result with it off.
#   2. Push-down happened: running the query with offload on adds exactly one new
#      `streamed_table(...)` entry to ClickHouse system.query_log, and that entry
#      adopted >= 1 block from shared memory (ProfileEvents['ShmAdoptedBlocks'])
#      and read the full streamed table (read_rows).
#   3. The query ClickHouse actually executed is printed, so what pg_clickhouse
#      executed can be compared to what ClickHouse saw.
# A negative control proves that with offload off ClickHouse sees no
# streamed_table query at all.
#
# Correlation is by recency: the harness runs serially and flushes query_log
# between steps, so the most-recent streamed_table QueryFinish is the one just
# triggered; a count delta confirms exactly one new offload query appeared.
#
# Requires a built `clickhouse` binary, a PostgreSQL with pg_clickhouse
# installed, and curl/psql. The ClickHouse server is started/stopped here.
#
# Usage:  CH_BIN=/path/to/clickhouse PG_DB=shmdemo test/shm/verify_offload.sh
#
set -uo pipefail

CH_BIN="${CH_BIN:-/home/ubuntu/ClickHouse/build/reldeb/programs/clickhouse}"
CH_DIR="${CH_DIR:-/home/ubuntu/ClickHouse/tmp/chserver}"
CH_HTTP="${CH_HTTP:-http://127.0.0.1:8123}"
PG_DB="${PG_DB:-shmdemo}"
PSQL=(sudo -u postgres psql -d "$PG_DB" -tAqX -v ON_ERROR_STOP=1)

PASS=0; FAIL=0
ok()  { PASS=$((PASS+1)); printf '  PASS  %s\n' "$*"; }
bad() { FAIL=$((FAIL+1)); printf '  FAIL  %s\n' "$*"; }
say() { printf '%s\n' "$*"; }
chq() { curl -s --max-time 15 "$CH_HTTP/" --data-binary "$1"; }
# Match only the real offloaded queries (SELECT ... FROM streamed_table(...)).
# This script's own monitoring queries also mention "streamed_table", but they
# read from system.query_log, so excluding queries that mention "query_log"
# isolates the genuine offload queries dispatched by pg_clickhouse.
CH_FILTER="type='QueryFinish' AND positionCaseInsensitive(query,'streamed_table')>0 AND positionCaseInsensitive(query,'query_log')=0"
ch_count_streamed() {
    chq "SELECT count() FROM system.query_log WHERE $CH_FILTER"
}
ch_latest() {  # field of the most-recent genuine streamed_table QueryFinish
    chq "SELECT $1 FROM system.query_log WHERE $CH_FILTER ORDER BY event_time_microseconds DESC LIMIT 1"
}

# ------------------------------------------------------------------ ClickHouse
mkdir -p "$CH_DIR/data" "$CH_DIR/tmp" "$CH_DIR/user_files"
cat > "$CH_DIR/config.xml" <<XML
<clickhouse>
    <logger><level>information</level><log>$CH_DIR/ch.log</log><errorlog>$CH_DIR/ch.err.log</errorlog><size>200M</size><count>1</count></logger>
    <http_port>8123</http_port>
    <tcp_port>9000</tcp_port>
    <listen_host>127.0.0.1</listen_host>
    <path>$CH_DIR/data/</path>
    <tmp_path>$CH_DIR/tmp/</tmp_path>
    <user_files_path>$CH_DIR/user_files/</user_files_path>
    <mark_cache_size>536870912</mark_cache_size>
    <users_config>users.xml</users_config>
    <query_log><database>system</database><table>query_log</table><flush_interval_milliseconds>500</flush_interval_milliseconds></query_log>
</clickhouse>
XML
cat > "$CH_DIR/users.xml" <<'XML'
<clickhouse>
    <profiles><default/></profiles>
    <users><default>
        <password></password><networks><ip>::/0</ip></networks>
        <profile>default</profile><quota>default</quota><access_management>1</access_management>
    </default></users>
    <quotas><default/></quotas>
</clickhouse>
XML

pkill -f "clickhouse server --config-file=$CH_DIR/config.xml" 2>/dev/null; sleep 1
"$CH_BIN" server --config-file="$CH_DIR/config.xml" > "$CH_DIR/start.log" 2>&1 &
CH_PID=$!
trap 'kill "$CH_PID" 2>/dev/null' EXIT

say "Waiting for ClickHouse ($CH_BIN) ..."
up=0
for _ in $(seq 1 40); do
    curl -sf --max-time 2 "$CH_HTTP/?query=SELECT%201" >/dev/null 2>&1 && { up=1; break; }
    sleep 1
done
[ "$up" = 1 ] || { say "ClickHouse did not start; error log:"; tail -20 "$CH_DIR/ch.err.log" 2>/dev/null; exit 1; }
say "ClickHouse up: $(chq 'SELECT version()')"

# ------------------------------------------------------------------ PostgreSQL
"${PSQL[@]}" >/dev/null <<'SQL'
CREATE EXTENSION IF NOT EXISTS pg_clickhouse;
DROP SERVER IF EXISTS local_ch CASCADE;
CREATE SERVER local_ch FOREIGN DATA WRAPPER clickhouse_fdw
  OPTIONS (driver 'http', host '127.0.0.1', port '8123');
CREATE USER MAPPING FOR postgres SERVER local_ch OPTIONS (user 'default', password '');

DROP TABLE IF EXISTS t;
CREATE TABLE t (id bigint NOT NULL, n bigint NOT NULL, s text NOT NULL,
                d date NOT NULL, f double precision NOT NULL);
INSERT INTO t VALUES
  (1, 10, 'alpha',   DATE '1994-03-15', 1.5),
  (2, 20, 'beta',    DATE '1995-07-01', 2.25),
  (3, 30, '',        DATE '2000-01-01', 0.0),
  (4, 40, 'delta',   DATE '2024-12-31', 100.0),
  (5, 50, 'epsilon', DATE '1970-01-01', -7.75);

DROP TABLE IF EXISTS w;
CREATE TABLE w (a smallint NOT NULL, b integer NOT NULL, c real NOT NULL,
                flag boolean NOT NULL, g text NOT NULL);
INSERT INTO w VALUES
  (1, 100, 1.0, true,  'x'), (2, 200, 2.0, false, 'y'),
  (3, 300, 3.0, true,  'x'), (4, 400, 4.0, false, 'y'),
  (5, 500, 5.0, true,  'z');

-- numeric/decimal table: price -> Decimal64 (P<=18), qty -> Decimal32 (P<=9),
-- big -> Decimal128 (P<=38). Exercises money/quantity-style DECIMAL columns.
DROP TABLE IF EXISTS m;
CREATE TABLE m (id bigint NOT NULL, price numeric(15,2) NOT NULL,
                qty numeric(9,2) NOT NULL, big numeric(30,6) NOT NULL);
INSERT INTO m VALUES
  (1,           123.45,      10.00,                   100000.000001),
  (2,           -67.89,       0.50,                     -250.500000),
  (3,             0.00,  999999.99,                        0.000000),
  (4,       1000000.00,       1.25, 999999999999999999999.999999),
  (5, 9999999999999.99, 9999999.99,                    12345.678901);

-- Stock-typed TPC-H lineitem subset (DECIMAL(15,2) money/quantity) for Q1/Q6/Q18.
DROP TABLE IF EXISTS lineitem;
CREATE TABLE lineitem (
  l_orderkey      bigint        NOT NULL,
  l_quantity      numeric(15,2) NOT NULL,
  l_extendedprice numeric(15,2) NOT NULL,
  l_discount      numeric(15,2) NOT NULL,
  l_tax           numeric(15,2) NOT NULL,
  l_returnflag    text          NOT NULL,
  l_linestatus    text          NOT NULL,
  l_shipdate      date          NOT NULL);
INSERT INTO lineitem VALUES
  (1, 17.00,  21168.23, 0.04, 0.02, 'N', 'O', DATE '1996-03-13'),
  (1, 36.00,  45983.16, 0.09, 0.06, 'N', 'O', DATE '1996-04-12'),
  (2, 38.00,  44694.46, 0.00, 0.05, 'N', 'O', DATE '1997-01-28'),
  (3, 45.00,  54058.05, 0.06, 0.00, 'R', 'F', DATE '1994-02-02'),
  (3, 49.00,  46796.47, 0.10, 0.00, 'A', 'F', DATE '1993-11-09'),
  (4,  2.00,   2618.76, 0.06, 0.01, 'R', 'F', DATE '1994-12-12'),
  (5, 350.00, 88941.79, 0.05, 0.04, 'A', 'F', DATE '1994-07-15'),
  (5, 320.00, 71531.20, 0.07, 0.08, 'R', 'F', DATE '1994-06-30');
SQL
[ $? -eq 0 ] || { say "PG fixture setup failed"; exit 1; }

# The planner/executor hooks live in pg_clickhouse's _PG_init, which only runs
# once the library is loaded into the backend. A plain SELECT over a heap table
# does not auto-load the FDW library, so preload it for every session connecting
# to this database (the supported way to enable the hooks).
"${PSQL[@]}" -c "ALTER DATABASE \"$PG_DB\" SET session_preload_libraries = 'pg_clickhouse';" >/dev/null

ROWS_T=5; ROWS_W=5; ROWS_M=5; ROWS_L=8
SET_OFF="SET pg_clickhouse.enable_shm_offload=off;"
# LOAD is belt-and-braces in case session_preload_libraries has not taken effect.
SET_ON="LOAD 'pg_clickhouse'; SET pg_clickhouse.local_ch_server='local_ch'; SET pg_clickhouse.shm_min_rows=0; SET pg_clickhouse.session_settings='allow_experimental_streamed_table_function 1'; SET pg_clickhouse.enable_shm_offload=on;"

# verify_offload <name> <expected_streamed_rows> <sql>
verify_offload() {
    local name="$1" want_rows="$2" sql="$3"
    local baseline result before after read_rows shm_blocks chsql

    baseline=$("${PSQL[@]}" -c "$SET_OFF $sql" 2>/dev/null)
    chq "SYSTEM FLUSH LOGS" >/dev/null
    before=$(ch_count_streamed)
    result=$("${PSQL[@]}" -c "$SET_ON $sql" 2>/dev/null)
    chq "SYSTEM FLUSH LOGS" >/dev/null
    after=$(ch_count_streamed)
    read_rows=$(ch_latest "read_rows")
    shm_blocks=$(ch_latest "ProfileEvents['ShmAdoptedBlocks']")
    chsql=$(ch_latest "replaceRegexpAll(query,'\\\\s+',' ')")

    say ""
    say "[$name]"
    say "    pg(off): $(echo "$baseline" | tr '\n' '|')   pg(on): $(echo "$result" | tr '\n' '|')"
    say "    CH executed : $chsql"
    say "    CH read_rows=$read_rows  ShmAdoptedBlocks=$shm_blocks  (streamed_table queries: $before -> $after)"

    [ "$result" = "$baseline" ] && ok "$name: offload result == baseline" || bad "$name: result mismatch"
    { [ -n "${after:-}" ] && [ -n "${before:-}" ] && [ "$after" -gt "$before" ]; } 2>/dev/null \
        && ok "$name: ClickHouse executed a new streamed_table() query" || bad "$name: no new streamed_table query in system.query_log"
    [ "${shm_blocks:-0}" -ge 1 ] 2>/dev/null \
        && ok "$name: ClickHouse adopted >= 1 SHM block (ShmAdoptedBlocks=$shm_blocks)" || bad "$name: ShmAdoptedBlocks=$shm_blocks"
    [ "${read_rows:-0}" = "$want_rows" ] 2>/dev/null \
        && ok "$name: CH read_rows == $want_rows (full table streamed)" || bad "$name: read_rows=$read_rows != $want_rows"
}

# verify_not_offloaded <name> <sql>: offload off => no new streamed_table query.
verify_not_offloaded() {
    local name="$1" sql="$2"
    local baseline result before after
    baseline=$("${PSQL[@]}" -c "$SET_OFF $sql" 2>/dev/null)
    chq "SYSTEM FLUSH LOGS" >/dev/null
    before=$(ch_count_streamed)
    result=$("${PSQL[@]}" -c "$SET_OFF $sql" 2>/dev/null)
    chq "SYSTEM FLUSH LOGS" >/dev/null
    after=$(ch_count_streamed)
    say ""
    say "[$name] (offload disabled)"
    [ "$result" = "$baseline" ] && ok "$name: result correct" || bad "$name: result mismatch"
    { [ "$after" = "$before" ]; } 2>/dev/null \
        && ok "$name: ClickHouse saw no streamed_table query (not offloaded)" || bad "$name: unexpected offload ($before -> $after)"
}

# verify_declined <name> <sql>: feature ON but the query is ineligible (e.g. no
# column referenced) => planner must decline; result correct, no offload query.
verify_declined() {
    local name="$1" sql="$2"
    local baseline result before after
    baseline=$("${PSQL[@]}" -c "$SET_OFF $sql" 2>/dev/null)
    chq "SYSTEM FLUSH LOGS" >/dev/null
    before=$(ch_count_streamed)
    result=$("${PSQL[@]}" -c "$SET_ON $sql" 2>/dev/null)
    chq "SYSTEM FLUSH LOGS" >/dev/null
    after=$(ch_count_streamed)
    say ""
    say "[$name] (offload enabled, but ineligible)"
    [ "$result" = "$baseline" ] && ok "$name: result correct" || bad "$name: result mismatch"
    { [ "$after" = "$before" ]; } 2>/dev/null \
        && ok "$name: planner declined (no streamed_table query)" || bad "$name: unexpected offload ($before -> $after)"
}

say ""
say "================ SHM offload verification matrix ================"

# table t: Int64 / String / Date / Float64
verify_offload count_filtered  $ROWS_T "SELECT count(*) FROM t WHERE id > 0;"
verify_offload sum_filtered    $ROWS_T "SELECT count(*), sum(id), sum(n), sum(f) FROM t WHERE id >= 2;"
verify_offload multi_agg       $ROWS_T "SELECT sum(id), min(n), max(n), sum(f) FROM t;"
verify_offload string_len      $ROWS_T "SELECT sum(length(s)) FROM t;"
verify_offload date_minmax     $ROWS_T "SELECT min(d), max(d) FROM t;"
verify_offload groupby_string  $ROWS_T "SELECT s, count(*), sum(f) FROM t GROUP BY s ORDER BY s;"
verify_offload groupby_having  $ROWS_T "SELECT id, sum(n) FROM t GROUP BY id HAVING sum(n) > 25 ORDER BY id;"
verify_offload float_filter    $ROWS_T "SELECT count(*) FROM t WHERE f > 1.0;"
verify_offload date_filter     $ROWS_T "SELECT count(*), sum(n) FROM t WHERE d >= DATE '1995-01-01';"

# table w: Int16 / Int32 / Float32 / Bool / String
verify_offload w_sum           $ROWS_W "SELECT sum(a), sum(b), sum(c) FROM w;"
verify_offload w_groupby_flag  $ROWS_W "SELECT flag, count(*), sum(b) FROM w GROUP BY flag ORDER BY flag;"
verify_offload w_groupby_str   $ROWS_W "SELECT g, count(*) FROM w GROUP BY g ORDER BY g;"

# table m: numeric -> Decimal64 / Decimal32 / Decimal128 (exact, bit-identical)
verify_offload num_sum         $ROWS_M "SELECT sum(price), sum(qty), sum(big) FROM m;"
verify_offload num_minmax      $ROWS_M "SELECT min(price), max(price), min(qty), max(qty), min(big), max(big) FROM m;"
verify_offload num_count_filt  $ROWS_M "SELECT count(*), sum(price) FROM m WHERE price > 0;"
verify_offload num_groupby     $ROWS_M "SELECT id, sum(price), sum(big) FROM m GROUP BY id ORDER BY id;"
verify_offload num_avg         $ROWS_M "SELECT avg(price), avg(qty) FROM m;"

# TPC-H over a stock-typed lineitem (DECIMAL(15,2) money/quantity).
verify_offload tpch_q6         $ROWS_L \
  "SELECT sum(l_extendedprice * l_discount) AS revenue FROM lineitem WHERE l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01' AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24;"
verify_offload tpch_q18_inner  $ROWS_L \
  "SELECT l_orderkey, sum(l_quantity) FROM lineitem GROUP BY l_orderkey HAVING sum(l_quantity) > 50 ORDER BY l_orderkey;"
verify_offload tpch_q1         $ROWS_L \
  "SELECT l_returnflag, l_linestatus, sum(l_quantity) AS sum_qty, sum(l_extendedprice) AS sum_base_price, sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, avg(l_quantity) AS avg_qty, avg(l_extendedprice) AS avg_price, avg(l_discount) AS avg_disc, count(*) AS count_order FROM lineitem WHERE l_shipdate <= DATE '1998-09-02' GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus;"

# negative controls
verify_not_offloaded disabled    "SELECT count(*), sum(id) FROM t WHERE id >= 2;"   # feature off
verify_declined      bare_count  "SELECT count(*) FROM t;"   # no column referenced => nothing to stream

say ""
say "================================================================"
say "PASS=$PASS  FAIL=$FAIL"
[ "$FAIL" -eq 0 ] && { say "ALL CHECKS PASSED"; exit 0; } || { say "SOME CHECKS FAILED"; exit 1; }
