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

-- Join partner for t: o.t_id references t.id (1..5), plus oid 6 with t_id 6 that
-- has NO match in t (so an INNER join drops it, a LEFT/SEMI join exercises the
-- unmatched-row path). Carries a numeric column to exercise join + numeric agg.
DROP TABLE IF EXISTS o;
CREATE TABLE o (oid bigint NOT NULL, t_id bigint NOT NULL, amt bigint NOT NULL,
                price numeric(15,2) NOT NULL, cat text NOT NULL);
INSERT INTO o VALUES
  (1, 1,  5,  10.50, 'a'),
  (2, 1,  7,  20.25, 'b'),
  (3, 2,  9,   0.00, 'a'),
  (4, 3,  2, 100.00, 'b'),
  (5, 3,  4,   7.75, 'a'),
  (6, 6, 99,  50.00, 'c');

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

-- Large table: far more rows than the SHM ring can hold at once, so it can only
-- offload by streaming through the bounded ring (the old whole-relation
-- pre-buffer would have errored on the size guard). Adopts many blocks.
DROP TABLE IF EXISTS big;
CREATE TABLE big (id bigint NOT NULL, v bigint NOT NULL, p numeric(15,2) NOT NULL);
INSERT INTO big SELECT g, g * 2, (g::numeric / 100) FROM generate_series(1, 1000000) g;

-- A numeric column holding NaN: the offloaded query must fail closed (the worker
-- raises rather than silently corrupting), and leave no leaked SHM object/socket.
DROP TABLE IF EXISTS nantbl;
CREATE TABLE nantbl (id bigint NOT NULL, amt numeric(10,2) NOT NULL);
INSERT INTO nantbl VALUES (1, 5.00), (2, 'NaN'), (3, 7.50);
SQL
[ $? -eq 0 ] || { say "PG fixture setup failed"; exit 1; }

# The planner/executor hooks live in pg_clickhouse's _PG_init, which only runs
# once the library is loaded into the backend. A plain SELECT over a heap table
# does not auto-load the FDW library, so preload it for every session connecting
# to this database (the supported way to enable the hooks).
"${PSQL[@]}" -c "ALTER DATABASE \"$PG_DB\" SET session_preload_libraries = 'pg_clickhouse';" >/dev/null

ROWS_T=5; ROWS_W=5; ROWS_M=5; ROWS_L=8; ROWS_BIG=1000000; ROWS_O=6
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

# verify_big <name> <want_rows> <min_blocks> <sql>: like verify_offload, but also
# asserts the stream adopted MANY blocks (scaling with row count, not 1). The
# relation is far larger than one ring slot, so it can only offload by streaming
# many blocks through the bounded ring rather than being pre-buffered whole.
verify_big() {
    local name="$1" want_rows="$2" min_blocks="$3" sql="$4"
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
    say "[$name] (bounded-ring streaming of a large relation)"
    say "    pg(off): $(echo "$baseline" | tr '\n' '|')   pg(on): $(echo "$result" | tr '\n' '|')"
    say "    CH executed : $chsql"
    say "    CH read_rows=$read_rows  ShmAdoptedBlocks=$shm_blocks (>= $min_blocks expected)"

    [ "$result" = "$baseline" ] && ok "$name: offload result == baseline" || bad "$name: result mismatch"
    { [ -n "${after:-}" ] && [ -n "${before:-}" ] && [ "$after" -gt "$before" ]; } 2>/dev/null \
        && ok "$name: ClickHouse executed a new streamed_table() query" || bad "$name: no new streamed_table query"
    [ "${shm_blocks:-0}" -ge "$min_blocks" ] 2>/dev/null \
        && ok "$name: adopted many SHM blocks (ShmAdoptedBlocks=$shm_blocks >= $min_blocks)" \
        || bad "$name: ShmAdoptedBlocks=$shm_blocks < $min_blocks (not streaming through the ring?)"
    [ "${read_rows:-0}" = "$want_rows" ] 2>/dev/null \
        && ok "$name: CH read_rows == $want_rows (whole relation streamed)" || bad "$name: read_rows=$read_rows != $want_rows"
}

# verify_join <name> <want_rows> <min_blocks> <sql>: like verify_offload, but for a
# JOIN pushed down to ClickHouse as streamed_table() <type> JOIN streamed_table().
# Each base relation is streamed into its own ring, so ClickHouse reads every base
# table fully (read_rows == sum of the joined relations' row counts) and adopts at
# least one block per source (min_blocks >= 2 for a two-table join).
verify_join() {
    local name="$1" want_rows="$2" min_blocks="$3" sql="$4"
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
    say "[$name] (JOIN pushdown: one SHM source per base relation)"
    say "    pg(off): $(echo "$baseline" | tr '\n' '|')   pg(on): $(echo "$result" | tr '\n' '|')"
    say "    CH executed : $chsql"
    say "    CH read_rows=$read_rows  ShmAdoptedBlocks=$shm_blocks (>= $min_blocks expected)"

    [ "$result" = "$baseline" ] && ok "$name: offload result == baseline" || bad "$name: result mismatch"
    { [ -n "${after:-}" ] && [ -n "${before:-}" ] && [ "$after" -gt "$before" ]; } 2>/dev/null \
        && ok "$name: ClickHouse executed a new streamed_table() query" || bad "$name: no new streamed_table query"
    { echo "$chsql" | grep -qi 'join'; } \
        && ok "$name: ClickHouse query is a JOIN over streamed_table() sources" || bad "$name: CH query is not a join"
    [ "${shm_blocks:-0}" -ge "$min_blocks" ] 2>/dev/null \
        && ok "$name: adopted >= $min_blocks SHM blocks (one per source) (ShmAdoptedBlocks=$shm_blocks)" \
        || bad "$name: ShmAdoptedBlocks=$shm_blocks < $min_blocks (not all sources streamed?)"
    [ "${read_rows:-0}" = "$want_rows" ] 2>/dev/null \
        && ok "$name: CH read_rows == $want_rows (every base relation streamed)" || bad "$name: read_rows=$read_rows != $want_rows"
}

# verify_error_closed <name> <sql>: an offloaded query over an out-of-domain value must FAIL
# (the worker raises) rather than silently corrupt, and must leave no leaked SHM object/socket.
verify_error_closed() {
    local name="$1" sql="$2"
    local before_shm after_shm
    before_shm=$(ls /dev/shm/ 2>/dev/null | grep -c '^pgch_' || true)
    if "${PSQL[@]}" -c "$SET_ON $sql" >/dev/null 2>&1; then
        bad "$name: offloaded query unexpectedly succeeded on an out-of-domain value"
    else
        ok "$name: offloaded query failed closed on an out-of-domain value"
    fi
    sleep 1
    after_shm=$(ls /dev/shm/ 2>/dev/null | grep -c '^pgch_' || true)
    [ "${after_shm:-0}" -le "${before_shm:-0}" ] \
        && ok "$name: no leaked /dev/shm object after the failed offload" \
        || bad "$name: leaked /dev/shm object after the failed offload ($before_shm -> $after_shm)"
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

# JOIN pushdown: t INNER/LEFT/SEMI JOIN o, each base relation streamed into its own
# SHM ring; ClickHouse runs a single streamed_table() <type> JOIN streamed_table().
# read_rows == rows(t) + rows(o) == 5 + 6 == 11 (both base relations streamed whole).
JOIN_ROWS=$((ROWS_T + ROWS_O))   # 11
# bare INNER join projecting columns (join CustomScan, no aggregate)
verify_join join_inner_project $JOIN_ROWS 2 \
  "SELECT t.id, t.s, o.amt FROM t INNER JOIN o ON t.id = o.t_id ORDER BY t.id, o.amt;"
# aggregate over an INNER join (non-numeric: full fragment pushed down)
verify_join join_agg $JOIN_ROWS 2 \
  "SELECT count(*), sum(o.amt) FROM t INNER JOIN o ON t.id = o.t_id;"
# GROUP BY over an INNER join (non-numeric agg: full fragment pushed down)
verify_join join_groupby $JOIN_ROWS 2 \
  "SELECT o.cat, count(*), sum(o.amt) FROM t INNER JOIN o ON t.id = o.t_id GROUP BY o.cat ORDER BY o.cat;"
# numeric aggregate over an INNER join: the aggregate stays in PostgreSQL for exact
# decimal results, but the JOIN itself still offloads (CH runs the bare join, PG sums).
verify_join join_numeric_agg $JOIN_ROWS 2 \
  "SELECT sum(o.price) FROM t INNER JOIN o ON t.id = o.t_id;"
# LEFT outer join (o has an unmatched row -> NULL right side in the result)
verify_join join_left $JOIN_ROWS 2 \
  "SELECT count(*) FROM o LEFT JOIN t ON o.t_id = t.id;"
# SEMI join (EXISTS): target references only the outer relation
verify_join join_semi $JOIN_ROWS 2 \
  "SELECT count(*) FROM t WHERE EXISTS (SELECT 1 FROM o WHERE o.t_id = t.id);"

# large-data streaming through a bounded ring (Task 2): the relation is far larger
# than one ring slot, so it can only offload by streaming many blocks.
verify_big big_stream  $ROWS_BIG 10 "SELECT count(*), sum(id), sum(v) FROM big;"
verify_big big_decimal $ROWS_BIG 10 "SELECT sum(p) FROM big;"

# fail-closed: an out-of-domain numeric (NaN) makes the streaming worker raise; the
# offloaded query must error rather than silently corrupt, leaving no leaked SHM object.
verify_error_closed nan_fail_closed "SELECT sum(amt) FROM nantbl;"

# negative controls
verify_not_offloaded disabled    "SELECT count(*), sum(id) FROM t WHERE id >= 2;"   # feature off
verify_declined      bare_count  "SELECT count(*) FROM t;"   # no column referenced => nothing to stream

# After the whole matrix (incl. repeated large-data runs), nothing must leak: no
# /dev/shm pgch object, no control socket, no leftover streaming worker.
say ""
say "[teardown] (no leaked resources after the full matrix)"
chq "SYSTEM FLUSH LOGS" >/dev/null
shm_left=$(ls /dev/shm/ 2>/dev/null | grep -c '^pgch_' || true)
sock_left=$(ls /tmp/clickhouse_shm_pgch_*.sock 2>/dev/null | wc -l)
bgw_left=$("${PSQL[@]}" -c "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'pg_clickhouse shm stream';" 2>/dev/null)
[ "${shm_left:-0}" = 0 ] && ok "teardown: no leaked /dev/shm pgch objects" || bad "teardown: $shm_left leaked /dev/shm objects"
[ "${sock_left:-0}" = 0 ] && ok "teardown: no leaked control sockets" || bad "teardown: $sock_left leaked control sockets"
[ "${bgw_left:-0}" = 0 ] && ok "teardown: no leaked streaming background workers" || bad "teardown: $bgw_left leaked workers"

say ""
say "================================================================"
say "PASS=$PASS  FAIL=$FAIL"
[ "$FAIL" -eq 0 ] && { say "ALL CHECKS PASSED"; exit 0; } || { say "SOME CHECKS FAILED"; exit 1; }
