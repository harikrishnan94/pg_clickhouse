#!/usr/bin/env bash
# Adversarial DECIMAL correctness probe for the SHM-offload numeric->Decimal
# converter. Builds a small table with hard-case numeric values across all three
# wire widths (Decimal32/64/128) and asserts the offloaded per-row result is
# byte-identical to native PostgreSQL, then asserts a stored NaN/Inf fails closed.
#
# Run after the ch_bench server is up:  RUN_ID=tpchcb ./decimal-adversarial.sh
# Use an assertion-enabled build (COPT=-DUSE_ASSERT_CHECKING) to also fire the
# in-converter text-oracle cross-check on every value the C path sees.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
. "$HERE/../bench/bench-common.sh"
PG_DB="${PG_DB:-tpch_sf10}"
PASS=0; FAIL=0
ok()  { PASS=$((PASS+1)); printf '    PASS  %s\n' "$*"; }
bad() { FAIL=$((FAIL+1)); printf '    FAIL  %s\n' "$*"; }

echo "=== building dec_probe (adversarial numeric values) ==="
sudo -u "$PG_SUPER" psql -d "$PG_DB" -X -q -v ON_ERROR_STOP=1 >/dev/null 2>&1 <<'SQL'
SET search_path=pg;
DROP TABLE IF EXISTS dec_probe;
CREATE TABLE dec_probe (
    k    int           NOT NULL,
    d32  numeric(9,2)  NOT NULL,   -- Decimal32
    d64  numeric(18,4) NOT NULL,   -- Decimal64
    d128 numeric(38,6) NOT NULL    -- Decimal128
);
INSERT INTO dec_probe(k,d32,d64,d128) VALUES
 (1,  0,                0,                    0),
 (2,  1,                1,                    1),
 (3, -1,               -1,                   -1),
 (4,  123.45,           12345.6789,           12345678.123456),
 (5, -123.45,          -12345.6789,          -12345678.123456),
 (6,  1.50,             1.5000,               1.500000),                       -- trailing zeros below dscale
 (7,  0.10,             0.0001,               0.000001),                       -- min fractional digit
 (8,  100.00,           100.0000,             100.000000),                     -- integer padded
 (9,  9999999.99,       99999999999999.9999,  99999999999999999999999999999999.999999),  -- max precision
 (10,-9999999.99,      -99999999999999.9999, -99999999999999999999999999999999.999999),  -- min (most negative)
 (11, 0.01,             0.0001,               0.000001),
 (12, 9999.99,          99999999.9999,        99999999999999.999999),
 (13, 0.99,             0.9999,               0.999999),
 (14, 10000.00,         10000.0000,           10000.000000),                   -- limb boundary
 (15,-0.01,            -0.0001,              -0.000001);
SQL
[ $? -eq 0 ] || { bad "table build failed"; exit 1; }
sudo -u "$PG_SUPER" psql -d "$PG_DB" -X -q -c "VACUUM (FREEZE, ANALYZE) pg.dec_probe;" >/dev/null 2>&1

run_native() {  # $1 = query
    sudo -u "$PG_SUPER" psql -d "$PG_DB" -X -q -tA -F'|' -v ON_ERROR_STOP=1 \
        -c "SET search_path=pg; SET pg_clickhouse.enable_shm_offload=off; $1" 2>&1
}
run_offload() { # $1 = query  -> prints rows; sets global OFFFLAG
    local out
    out=$(sudo -u "$PG_SUPER" psql -d "$PG_DB" -X -q -tA -F'|' -v ON_ERROR_STOP=0 2>&1 <<SQL
\pset format unaligned
\pset tuples_only on
\pset fieldsep '|'
SET search_path=pg;
LOAD 'pg_clickhouse';
SET pg_clickhouse.local_ch_server='ch_bench';
SET pg_clickhouse.shm_min_rows=0;
SET pg_clickhouse.session_settings='allow_experimental_streamed_table_function 1';
SET pg_clickhouse.enable_shm_offload=on;
$1;
SQL
)
    printf '%s\n' "$out"
}

echo "=== per-value byte-identical: SELECT each decimal column, all 3 widths ==="
Q="SELECT k, d32, d64, d128 FROM dec_probe ORDER BY k"
NAT=$(run_native "$Q")
OFF=$(run_offload "$Q")
# Confirm it actually offloaded (look for a clean numeric result, not an error).
if printf '%s' "$OFF" | grep -qiE 'error|fatal|does not exist'; then
    bad "offload query errored: $(printf '%s' "$OFF" | head -1)"
else
    if [ "$NAT" = "$OFF" ]; then ok "offload == native byte-identical for all adversarial values"
    else
        bad "offload != native"
        echo "    --- native ---"; printf '%s\n' "$NAT" | sed 's/^/      /'
        echo "    --- offload ---"; printf '%s\n' "$OFF" | sed 's/^/      /'
    fi
fi

echo "=== confirm offload engaged (last_query_used_clickhouse on the per-value query) ==="
FLAG=$(sudo -u "$PG_SUPER" psql -d "$PG_DB" -X -q -tA 2>&1 <<SQL
SET search_path=pg; LOAD 'pg_clickhouse';
SET pg_clickhouse.local_ch_server='ch_bench'; SET pg_clickhouse.shm_min_rows=0;
SET pg_clickhouse.session_settings='allow_experimental_streamed_table_function 1';
SET pg_clickhouse.enable_shm_offload=on;
$Q;
SHOW pg_clickhouse.last_query_used_clickhouse;
SQL
)
echo "$FLAG" | tail -1 | grep -qiE 'on|t|true' && ok "offload engaged for dec_probe (so the byte-identical check above is offload-vs-native)" || bad "dec_probe NOT offloaded: $(echo "$FLAG" | tail -1)"

echo "=== NaN must fail closed (clean producer diagnostic, no crash, no wrong result) ==="
sudo -u "$PG_SUPER" psql -d "$PG_DB" -X -q -v ON_ERROR_STOP=1 >/dev/null 2>&1 <<'SQL'
SET search_path=pg;
DROP TABLE IF EXISTS dec_nan;
CREATE TABLE dec_nan (k int NOT NULL, v numeric(18,4) NOT NULL);
INSERT INTO dec_nan VALUES (1, 1.0), (2, 'NaN'), (3, 2.0);
VACUUM (FREEZE, ANALYZE) pg.dec_nan;
SQL
PGLOG="${PGLOG:-/var/log/postgresql/postgresql-18-main.log}"
LOGSTART=$(sudo wc -l < "$PGLOG" 2>/dev/null || echo 0)
NANOUT=$(run_offload "SELECT v FROM dec_nan ORDER BY k")
# The producer (a bgworker) raises the clean diagnostic into the server log; the
# client sees the downstream ClickHouse attach failure once the producer aborts.
# "Failed closed" = the query ERRORED (no numeric result) AND the producer logged
# the NaN diagnostic AND PG survived. A SIGSEGV / crash-recovery is a FAIL.
PRODLOG=$(sudo tail -n +"$((LOGSTART+1))" "$PGLOG" 2>/dev/null | grep -iE 'holds NaN/Infinity|not representable' | head -1 | sed 's/^.*ERROR:[[:space:]]*//')
if printf '%s' "$NANOUT" | grep -qiE 'server closed|terminated by signal|recovery mode|connection to server'; then
    bad "NaN offload CRASHED the producer (should fail closed): $(printf '%s' "$NANOUT" | head -1)"
elif printf '%s' "$NANOUT" | grep -qiE 'error' && [ -n "$PRODLOG" ]; then
    ok "NaN failed closed: producer raised \"$PRODLOG\" (client saw downstream attach error); no wrong result"
elif printf '%s' "$NANOUT" | grep -qiE 'NaN|not representable'; then
    ok "NaN failed closed with a clean client-visible error"
else
    bad "NaN offload did not fail closed cleanly (no producer diagnostic in log): out=$(printf '%s' "$NANOUT" | tr '\n' ';' | head -c 200)"
fi
# Confirm PG is still alive after the NaN test.
sudo -u "$PG_SUPER" psql -d "$PG_DB" -X -q -tAc "SET search_path=pg; SELECT count(*) FROM dec_nan" >/dev/null 2>&1 \
  && ok "PG alive after NaN test" || bad "PG not alive after NaN test"

echo ""
echo "decimal-adversarial: PASS=$PASS FAIL=$FAIL"
[ "$FAIL" -eq 0 ] && echo "DECIMAL ADVERSARIAL OK" || { echo "DECIMAL ADVERSARIAL HAD FAILURES"; exit 1; }
