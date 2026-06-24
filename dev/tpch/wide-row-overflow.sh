#!/usr/bin/env bash
# Regression guard for the wide-row per-slot overflow cliff (Phase 3, scoped:
# byte-sized blocks). Before the fix, a projection whose 65536-row block exceeds
# the 16 MB slot tripped publish_block's `goto overflow` ERROR (producer died ->
# ClickHouse SHM_BLOCK_FRAMING_INVALID). After the fix the columnizer publishes
# short, byte-bounded blocks, so wide rows offload and stay byte-identical to
# native. Run:  RUN_ID=tpchcb ./wide-row-overflow.sh
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
. "$HERE/../bench/bench-common.sh"
PG_DB="${PG_DB:-tpch_sf10}"
PASS=0; FAIL=0
ok()  { PASS=$((PASS+1)); printf '    PASS  %s\n' "$*"; }
bad() { FAIL=$((FAIL+1)); printf '    FAIL  %s\n' "$*"; }

echo "=== build wide tables (65536-row blocks would exceed the 16 MB slot) ==="
sudo -u "$PG_SUPER" psql -d "$PG_DB" -X -q -v ON_ERROR_STOP=1 >/dev/null 2>&1 <<'SQL'
SET search_path=pg;
DROP TABLE IF EXISTS wide_fixed, wide_var2;
-- (a) ~600 B/row text, content varying per row: 600 * 65536 ~= 39 MB/block > 16 MB slot.
CREATE TABLE wide_fixed (k int NOT NULL, w text NOT NULL);
INSERT INTO wide_fixed SELECT g, md5(g::text) || repeat(chr(65 + (g % 26)), 560) FROM generate_series(1, 200000) g;
-- (b) variable 2..3000 B/row text + decimal + bpchar: flushes at varied points, mixes widths.
CREATE TABLE wide_var2 (k int NOT NULL, a numeric(18,4) NOT NULL, w text NOT NULL, b char(20) NOT NULL);
INSERT INTO wide_var2 SELECT g, (g*1.5)::numeric(18,4), repeat(md5(g::text), (g % 90) + 1), 'tag'||(g%100)
  FROM generate_series(1, 150000) g;
VACUUM (FREEZE, ANALYZE) pg.wide_fixed;
VACUUM (FREEZE, ANALYZE) pg.wide_var2;
SQL
[ $? -eq 0 ] || { bad "table build failed"; exit 1; }

# Plain projections decline offload, so the cliff is exercised via OFFLOADED,
# content-dependent aggregates (min/max/count-distinct over the wide string +
# sum over decimals). These stream every wide row through the producer (where the
# overflow used to fire) and would diverge if a byte-sized block boundary corrupted
# any row's content.
cmp_agg() {  # $1 label  $2 query
    local nat off flag
    nat=$(sudo -u "$PG_SUPER" psql -d "$PG_DB" -X -q -tA -F'|' -v ON_ERROR_STOP=1 \
          -c "SET search_path=pg; SET pg_clickhouse.enable_shm_offload=off; $2" 2>&1)
    off=$(sudo -u "$PG_SUPER" psql -d "$PG_DB" -X -q -tA -F'|' 2>&1 <<SQL
SET search_path=pg; LOAD 'pg_clickhouse';
SET pg_clickhouse.local_ch_server='ch_bench'; SET pg_clickhouse.shm_min_rows=0;
SET pg_clickhouse.session_settings='allow_experimental_streamed_table_function 1';
SET pg_clickhouse.enable_shm_offload=on;
$2;
SQL
)
    if printf '%s' "$off" | grep -qiE 'overflow|framing|died|server closed|terminated|error|fatal'; then
        bad "$1: offload FAILED (cliff?): $(printf '%s' "$off" | head -1)"; return; fi
    flag=$(sudo -u "$PG_SUPER" psql -d "$PG_DB" -X -q -tA 2>&1 <<SQL
SET search_path=pg; LOAD 'pg_clickhouse';
SET pg_clickhouse.local_ch_server='ch_bench'; SET pg_clickhouse.shm_min_rows=0;
SET pg_clickhouse.session_settings='allow_experimental_streamed_table_function 1';
SET pg_clickhouse.enable_shm_offload=on;
$2; SHOW pg_clickhouse.last_query_used_clickhouse;
SQL
)
    flag=$(echo "$flag" | tail -1)
    if [ "$flag" != on ] && [ "$flag" != t ]; then bad "$1: NOT offloaded (flag=$flag) -- cliff not exercised"; return; fi
    [ "$nat" = "$off" ] && ok "$1: offloaded, no overflow, shm==native ($off)" \
                        || { bad "$1: offload != native"; echo "      nat=$nat"; echo "      off=$off"; }
}

cmp_agg "wide_fixed (~600 B text x 200k)" "SELECT count(*), count(distinct w), min(w), max(w) FROM wide_fixed"
cmp_agg "wide_var2 (variable-width strings)" "SELECT count(*), count(distinct w), min(w), max(w) FROM wide_var2"
cmp_agg "wide_var2 decimal+bpchar (mixed cols)" "SELECT count(*), sum(a), min(b), max(b) FROM wide_var2"

echo ""
echo "wide-row-overflow: PASS=$PASS FAIL=$FAIL"
[ "$FAIL" -eq 0 ] && echo "WIDE-ROW OVERFLOW OK" || { echo "WIDE-ROW OVERFLOW HAD FAILURES"; exit 1; }
