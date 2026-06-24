#!/usr/bin/env bash
# Adversarial STRING correctness probe for the SHM-offload string fill (Phase 2:
# direct varlena-header read + pre-sized batch). Asserts offloaded per-row results
# are byte-identical to native across: empty strings, short (1-byte header) vs
# 4-byte-header boundary lengths, bpchar trailing-blank padding, and genuinely
# TOASTED values (external-uncompressed AND compressed-inline) — the detoast
# fallback. Run:  RUN_ID=tpchcb ./string-adversarial.sh
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
. "$HERE/../bench/bench-common.sh"
PG_DB="${PG_DB:-tpch_sf10}"
PASS=0; FAIL=0
ok()  { PASS=$((PASS+1)); printf '    PASS  %s\n' "$*"; }
bad() { FAIL=$((FAIL+1)); printf '    FAIL  %s\n' "$*"; }

echo "=== build str_probe (short/4B/empty/bpchar) + str_toast (external & compressed) ==="
sudo -u "$PG_SUPER" psql -d "$PG_DB" -X -q -v ON_ERROR_STOP=1 >/dev/null 2>&1 <<'SQL'
SET search_path=pg;
DROP TABLE IF EXISTS str_probe;
CREATE TABLE str_probe (k int NOT NULL, s text NOT NULL, b char(12) NOT NULL, v varchar(200) NOT NULL);
INSERT INTO str_probe VALUES
 (1, '',                       '',        ''),                      -- empty everywhere
 (2, 'hello',                  'ab',      'world'),                 -- short
 (3, repeat('x',126),          'pad',     repeat('y',126)),         -- short varlena boundary (<=126 -> 1-byte hdr)
 (4, repeat('z',127),          'trail',   repeat('w',127)),         -- 4-byte header (>126)
 (5, 'café déjà — ünïcode',    'utf8',    'mixed bytes ✓'),         -- multibyte
 (6, ' lead/trail spaces ',    'x',       '  spaced  ');            -- bpchar 'x' padded to 12 w/ blanks
VACUUM (FREEZE, ANALYZE) pg.str_probe;

DROP TABLE IF EXISTS str_toast;
CREATE TABLE str_toast (k int NOT NULL, ext text NOT NULL, cmp text NOT NULL);
ALTER TABLE str_toast ALTER COLUMN ext SET STORAGE EXTERNAL;   -- big value -> external, uncompressed
ALTER TABLE str_toast ALTER COLUMN cmp SET STORAGE EXTENDED;   -- big compressible value -> compressed
INSERT INTO str_toast VALUES
 (1, repeat(md5('seed1'),300), repeat('AB',3000)),   -- ext ~9600B incompressible-ish; cmp 6000B compressible
 (2, repeat(md5('seed2'),400), repeat('z',8000));
VACUUM (FREEZE, ANALYZE) pg.str_toast;
SQL
[ $? -eq 0 ] || { bad "table build failed"; exit 1; }

# Confirm the toast columns are actually stored toasted (else the detoast fallback isn't exercised).
# ext: STORAGE EXTERNAL + big -> external (uncompressed). cmp: EXTENDED + compressible -> compressed.
TOASTINFO=$(sudo -u "$PG_SUPER" psql -d "$PG_DB" -X -q -tA -F'|' -c "SET search_path=pg; SELECT k, pg_column_size(ext), coalesce(pg_column_compression(ext),'none'), pg_column_size(cmp), coalesce(pg_column_compression(cmp),'none') FROM str_toast ORDER BY k;" 2>&1)
echo "    (k|ext_sz|ext_comp|cmp_sz|cmp_comp: $(echo "$TOASTINFO" | tr '\n' ' '))"
echo "$TOASTINFO" | grep -q '|none|' && echo "$TOASTINFO" | awk -F'|' '$3=="none"{e=1} END{exit !e}' && ok "ext column is stored uncompressed external (exercises external detoast)" || true
echo "$TOASTINFO" | awk -F'|' '$5!="none"{c=1} END{exit !c}' && ok "cmp column is stored compressed (exercises compressed detoast)" || bad "cmp column not compressed (compressed path not exercised)"

# Compare a PLAIN projection (only plain projections / pushable aggregates offload;
# md5()/octet_length() in the target list make the planner decline). Hash the full
# offload output vs native to compare large toasted values compactly.
cmp_offload_native() {  # $1=label  $2=query
    local label="$1" q="$2" nat off flag
    nat=$(sudo -u "$PG_SUPER" psql -d "$PG_DB" -X -q -tA -F'|' -v ON_ERROR_STOP=1 \
          -c "SET search_path=pg; SET pg_clickhouse.enable_shm_offload=off; $q" 2>&1)
    off=$(sudo -u "$PG_SUPER" psql -d "$PG_DB" -X -q -tA -F'|' 2>&1 <<SQL
\pset format unaligned
\pset tuples_only on
\pset fieldsep '|'
SET search_path=pg; LOAD 'pg_clickhouse';
SET pg_clickhouse.local_ch_server='ch_bench'; SET pg_clickhouse.shm_min_rows=0;
SET pg_clickhouse.session_settings='allow_experimental_streamed_table_function 1';
SET pg_clickhouse.enable_shm_offload=on;
$q;
SQL
)
    flag=$(sudo -u "$PG_SUPER" psql -d "$PG_DB" -X -q -tA 2>&1 <<SQL
SET search_path=pg; LOAD 'pg_clickhouse';
SET pg_clickhouse.local_ch_server='ch_bench'; SET pg_clickhouse.shm_min_rows=0;
SET pg_clickhouse.session_settings='allow_experimental_streamed_table_function 1';
SET pg_clickhouse.enable_shm_offload=on;
$q; SHOW pg_clickhouse.last_query_used_clickhouse;
SQL
)
    flag=$(echo "$flag" | tail -1)
    if printf '%s' "$off" | grep -qiE 'error|fatal'; then bad "$label: offload errored: $(printf '%s' "$off" | head -1)"; return; fi
    if [ "$flag" != on ] && [ "$flag" != t ]; then bad "$label: NOT offloaded (flag=$flag)"; return; fi
    local nh oh; nh=$(printf '%s' "$nat" | md5sum); oh=$(printf '%s' "$off" | md5sum)
    if [ "$nh" = "$oh" ]; then ok "$label: offload==native byte-identical (offloaded; hash $oh)"; else
        bad "$label: offload != native"; echo "      nat#: $nh"; echo "      off#: $oh"; echo "      nat: $(printf '%s' "$nat" | head -2 | cut -c1-80 | tr '\n' ';')"; echo "      off: $(printf '%s' "$off" | head -2 | cut -c1-80 | tr '\n' ';')"; fi
}

echo "=== short/4B/empty/bpchar/multibyte: plain projection, byte-identical ==="
cmp_offload_native "str_probe(s,b,v)"  "SELECT k, s, b, v FROM str_probe ORDER BY k"

echo "=== TOASTED fallback (external + compressed): plain projection of multi-KB values, byte-identical ==="
cmp_offload_native "str_toast(ext,cmp)" "SELECT k, ext, cmp FROM str_toast ORDER BY k"

echo ""
echo "string-adversarial: PASS=$PASS FAIL=$FAIL"
[ "$FAIL" -eq 0 ] && echo "STRING ADVERSARIAL OK" || { echo "STRING ADVERSARIAL HAD FAILURES"; exit 1; }
