#!/usr/bin/env bash
# Visibility-state correctness matrix for the vectorized SHM page reader.
#
# For each representative tuple-visibility state, run the SAME aggregate with
# pg_clickhouse.enable_shm_offload OFF (baseline PostgreSQL) and ON (vectorized
# page reader), and assert byte-identical results. enable_shm_offload toggling
# is the correctness oracle; with the reader ON these exercise the all-visible
# page fast path, the branch-light hint-bit/xmin/xmax classifier, and the
# HeapTupleSatisfiesMVCC fallback for undecided tuples.
#
# Requires the ch_bench server running:
#   RUN_ID=tpchcb dev/bench/ch-bench-server.sh start
#
# Env: PGDB (tpch_sf10), CH_SERVER (ch_bench)
set -uo pipefail

PGDB="${PGDB:-tpch_sf10}"
CH_SERVER="${CH_SERVER:-ch_bench}"
PASS=0
FAIL=0

PSQL=(sudo -u postgres psql -d "$PGDB" -tAqX -v ON_ERROR_STOP=1)

# Offload session settings (vectorized reader ON).
OFFLOAD_ON="LOAD 'pg_clickhouse';
SET pg_clickhouse.local_ch_server = '${CH_SERVER}';
SET pg_clickhouse.shm_min_rows = 0;
SET pg_clickhouse.session_settings = 'allow_experimental_streamed_table_function 1, max_threads 1';
SET pg_clickhouse.shm_vectorized_reader = on;
SET pg_clickhouse.enable_shm_offload = on;"

# The query: a few aggregates + a filter, over the int/date/numeric-free columns
# so the vectorized path is eligible (no Decimal).
Q="SELECT count(*), sum(a), sum(b), min(d), max(d) FROM vis WHERE a >= 0;"

run_off() { "${PSQL[@]}" -c "SET pg_clickhouse.enable_shm_offload = off; $Q"; }
run_on()  { printf '%s\n%s\n' "$OFFLOAD_ON" "$Q" | "${PSQL[@]}"; }

check() {
  # $1 = label
  local label="$1" off on
  off="$(run_off)"
  on="$(run_on)"
  if [ "$off" = "$on" ]; then
    PASS=$((PASS+1)); printf '  PASS  %-28s off==on  [%s]\n' "$label" "$on"
  else
    FAIL=$((FAIL+1)); printf '  FAIL  %-28s off=[%s] on=[%s]\n' "$label" "$off" "$on"
  fi
}

echo "=== vectorized reader visibility matrix (db=${PGDB}) ==="

# Fixtures: a = int4 (sign drives the filter), b = float8, c = text (forces a
# varlena before d), d = date. All NOT NULL so the table is offload-eligible.
"${PSQL[@]}" -c "DROP TABLE IF EXISTS vis;" >/dev/null
"${PSQL[@]}" -c "CREATE TABLE vis (a int NOT NULL, b float8 NOT NULL, c text NOT NULL, d date NOT NULL);" >/dev/null
"${PSQL[@]}" -c "INSERT INTO vis SELECT g, g*1.5, 'row'||g, date '2000-01-01' + g FROM generate_series(1,5000) g;" >/dev/null

# 1) Freshly inserted + committed, hint bits NOT yet set -> classifier UNDECIDED
#    -> HeapTupleSatisfiesMVCC fallback path.
check "fresh_committed_unhinted"

# 2) Hint bits now set (the OFF baseline run above scanned the heap and set
#    HEAP_XMIN_COMMITTED) -> classifier fast VISIBLE lane (not yet all-visible).
check "committed_hinted_not_allvisible"

# 3) VACUUM (FREEZE) -> page PD_ALL_VISIBLE -> all-visible page fast path
#    (zero per-tuple visibility work).
"${PSQL[@]}" -c "VACUUM (FREEZE, ANALYZE) vis;" >/dev/null
check "all_visible_frozen"

# 4) Deleted + committed rows must NOT appear.
"${PSQL[@]}" -c "DELETE FROM vis WHERE a % 7 = 0;" >/dev/null
check "deleted_committed"

# 5) HOT update (in-page new version; old version's xmax = updater) -> exercises
#    the xmax classifier / fallback and HOT-chain handling (we only ever see the
#    live LP_NORMAL version).
"${PSQL[@]}" -c "UPDATE vis SET b = b + 1 WHERE a % 11 = 0;" >/dev/null
check "hot_updated"

# 6) Concurrent UNCOMMITTED insert in another session must NOT be visible to the
#    offload scan (its snapshot predates the other xact). Session B holds an
#    uncommitted INSERT open via pg_sleep while we run OFF then ON here.
"${PSQL[@]}" -c "BEGIN; INSERT INTO vis SELECT 1000000+g, 0, 'u'||g, date '2000-01-01' FROM generate_series(1,3000) g; SELECT pg_sleep(12); ROLLBACK;" >/dev/null 2>&1 &
BG=$!
sleep 3   # let session B's INSERT land (still uncommitted)
check "concurrent_uncommitted_insert"
wait "$BG" 2>/dev/null || true

# 7) Concurrent UNCOMMITTED delete in another session must STILL be visible
#    (the delete is not visible to our snapshot).
"${PSQL[@]}" -c "BEGIN; DELETE FROM vis WHERE a % 13 = 0; SELECT pg_sleep(12); ROLLBACK;" >/dev/null 2>&1 &
BG=$!
sleep 3
check "concurrent_uncommitted_delete"
wait "$BG" 2>/dev/null || true

# 8) Post-vacuum steady state again (mix of all-visible and not).
"${PSQL[@]}" -c "VACUUM (ANALYZE) vis;" >/dev/null
check "post_vacuum_mixed"

"${PSQL[@]}" -c "DROP TABLE IF EXISTS vis;" >/dev/null

echo "================================================================"
echo "PASS=${PASS}  FAIL=${FAIL}"
[ "$FAIL" -eq 0 ] && echo "ALL VISIBILITY CHECKS PASSED" || { echo "VISIBILITY CHECKS FAILED"; exit 1; }
