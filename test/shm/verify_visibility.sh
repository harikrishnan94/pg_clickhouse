#!/usr/bin/env bash
# Visibility-state correctness AND path-coverage matrix for the vectorized SHM
# page reader (the branch-free SoA classify kernel + the MVCC slow path).
#
# The trap this suite is built to avoid: result-equivalence on all-visible data
# passes WITHOUT ever running the classifier or the slow path. A freshly
# VACUUM/FREEZE'd relation takes the all-visible page fast path and emits every
# tuple, so the not-all-visible classifier and the MVCC oracle are never
# entered, yet every result assertion goes green. So every case here PROVES the
# intended path was taken, via two independent instruments:
#
#   1. pg_visibility -- proves the PAGE-STATE precondition (e.g. that the table
#      is genuinely NOT all-visible, so the classify path is the only one that
#      can run). Independent of our own code.
#   2. The pg_clickhouse.shm_log_stream_stats "shm visibility:" LOG line -- the
#      per-scan path/verdict counters (pages classified, per-verdict tuple
#      tallies, slow-path resolutions). Proves which classifier lane and the
#      MVCC oracle actually executed.
#
# Correctness oracle: every case asserts the offloaded result is byte-identical
# with the blessed scalar scan (enable_shm_offload off) -- so the SoA classify
# kernel and PostgreSQL agree on the visible set. (The scalar reference
# classifier is retained as an assertion-build cross-check of the kernel, see
# shm_page_reader.c; an assert-enabled build exercises it on every tuple here.)
#
# autovacuum is DISABLED on the fixtures: an insert-triggered autovacuum will
# otherwise asynchronously mark a table all-visible and silently defeat the
# not-all-visible cases.
#
# Requires the ch_bench server running:
#   RUN_ID=tpchcb dev/bench/ch-bench-server.sh start
#
# Env: PGDB (tpch_sf10), CH_SERVER (ch_bench), PGLOG (postgres server log).
set -uo pipefail

PGDB="${PGDB:-tpch_sf10}"
CH_SERVER="${CH_SERVER:-ch_bench}"
PGLOG="${PGLOG:-/var/log/postgresql/postgresql-18-main.log}"
PASS=0
FAIL=0

PSQL=(sudo -u postgres psql -d "$PGDB" -tAqX -v ON_ERROR_STOP=1)

Q="SELECT count(*), sum(a), sum(b), min(d), max(d) FROM vis WHERE a >= 0;"
RESULT=""          # query result of the most recent run_on
VIS_LINE=""        # the "shm visibility:" LOG line from the most recent run_on

# ---- session settings for an offload run.
on_prelude() {
  printf '%s\n' \
    "LOAD 'pg_clickhouse';" \
    "SET pg_clickhouse.local_ch_server = '${CH_SERVER}';" \
    "SET pg_clickhouse.shm_min_rows = 0;" \
    "SET pg_clickhouse.session_settings = 'allow_experimental_streamed_table_function 1, max_threads 1';" \
    "SET pg_clickhouse.shm_log_stream_stats = on;" \
    "SET pg_clickhouse.shm_transport_mode = '${TRANSPORT:-adopt}';" \
    "SET pg_clickhouse.enable_shm_offload = on;"
}

qx() { "${PSQL[@]}" -c "$1" >/dev/null 2>&1; }

# Run query $1 with offload OFF (the blessed scalar table scan). Echoes result.
run_off() { "${PSQL[@]}" -c "SET pg_clickhouse.enable_shm_offload = off; $1"; }

# Run query $1 offloaded (vectorized page reader). Sets globals RESULT (query
# output) and VIS_LINE (the "shm visibility:" LOG line emitted by the background
# streaming worker; that line can land in the server log slightly after the
# client gets its result, so poll). NB: call as a statement, never as
# $(run_on ...) -- a command-substitution subshell would discard the globals.
run_on() {
  local mark i line
  mark=$(sudo wc -l < "$PGLOG")
  RESULT=$(printf '%s\n%s\n' "$(on_prelude)" "$1" | "${PSQL[@]}")
  VIS_LINE=""
  for i in $(seq 1 25); do
    line=$(sudo tail -n +"$((mark+1))" "$PGLOG" | grep -E "pg_clickhouse shm visibility:" | tail -1)
    [ -n "$line" ] && { VIS_LINE="$line"; break; }
    sleep 0.2
  done
}

# A counter field from VIS_LINE (e.g. vf undecided).
vf() { grep -oE "$1=[0-9]+" <<<"$VIS_LINE" | head -1 | cut -d= -f2; }

allvis_pages() { "${PSQL[@]}" -c "SELECT count(*) FILTER (WHERE all_visible) FROM pg_visibility('$1'::regclass);"; }
total_pages()  { "${PSQL[@]}" -c "SELECT count(*) FROM pg_visibility('$1'::regclass);"; }

ok()  { PASS=$((PASS+1)); printf '  PASS  %-34s %s\n' "$1" "$2"; }
bad() { FAIL=$((FAIL+1)); printf '  FAIL  %-34s %s\n' "$1" "$2"; }

# $1 label, $2 actual, $3 expected
expect_eq() { [ "$2" = "$3" ] && ok "$1" "[$2]" || bad "$1" "got [$2] want [$3]"; }
# $1 label, $2 actual, $3 threshold, $4 note
expect_gt() { { [ -n "$2" ] && [ "$2" -gt "$3" ] 2>/dev/null; } && ok "$1" "$4 ($2 > $3)" || bad "$1" "$4: got [$2] want > $3"; }
# $1 label, offload-off result, offload-on result
expect_equiv() { [ "$2" = "$3" ] && ok "$1" "off==vec [$2]" || bad "$1" "off=[$2] vec=[$3]"; }

recreate() {  # fresh, autovacuum-disabled fixture (a int, b float8, c text forces a varlena, d date)
  qx "DROP TABLE IF EXISTS vis;"
  qx "CREATE TABLE vis (a int NOT NULL, b float8 NOT NULL, c text NOT NULL, d date NOT NULL)
        WITH (autovacuum_enabled = false, toast.autovacuum_enabled = false);"
  # date kept within ClickHouse Date range (1970..2149) via % 3000 regardless of N
  qx "INSERT INTO vis SELECT g, g*1.5, 'row'||g, date '2000-01-01' + (g % 3000) FROM generate_series(1,$1) g;"
}

echo "=== vectorized reader visibility path+result matrix (db=${PGDB}) ==="
qx "CREATE EXTENSION IF NOT EXISTS pg_visibility;"

# ---------------------------------------------------------------------------
# 1) fresh_committed_unhinted -> UNDECIDED -> MVCC slow path.
#    Freshly inserted+committed, no hint bits, not vacuumed. The kernel cannot
#    prove visibility from hints, so every tuple is UNDECIDED and resolved by the
#    oracle. Run the kernel FIRST (before any scan sets hint bits).
# ---------------------------------------------------------------------------
recreate 5000
expect_eq "1.precondition_not_all_visible" "$(allvis_pages vis)" "0"
run_on "$Q"; on_vec=$RESULT              # kernel run while still unhinted
expect_gt "1.classified_pages"        "$(vf classified)"   "0" "classify path ran"
expect_gt "1.undecided_to_oracle"     "$(vf undecided)"    "0" "unhinted -> UNDECIDED"
expect_gt "1.slow_path_visible"       "$(vf slow_visible)" "0" "oracle resolved visible"
off=$(run_off "$Q")
expect_equiv "1.result_equiv" "$off" "$on_vec"

# ---------------------------------------------------------------------------
# 2) committed_hinted_not_all_visible -> definite VISIBLE fast lane.
#    The oracle above set HEAP_XMIN_COMMITTED; autovacuum is off so PD_ALL_VISIBLE
#    is still unset. The kernel now proves VISIBLE from hints alone, no oracle.
# ---------------------------------------------------------------------------
expect_eq "2.precondition_not_all_visible" "$(allvis_pages vis)" "0"
run_on "$Q"; on_vec=$RESULT
expect_gt "2.classified_pages"   "$(vf classified)"    "0" "classify path ran"
expect_gt "2.visible_fast"       "$(vf visible_fast)"  "0" "hinted -> fast VISIBLE"
expect_eq "2.no_undecided"       "$(vf undecided)"     "0"
off=$(run_off "$Q")
expect_equiv "2.result_equiv" "$off" "$on_vec"

# ---------------------------------------------------------------------------
# 3) all_visible_frozen -> all-visible page fast path (zero per-tuple work).
# ---------------------------------------------------------------------------
qx "VACUUM (FREEZE, ANALYZE) vis;"
np=$(total_pages vis)
expect_eq "3.precondition_all_visible" "$(allvis_pages vis)" "$np"
run_on "$Q"; on_vec=$RESULT
expect_eq "3.all_visible_pages"  "$(vf all_visible)" "$np"
expect_eq "3.no_classify"        "$(vf classified)" "0"
expect_eq "3.no_gather"          "$(vf gathered)"   "0"
off=$(run_off "$Q")
expect_equiv "3.result_equiv" "$off" "$on_vec"

# ---------------------------------------------------------------------------
# 4) deleted_committed -> definite INVISIBLE fast lane (committed visible deleter).
#    DELETE+commit on the frozen table clears PD_ALL_VISIBLE on touched pages.
#    First kernel run finds the dead tuples' xmax unhinted (UNDECIDED -> oracle,
#    which sets HEAP_XMAX_COMMITTED); the second run proves the INVISIBLE fast
#    lane. Only offload scans touch the table in between (they never prune), so
#    the dead tuples persist long enough to be classified.
# ---------------------------------------------------------------------------
qx "DELETE FROM vis WHERE a % 7 = 0;"
expect_eq "4.precondition_not_all_visible" "$(allvis_pages vis)" "0"
run_on "$Q"                               # run #1: oracle hints xmax committed
run_on "$Q"; on_vec=$RESULT               # run #2: kernel proves INVISIBLE fast
expect_gt "4.classified_pages"  "$(vf classified)"     "0" "classify path ran"
expect_gt "4.invisible_fast"    "$(vf invisible_fast)" "0" "committed deleter -> fast INVISIBLE"
off=$(run_off "$Q")
expect_equiv "4.result_equiv" "$off" "$on_vec"

# ---------------------------------------------------------------------------
# 5) hot_updated -> old version xmax=committed updater, new version xmin=committed.
#    Exercises both classifier sides on a not-all-visible page; the visible set
#    is only the live versions.
# ---------------------------------------------------------------------------
qx "VACUUM (FREEZE, ANALYZE) vis;"          # reset to all-visible, then dirty it
qx "UPDATE vis SET b = b + 1 WHERE a % 11 = 0;"
expect_eq "5.precondition_not_all_visible" "$(allvis_pages vis)" "0"
run_on "$Q"                                  # hint the updater xmax / new xmin
run_on "$Q"; on_vec=$RESULT
expect_gt "5.classified_pages"  "$(vf classified)"   "0" "classify path ran"
expect_gt "5.visible_fast"      "$(vf visible_fast)" "0" "live versions -> fast VISIBLE"
off=$(run_off "$Q")
expect_equiv "5.result_equiv" "$off" "$on_vec"

# ---------------------------------------------------------------------------
# Concurrency: a second session holds an xact open (pg_sleep) so our snapshot
# cannot resolve its xid from hints -> the affected tuples are UNDECIDED and go
# to the oracle. We assert undecided>0 (path proof) AND off==vec for the SAME
# concurrent state. Fixtures are FREEZE'd first so only the concurrently touched
# tuples are on not-all-visible pages.
# ---------------------------------------------------------------------------
run_under_concurrency() {  # $1 label, $2 bg-sql, $3 expect-undecided-note
  qx "VACUUM (FREEZE, ANALYZE) vis;"
  "${PSQL[@]}" -c "BEGIN; $2 SELECT pg_sleep(14); ROLLBACK;" >/dev/null 2>&1 &
  local bg=$!
  sleep 3                                   # let the bg statement land (uncommitted)
  local on_vec off
  run_on "$Q"; on_vec=$RESULT
  expect_gt "${1}.undecided_to_oracle" "$(vf undecided)" "0" "$3"
  off=$(run_off "$Q")
  expect_equiv "${1}.result_equiv" "$off" "$on_vec"
  wait "$bg" 2>/dev/null || true
}

# 6) concurrent UNCOMMITTED insert: in-flight rows must NOT be visible to us.
recreate 5000
run_under_concurrency "6.concurrent_insert" \
  "INSERT INTO vis SELECT 1000000+g, 0, 'u'||g, date '2000-01-01' FROM generate_series(1,3000) g;" \
  "in-progress inserter -> UNDECIDED"

# 7) concurrent UNCOMMITTED delete (rolled back): rows must STILL be visible.
recreate 5000
run_under_concurrency "7.concurrent_delete" \
  "DELETE FROM vis WHERE a % 13 = 0;" \
  "in-progress deleter -> UNDECIDED"

# 8) concurrent in-flight UPDATE: old version's xmax is an in-progress updater.
recreate 5000
run_under_concurrency "8.concurrent_update" \
  "UPDATE vis SET b = b + 1 WHERE a % 9 = 0;" \
  "in-progress updater -> UNDECIDED"

# 9) multixact (non-lock-only): a held FOR KEY SHARE locker + a concurrent UPDATE
#    makes the tuple's xmax a MultiXactId that is NOT lock-only -> HEAP_XMAX_IS_MULTI
#    -> UNDECIDED -> oracle resolves the update xid. Session B holds the key-share
#    lock open; session C performs the update inside that window.
recreate 5000
qx "VACUUM (FREEZE, ANALYZE) vis;"
"${PSQL[@]}" -c "BEGIN; SELECT * FROM vis WHERE a % 17 = 0 FOR KEY SHARE; SELECT pg_sleep(14); ROLLBACK;" >/dev/null 2>&1 &
BG=$!
sleep 3
"${PSQL[@]}" -c "UPDATE vis SET b = b + 1 WHERE a % 17 = 0;" >/dev/null 2>&1   # creates the multixact
run_on "$Q"; on_vec=$RESULT
expect_gt "9.multixact_undecided" "$(vf undecided)" "0" "HEAP_XMAX_IS_MULTI -> UNDECIDED"
off=$(run_off "$Q")
expect_equiv "9.result_equiv" "$off" "$on_vec"
wait "$BG" 2>/dev/null || true

# ---------------------------------------------------------------------------
# 10) Mixed page: visible + invisible + undecided tuples on the same not-all-
#     visible pages. Committed-hinted survivors (VISIBLE fast) + committed-deleted
#     (INVISIBLE fast) + a concurrent in-flight delete (UNDECIDED) all at once.
# ---------------------------------------------------------------------------
recreate 5000
qx "DELETE FROM vis WHERE a % 7 = 0;"           # committed deletes
run_on "$Q"                                      # hint xmin (survivors) + xmax (deleted)
run_on "$Q"                                      # ensure committed-deleted now hinted INVISIBLE-fast
"${PSQL[@]}" -c "BEGIN; DELETE FROM vis WHERE a % 5 = 0; SELECT pg_sleep(14); ROLLBACK;" >/dev/null 2>&1 &
BG=$!
sleep 3
run_on "$Q"; on_vec=$RESULT
expect_gt "10.mixed_visible_fast"   "$(vf visible_fast)"   "0" "survivors VISIBLE"
expect_gt "10.mixed_invisible_fast" "$(vf invisible_fast)" "0" "committed-deleted INVISIBLE"
expect_gt "10.mixed_undecided"      "$(vf undecided)"      "0" "in-flight delete UNDECIDED"
off=$(run_off "$Q")
expect_equiv "10.mixed_result_equiv" "$off" "$on_vec"
wait "$BG" 2>/dev/null || true

# ---------------------------------------------------------------------------
# 11) Block-flush straddle on a not-all-visible relation: > rows_per_block (65536)
#     visible rows, freshly inserted+unhinted, so the classify path (not the
#     all-visible page path) feeds tuples across the 65536-row publish boundary.
# ---------------------------------------------------------------------------
recreate 200000
expect_eq "11.precondition_not_all_visible" "$(allvis_pages vis)" "0"
run_on "$Q"; on_vec=$RESULT
expect_gt "11.classified_pages" "$(vf classified)" "0" "classify path ran across blocks"
expect_eq "11.gathered_all"     "$(vf gathered)"   "200000"
off=$(run_off "$Q")
expect_equiv "11.straddle_result_equiv" "$off" "$on_vec"

qx "DROP TABLE IF EXISTS vis;"

echo "================================================================"
echo "PASS=${PASS}  FAIL=${FAIL}"
[ "$FAIL" -eq 0 ] && echo "ALL VISIBILITY CHECKS PASSED" || { echo "VISIBILITY CHECKS FAILED"; exit 1; }
