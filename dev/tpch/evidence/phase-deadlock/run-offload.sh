#!/usr/bin/env bash
# Run a TPC-H query (or arbitrary SQL file) with SHM offload ON, capturing output.
# Usage: run-offload.sh <sql_file> <tag> <stmt_timeout_s> [extra_session_settings]
# Writes <tag>.out / <tag>.err in this dir. Strips a leading EXPLAIN line.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
. "$HERE/../../../bench/bench-common.sh"

SQLFILE="$1"; TAG="$2"; STMT_TO="${3:-120}"; EXTRA="${4:-}"
OUT="$HERE/$TAG.out"; ERR="$HERE/$TAG.err"

SESS="join_use_nulls 1, group_by_use_nulls 1, final 1, allow_experimental_streamed_table_function 1, log_comment $TAG"
[ -n "$EXTRA" ] && SESS="$SESS, $EXTRA"

RSQL="$(sed '/^EXPLAIN/d' "$SQLFILE")"

chq "SYSTEM FLUSH LOGS" >/dev/null 2>&1
printf "LOAD 'pg_clickhouse';
SET pg_clickhouse.local_ch_server='ch_bench';
SET pg_clickhouse.shm_min_rows=0;
SET pg_clickhouse.session_settings='%s';
SET pg_clickhouse.enable_shm_offload=on;
SET search_path=pg;
SET statement_timeout='%ss';
%s
" "$SESS" "$STMT_TO" "$RSQL" \
  | sudo -u postgres psql -d tpch_sf10 -tAqX -F'|' -v ON_ERROR_STOP=0 -P null=NULL \
  > "$OUT" 2> "$ERR"
echo "EXIT=$? tag=$TAG out=$OUT err=$ERR"
chq "SYSTEM FLUSH LOGS" >/dev/null 2>&1
