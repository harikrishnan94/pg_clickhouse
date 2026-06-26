#!/usr/bin/env bash
# Hot-Cold Phase 0 — focused copy-cost capture (direct instrument for convergence gates C2/C3).
#
# For each query, runs adopt then copy (warm) and reads the per-block copy ProfileEvents straight
# from system.query_log:
#   - ShmCopiedBytesLogical / ShmCopyTimeMicroseconds  -> in-query copy rate (ns/byte, GB/s)  [C2]
#   - (UserTime+SystemTime) copy vs adopt              -> consumer-CPU copy overhead           [C3]
#   - ShmCopyTimeMicroseconds / consumer CPU           -> copy cost as a fraction of consumer CPU
# This is NOT the headline timing (that is the W=8 wsweep). It isolates the copy cost with the same
# real CH consumer, so the microbench ns/byte and the in-query ns/byte can be compared (C2).
#
# Env: BENCH=tpch|clickbench  QUERIES="..."  (defaults: a high-byte representative set)
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
BENCH="${BENCH:-tpch}"
case "$BENCH" in
  tpch)       PGDB=tpch_sf10; QDIR="$HERE/../../tpch/queries"; MT="" ;;
  clickbench) PGDB=clickbench; QDIR="$HERE/../../clickbench/queries"; MT=16 ;;
  *) echo "bad BENCH=$BENCH" >&2; exit 1 ;;
esac
QUERIES="${QUERIES:-1 9 14 19}"
# shellcheck disable=SC1091
. /home/ubuntu/ch-bench/tpchcb/manifest.env
CH="http://$CH_HOST:$CH_HTTP_PORT/"
chq(){ curl -s --max-time 120 "$CH" --data-binary "$1"; }
PSQL=(sudo -u postgres psql -d "$PGDB" -tAqX -F'|' -v ON_ERROR_STOP=0)

offsql(){ # $1=tag $2=transport
  local ss="join_use_nulls 1, group_by_use_nulls 1, final 1, allow_experimental_streamed_table_function 1"
  [ -n "$MT" ] && ss="$ss, max_threads $MT"
  ss="$ss, log_comment $1"
  printf "LOAD 'pg_clickhouse'; SET search_path=pg; SET pg_clickhouse.local_ch_server='ch_bench'; SET pg_clickhouse.shm_min_rows=0; SET pg_clickhouse.session_settings='%s'; SET pg_clickhouse.enable_shm_offload=on; SET pg_clickhouse.shm_transport_mode='%s'; SET max_parallel_workers_per_gather=16; SET statement_timeout='300s';" "$ss" "$2"
}

printf "bench\tq\tmode\tblocks\tcopied_bytes\tcopy_us\tuser_us\tsys_us\tdur_ms\tread_rows\n"
for q in $QUERIES; do
  qf="$QDIR/$q.sql"; [ -f "$qf" ] || { echo "Q$q: no file" >&2; continue; }
  sql="$(sed -e '/^EXPLAIN/d' -e '/^[[:space:]]*--/d' "$qf" | sed 's/;[[:space:]]*$//')"
  for mode in adopt copy; do
    { offsql "warm_${BENCH}_${q}_${mode}_$$" "$mode"; echo "$sql;"; } | "${PSQL[@]}" >/dev/null 2>&1   # warmup
    tag="hccap_${BENCH}_${q}_${mode}_$$"
    { offsql "$tag" "$mode"; echo "$sql;"; } | "${PSQL[@]}" >/dev/null 2>&1                            # measured
    chq "SYSTEM FLUSH LOGS" >/dev/null
    r=$(chq "SELECT sum(ProfileEvents['ShmAdoptedBlocks']+ProfileEvents['ShmCopiedBlocks']), sum(ProfileEvents['ShmCopiedBytesLogical']), sum(ProfileEvents['ShmCopyTimeMicroseconds']), sum(ProfileEvents['UserTimeMicroseconds']), sum(ProfileEvents['SystemTimeMicroseconds']), max(query_duration_ms), sum(read_rows) FROM system.query_log WHERE log_comment='$tag' AND type='QueryFinish' AND positionCaseInsensitive(query,'streamed_table')>0 AND positionCaseInsensitive(query,'query_log')=0")
    printf "%s\t%s\t%s\t%s\n" "$BENCH" "$q" "$mode" "$r"
  done
done
