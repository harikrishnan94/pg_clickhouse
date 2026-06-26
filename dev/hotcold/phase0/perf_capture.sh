#!/usr/bin/env bash
# Hot-Cold Phase 0 — PMU + profile MECHANISM capture (adopt vs copy).
#
# Drives a loop of one heavy offload query in each transport mode against the live CH server
# while perf-stat'ing (hardware PMU) and perf-record'ing (sampling profile) the CH consumer
# process. Proves the mechanism: copy adds bandwidth-bound work (instructions + cache traffic)
# with ~flat branch-misses, and a cloneResized/memcpy hotspot appears in the COPY profile and is
# ABSENT in the adopt profile (where the zero-copy adoption path takes its place).
#
# Uncapped on purpose: per-query PMU counts (instructions, cache-misses) are absolute and
# comparable between modes regardless of the cgroup cap; the cap only changes wall time.
# Run on an idle host (no other CPU load), AFTER any build has finished.
#
# Env: BENCH (default clickbench), Q (default 24 — SELECT * 105 cols, the largest copy), N (iters).
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
BENCH="${BENCH:-clickbench}"; Q="${Q:-24}"; N="${N:-20}"
EV="${EV:-cycles,instructions,cache-references,cache-misses,branch-instructions,branch-misses}"
case "$BENCH" in
  tpch)       PGDB=tpch_sf10;  QDIR="$HERE/../../tpch/queries";       MT="" ;;
  clickbench) PGDB=clickbench; QDIR="$HERE/../../clickbench/queries"; MT=16 ;;
esac
# shellcheck disable=SC1091
. /home/ubuntu/ch-bench/tpchcb/manifest.env
CHPID=$(sudo ss -ltnp 2>/dev/null | grep ":$CH_HTTP_PORT " | grep -oP 'pid=\K[0-9]+' | head -1)
[ -n "$CHPID" ] || { echo "no CH pid" >&2; exit 1; }
OUT="$HERE/evidence"; mkdir -p "$OUT"
PSQL=(sudo -u postgres psql -d "$PGDB" -tAqX -v ON_ERROR_STOP=0)
sql="$(sed -e '/^EXPLAIN/d' -e '/^[[:space:]]*--/d' "$QDIR/$Q.sql" | sed 's/;[[:space:]]*$//')"

build_loopfile(){ # $1=mode $2=n -> path
  local mode="$1" n="$2" lf; lf="$(mktemp)"
  local ss="join_use_nulls 1, group_by_use_nulls 1, final 1, allow_experimental_streamed_table_function 1"
  [ -n "$MT" ] && ss="$ss, max_threads $MT"
  { echo "LOAD 'pg_clickhouse'; SET search_path=pg; SET pg_clickhouse.local_ch_server='ch_bench'; SET pg_clickhouse.shm_min_rows=0; SET pg_clickhouse.session_settings='$ss'; SET pg_clickhouse.enable_shm_offload=on; SET pg_clickhouse.shm_transport_mode='$mode'; SET max_parallel_workers_per_gather=16; SET statement_timeout='120s';"
    for _ in $(seq 1 "$n"); do echo "$sql;"; done; } > "$lf"
  chmod 644 "$lf"   # psql runs as the postgres user; the mktemp file is 600/owned-by-ubuntu
  echo "$lf"
}

echo "perf mechanism capture: BENCH=$BENCH Q=$Q N=$N CHPID=$CHPID events=$EV"
for mode in adopt copy; do
  lf="$(build_loopfile "$mode" "$N")"
  warm="$(build_loopfile "$mode" 2)"
  "${PSQL[@]}" -f "$warm" >/dev/null 2>&1   # warmup (page cache, CH marks)
  echo "===== perf stat ($mode) =====" | tee "$OUT/perf-stat-$mode.txt"
  sudo perf stat -p "$CHPID" -e "$EV" -- "${PSQL[@]}" -f "$lf" >/dev/null 2>>"$OUT/perf-stat-$mode.txt"
  cat "$OUT/perf-stat-$mode.txt" | grep -vE '^(LOAD|SET|===)' | tail -20
  echo "===== perf record profile ($mode) ====="
  sudo perf record -q -e cpu-clock -F 999 -g -p "$CHPID" -o "/tmp/perf-$mode.data" -- "${PSQL[@]}" -f "$lf" >/dev/null 2>&1
  sudo perf report -i "/tmp/perf-$mode.data" --stdio --no-children 2>/dev/null \
    | grep -vE '^#' | head -30 > "$OUT/perf-prof-$mode-top.txt"
  echo "-- copy/adopt-relevant frames in $mode profile --" | tee "$OUT/perf-prof-$mode.txt"
  sudo perf report -i "/tmp/perf-$mode.data" --stdio --no-children 2>/dev/null \
    | grep -iE 'cloneResized|memcpy|memmove|ColumnString|ColumnVector|ColumnDecimal|createAdopted|convertToFull|insertRangeFrom|PODArray' \
    | head -25 | tee -a "$OUT/perf-prof-$mode.txt"
  rm -f "$lf" "$warm"
done
echo "DONE perf capture -> $OUT/perf-stat-{adopt,copy}.txt, perf-prof-{adopt,copy}*.txt"
