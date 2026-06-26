#!/usr/bin/env bash
# Hot-Cold Phase 3 P1 parity: drift-controlled interleaved A/B of the PRODUCER send method,
# epoll (P1 .so) vs io_uring (baseline .so). The consumer is the live C1 ClickHouse (constant); only the
# producer .so is swapped between rounds (atomic install; bench DBs use session_preload + dynamic bgworker
# bgw_library_name, so a fresh psql per query loads the swapped .so — no PG restart). The LTO bitcode is
# left as P1's for BOTH rounds, so it is a constant, not a confound — only the send method varies.
#   bash dev/hotcold/phase3/p1_interleave_ab.sh <p1_so> <baseline_so>
set -uo pipefail
export EXTRA_SS="" EXTRA_SET=""
P1SO="${1:?p1 .so}"; BLSO="${2:?baseline .so}"
ROUNDS="${ROUNDS:-5}"; N="${N:-5}"
INSTALLED=/usr/lib/postgresql/18/lib/pg_clickhouse.so
HERE=/home/ubuntu/pg_clickhouse
WS="$HERE/dev/wsweep-report/wsweep_split.sh"
ROOT="$HERE/dev/hotcold/phase3/results/p1_interleave"
CBQ="${CBQ:-2 17 33 38}"; TPQ="${TPQ:-6 7 9 14}"
rm -rf "$ROOT"; mkdir -p "$ROOT"

swap_so(){   # $1 = .so path, $2 = label for the assert
  sudo install -c -m 755 "$1" "$INSTALLED"
  local u; u=$(ldd "$INSTALLED" 2>/dev/null | grep -ic uring)
  echo "   installed $2: liburing_links=$u"
}
measure_set(){   # $1 = out_root
  BENCH=clickbench TRANSPORT=tcp W_LIST=8 N="$N" K=1 QUERIES="$CBQ" OUT="$1/tcp/clickbench" bash "$WS" >/dev/null 2>&1
  BENCH=tpch       TRANSPORT=tcp W_LIST=8 N="$N" K=1 QUERIES="$TPQ" OUT="$1/tcp/tpch"       bash "$WS" >/dev/null 2>&1
}
echo "=== P1 interleave A/B  epoll(P1)=$P1SO  io_uring(baseline)=$BLSO  ROUNDS=$ROUNDS N=$N  $(date -u +%FT%TZ) ==="
echo "consumer = live C1 ClickHouse (constant); lo qdisc: $(tc qdisc show dev lo | head -1)"
for r in $(seq 1 "$ROUNDS"); do
  echo "-- round $r / epoll(P1) $(date -u +%T) --";    swap_so "$P1SO" "P1(epoll)";    measure_set "$ROOT/p1/r$r"
  echo "-- round $r / io_uring(base) $(date -u +%T) --"; swap_so "$BLSO" "baseline(io_uring)"; measure_set "$ROOT/baseline/r$r"
done
# Restore P1 as the installed producer (the committed state) at the end.
swap_so "$P1SO" "P1(epoll) [restored]"
echo "=== P1 INTERLEAVE DONE  $(date -u +%FT%TZ) ==="
