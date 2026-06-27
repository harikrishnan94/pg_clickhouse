#!/usr/bin/env bash
# Lean P2 K-bench: OFFLOAD-ONLY client-wall timer (no native runs, no oracle — correctness is already
# proven by verify_offload + the K=1/2/8=native checks). Varies the run-ahead K via the GUC, interleaved
# across rounds for drift control; consumer = live C1 ClickHouse. UNCAPPED (no cgroup) for speed — the
# K-RELATIVE comparison is cap-independent (the netem rate, not CPU, is the binding constraint here).
# Writes results/p2_kbench_<TAG>/walls.tsv (q  KV  round  wall_ms).
#   TAG=netem3gzc METHOD=msg_zerocopy KLIST="1 2 4 8" ROUNDS=3 N=4 SNDBUF=65536 QFILES="6 9" \
#       bash dev/hotcold/phase3/p2_kbench.sh
set -uo pipefail
TAG="${TAG:?}"; METHOD="${METHOD:-epoll}"; KLIST="${KLIST:-1 2 4 8}"; ROUNDS="${ROUNDS:-3}"
N="${N:-4}"; SNDBUF="${SNDBUF:-0}"; QFILES="${QFILES:-6 9}"; PGDB="${PGDB:-tpch_sf10}"
HERE=/home/ubuntu/pg_clickhouse
OUT="$HERE/dev/hotcold/phase3/results/p2_kbench_$TAG"; rm -rf "$OUT"; mkdir -p "$OUT"; TSV="$OUT/walls.tsv"
printf 'q\tKV\tround\twall_ms\n' > "$TSV"

runq(){   # $1=KV $2=sql ; runs once, prints wall_ms
  local kv="$1" sql="$2" t0 t1
  t0=$(date +%s.%N)
  sudo -u postgres psql -d "$PGDB" -tAqX -v ON_ERROR_STOP=0 >/dev/null 2>&1 <<SQL
LOAD 'pg_clickhouse'; SET search_path=pg; SET pg_clickhouse.local_ch_server='ch_bench';
SET pg_clickhouse.shm_min_rows=0;
SET pg_clickhouse.session_settings='allow_experimental_streamed_table_function 1, max_threads 16';
SET pg_clickhouse.enable_shm_offload=on; SET pg_clickhouse.shm_transport_mode='tcp';
SET pg_clickhouse.tcp_send_method='$METHOD'; SET pg_clickhouse.tcp_send_inflight_blocks=$kv;
SET pg_clickhouse.tcp_sndbuf_bytes=$SNDBUF; SET pg_clickhouse.tcp_send_delay_us=${DELAYUS:-0};
SET max_parallel_workers=64; SET max_parallel_workers_per_gather=8;
$sql
SQL
  t1=$(date +%s.%N)
  awk "BEGIN{printf \"%.0f\", ($t1-$t0)*1000}"
}

echo "=== p2_kbench TAG=$TAG METHOD=$METHOD KLIST='$KLIST' R=$ROUNDS N=$N SNDBUF=$SNDBUF QFILES='$QFILES' $(date -u +%FT%TZ) ==="
echo "lo: $(tc qdisc show dev lo | head -1)"
for r in $(seq 1 "$ROUNDS"); do
  for KV in $KLIST; do
    for q in $QFILES; do
      sql=$(sed '/^EXPLAIN/d' "$HERE/dev/tpch/queries/$q.sql")
      runq "$KV" "$sql" >/dev/null   # warmup (one untimed run)
      for i in $(seq 1 "$N"); do
        w=$(runq "$KV" "$sql")
        printf 'tpch%s\t%s\t%s\t%s\n' "$q" "$KV" "$r" "$w" >> "$TSV"
      done
    done
    echo "  round $r KV=$KV done $(date -u +%T)"
  done
done
echo "=== P2 KBENCH DONE TAG=$TAG $(date -u +%FT%TZ) ==="
