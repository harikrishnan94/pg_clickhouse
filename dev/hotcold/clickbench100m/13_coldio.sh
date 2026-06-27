#!/usr/bin/env bash
# Hot/Cold ClickBench-100M — Unit 3: cold-IO added-pressure (Andrey's concern).
#
# Forces the hot slice to be read COLD from disk: shrinks PG shared_buffers (so the 0.66/6.6 GB hot table
# does not fit) + drops the OS page cache before each run. Measures, over the SAME cold pages, peak memory
# (per-backend VmHWM, kernel high-water) + throughput + disk-read confirmation (/proc/<pid>/io read_bytes)
# for TWO arms:
#   native   = native PG scanning the hot table (offload off)         -- PG's own scan footprint
#   stream   = clickhouse_stream_relation (scan+columnize+SHM ring), drained by a CH consumer  -- the merge's hot arm
# The DELTA (stream peak - native peak) answers: does streaming add pressure BEYOND what PG already incurs?
# Prereg §UNIT3: streaming adds a BOUNDED increment (columnar staging + the fixed 64 MiB SHM ring), not runaway.
# HIGH-IMPACT: restarts PG with shared_buffers=256MB, RESTORES 16GB at the end (D-HC-0409).
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; CH_PORT="${CH_PORT:-21002}"
PSQLU="sudo -u postgres psql -d clickbench -tAqX -v ON_ERROR_STOP=0"
N="${N:-3}"; FRACS="${FRACS:-p01 p10}"; SB="${SB:-256MB}"; ORIG_SB="${ORIG_SB:-16GB}"
OUT="$HERE/results/bench"; mkdir -p "$OUT"; CIO="$OUT/coldio.tsv"
echo -e "arm\tf\trun\tpeak_rss_mb\trss_workers_mb\twall_ms\tdisk_read_mb\tshm_ring_mb" > "$CIO"
restore_sb(){ echo ">> RESTORING shared_buffers=$ORIG_SB + restart"; sudo -u postgres psql -tAc "ALTER SYSTEM SET shared_buffers='$ORIG_SB';" >/dev/null 2>&1; sudo pg_ctlcluster 18 main restart >/dev/null 2>&1 || sudo systemctl restart postgresql@18-main; sleep 3; sudo -u postgres psql -tAc "show shared_buffers;"; }
trap restore_sb EXIT
echo ">> shrinking shared_buffers to $SB + restart (was $ORIG_SB)"
sudo -u postgres psql -tAc "ALTER SYSTEM SET shared_buffers='$SB';" >/dev/null 2>&1
sudo pg_ctlcluster 18 main restart >/dev/null 2>&1 || sudo systemctl restart postgresql@18-main; sleep 3
echo ">> shared_buffers now: $(sudo -u postgres psql -tAc 'show shared_buffers;')"
$PSQLU -c "ALTER TABLE pg.hits SET (parallel_workers=4);" >/dev/null 2>&1   # modest parallelism for native

# poll max VmRSS (MB) of a pid + its PG parallel workers (leader_pid=$1) until $done_flag exists
poll_rss(){ local pid="$1" flag="$2" mx=0 wmx=0; while [ ! -f "$flag" ]; do
    local r; r=$(awk '/^VmRSS/{print int($2/1024)}' "/proc/$pid/status" 2>/dev/null||echo 0); [ "${r:-0}" -gt "$mx" ] && mx=$r
    # sum parallel workers' RSS
    local ws=0; for w in $(pgrep -P "$pid" 2>/dev/null) $(ps --ppid "$pid" -o pid= 2>/dev/null); do :; done
    local wsum=0; for wp in $($PSQLU -c "select pid from pg_stat_activity where leader_pid=$pid" 2>/dev/null); do
      local rr; rr=$(awk '/^VmRSS/{print int($2/1024)}' "/proc/$wp/status" 2>/dev/null||echo 0); wsum=$((wsum+rr)); done
    [ "$wsum" -gt "$wmx" ] && wmx=$wsum
    sleep 0.02; done; echo "$mx $wmx"; }

drop_caches(){ sync; echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null; }

for f in $FRACS; do for run in $(seq 1 $N); do
  # ---- native arm ----
  drop_caches; flag=/tmp/cio_done_$$; rm -f "$flag"
  ( $PSQLU -c "SET search_path=pg; SET pg_clickhouse.enable_shm_offload=off; SET max_parallel_workers_per_gather=4; SET statement_timeout='600s'; /*cio_native*/ SELECT count(*), sum(WatchID::numeric) FROM pg.hits_hot_${f};" >/dev/null 2>&1; touch "$flag" ) & qpid=$!
  sleep 0.15; bpid=$($PSQLU -c "select pid from pg_stat_activity where query like '%cio_native%' and pid<>pg_backend_pid() order by query_start desc limit 1" 2>/dev/null)
  io0=$(awk '/^read_bytes/{print $2}' /proc/${bpid:-self}/io 2>/dev/null||echo 0)
  t0=$(date +%s.%N); read prss pwrk <<<"$(poll_rss "${bpid:-1}" "$flag")"; wait $qpid 2>/dev/null; t1=$(date +%s.%N)
  io1=$(awk '/^read_bytes/{print $2}' /proc/${bpid:-self}/io 2>/dev/null||echo 0)
  echo -e "native\t${f}\t${run}\t${prss}\t${pwrk}\t$(awk -v a=$t0 -v b=$t1 'BEGIN{printf "%.0f",(b-a)*1000}')\t$(awk -v a=${io0:-0} -v b=${io1:-0} 'BEGIN{printf "%.0f",(b-a)/1048576}')\t0" | tee -a "$CIO"
  # ---- streaming arm ----
  drop_caches; rm -f "$flag" /tmp/cio_prod.out
  ( $PSQLU -c "LOAD 'pg_clickhouse'; SET search_path=pg,public; SET statement_timeout='600s'; /*cio_stream*/ SELECT clickhouse_stream_relation('pg.hits_hot_${f}'::regclass,'pgch_cio_${f}',65536);" >/tmp/cio_prod.out 2>&1; touch "$flag" ) & qpid=$!
  for i in $(seq 1 600); do [ -S "/tmp/clickhouse_shm_pgch_cio_${f}.sock" ]&&break; sleep 0.02; done
  bpid=$($PSQLU -c "select pid from pg_stat_activity where query like '%cio_stream%' and pid<>pg_backend_pid() order by query_start desc limit 1" 2>/dev/null)
  io0=$(awk '/^read_bytes/{print $2}' /proc/${bpid:-self}/io 2>/dev/null||echo 0)
  ringmb=$(ls -la /dev/shm/pgch_cio_${f} 2>/dev/null | awk '{printf "%.0f",$5/1048576}')
  # drain the ring with a CH consumer (counts the streamed rows)
  ( python3 "$HERE/mk_merge_sql.py" "$HERE/templates/q8.ch.sql" pure >/dev/null 2>&1 || true )  # noop keep python warm
  ncols=$(python3 -c "import sys;sys.path.insert(0,'$HERE');import importlib.util as u;s=u.spec_from_file_location('m','$HERE/mk_merge_sql.py');m=u.module_from_spec(s);s.loader.exec_module(m);print(m.full_hot_schema(m.load_colmap()))")
  t0=$(date +%s.%N)
  ( echo "SELECT count() FROM streamed_table('pgch_cio_${f}','${ncols}')" | curl -s "127.0.0.1:${CH_PORT}/?allow_experimental_streamed_table_function=1" --data-binary @- >/dev/null 2>&1 ) &
  read prss pwrk <<<"$(poll_rss "${bpid:-1}" "$flag")"; wait $qpid 2>/dev/null; t1=$(date +%s.%N)
  io1=$(awk '/^read_bytes/{print $2}' /proc/${bpid:-self}/io 2>/dev/null||echo 0)
  echo -e "stream\t${f}\t${run}\t${prss}\t${pwrk}\t$(awk -v a=$t0 -v b=$t1 'BEGIN{printf "%.0f",(b-a)*1000}')\t$(awk -v a=${io0:-0} -v b=${io1:-0} 'BEGIN{printf "%.0f",(b-a)/1048576}')\t${ringmb:-0}" | tee -a "$CIO"
done; done
rm -f /tmp/cio_done_$$
echo "== coldio done: $CIO =="