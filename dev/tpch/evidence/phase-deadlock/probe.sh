#!/usr/bin/env bash
# Sample CH liveness + PG stream workers + SHM rings. Usage: probe.sh <iters> <sleep_s>
set +e
CH=http://127.0.0.1:21002
ITERS="${1:-10}"; SLP="${2:-3}"
for i in $(seq 1 "$ITERS"); do
  ts=$(date +%H:%M:%S.%2N)
  proc=$(curl -s --max-time 10 "$CH/" --data-binary \
    "SELECT query_id, round(elapsed,1), read_rows, written_rows, peak_memory_usage FROM system.processes WHERE query NOT LIKE '%system.proc%' FORMAT TSV" 2>/dev/null)
  workers=$(sudo -u postgres psql -d tpch_sf10 -tAqX \
    -c "SELECT count(*) FROM pg_stat_activity WHERE backend_type='pg_clickhouse shm stream'" 2>/dev/null)
  wstates=$(sudo -u postgres psql -d tpch_sf10 -tAqX -F',' \
    -c "SELECT string_agg(coalesce(wait_event_type,'')||':'||coalesce(wait_event,'run')||':'||state,' ') FROM pg_stat_activity WHERE backend_type='pg_clickhouse shm stream'" 2>/dev/null)
  rings=$(ls /dev/shm 2>/dev/null | grep -c '^pgch_')
  printf '[%s] i=%s workers=%s rings=%s\n   proc=%s\n   wstates=%s\n' "$ts" "$i" "$workers" "$rings" "${proc:-<none>}" "${wstates:-<none>}"
  sleep "$SLP"
done
exit 0
