#!/usr/bin/env bash
# Hot/Cold ClickBench-100M — Unit 0 step 1: build the indexed 100M source table.
#
# Builds clickbench.hits_idx = clickbench.hits_100m + a synthetic, deterministic,
# EXACT recency rank column hc_row_id (UInt64, 1..N):
#
#   hc_row_id = rowNumberInAllBlocks()+1 over (ORDER BY EventTime DESC, WatchID DESC,
#               UserID DESC, CounterID DESC, EventDate DESC)
#
# Smallest hc_row_id = most recent row. The full-tuple ORDER BY makes the enumeration
# deterministic for this build; rowNumberInAllBlocks GUARANTEES a bijection 1..N even
# across rows that share an EventTime second (the sec-granularity tie a pure-timestamp
# cutoff cannot split — see D-HC-0403). hc_row_id is then PERSISTED, so every downstream
# fraction split is exact and reproducible:
#   hot(f)  = hc_row_id <= N(f)        cold(f) = hc_row_id > N(f)
#   N(1%)=1,000,000  N(5%)=5,000,000  N(10%)=10,000,000   (of 99,997,497 rows)
#
# The table is stored ORDER BY the canonical ClickBench primary key so it is a FAIR
# pure-CH-100M baseline (SELECT <agg> FROM hits_idx); the cold arm is the filtered
# subquery hits_idx WHERE hc_row_id > N(f)  (D-HC-0404). Numbering runs single-threaded
# (rowNumberInAllBlocks determinism); the build takes ~10-20 min on this host.
set -euo pipefail
CH_PORT="${CH_PORT:-21002}"
CH="curl -s 127.0.0.1:${CH_PORT}/"
ch() { echo "$1" | $CH --data-binary @- ; }

echo "[$(date -Is)] dropping any stale hits_idx ..."
ch "DROP TABLE IF EXISTS clickbench.hits_idx"

echo "[$(date -Is)] building clickbench.hits_idx from hits_100m (single-thread numbering) ..."
t0=$(date +%s)
ch "CREATE TABLE clickbench.hits_idx
    ENGINE = MergeTree
    ORDER BY (CounterID, EventDate, UserID, EventTime, WatchID)
    SETTINGS index_granularity = 8192 AS
    SELECT *, toUInt64(rowNumberInAllBlocks() + 1) AS hc_row_id
    FROM (
        SELECT * FROM clickbench.hits_100m
        ORDER BY EventTime DESC, WatchID DESC, UserID DESC, CounterID DESC, EventDate DESC
    )
    SETTINGS max_threads = 1, max_block_size = 65505, max_bytes_before_external_sort = '8G'"
t1=$(date +%s)
echo "[$(date -Is)] build done in $((t1-t0))s"

echo "=== exactness oracle (count == min..max bijection == uniqExact) ==="
ch "SELECT count() AS n, min(hc_row_id) AS lo, max(hc_row_id) AS hi, uniqExact(hc_row_id) AS distinct_ids,
        if(count()=max(hc_row_id) AND min(hc_row_id)=1 AND uniqExact(hc_row_id)=count(),'EXACT','BROKEN') AS verdict
    FROM clickbench.hits_idx FORMAT Vertical"
echo "=== recency: hot(<=1e7) must be most recent; cold(>1e7) older ==="
ch "SELECT 'hot10M' arm, toString(min(EventTime)) mn, toString(max(EventTime)) mx, count() c FROM clickbench.hits_idx WHERE hc_row_id<=10000000 FORMAT TSV"
ch "SELECT 'cold'   arm, toString(min(EventTime)) mn, toString(max(EventTime)) mx, count() c FROM clickbench.hits_idx WHERE hc_row_id>10000000 FORMAT TSV"
echo "[$(date -Is)] hits_idx ready."
