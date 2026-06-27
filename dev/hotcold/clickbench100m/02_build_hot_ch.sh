#!/usr/bin/env bash
# Hot/Cold ClickBench-100M — Unit 0 step 2 (CH side): materialize the hot subsets in ClickHouse.
#
# Materializes the N(f) most-recent rows (tuple >= B(f)) of clickbench.hits_100m into small
# CH tables clickbench.hits_hot_p01/p05/p10 (cumulative, by recency). These are the typed source
# from which the PG hot heap tables are filled (step 3, via the FDW) — robust typed transfer, no
# TSV text-escaping risk on the string-heavy ClickBench columns. Also reclaims stale tables.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; . "$HERE/boundaries.env"
CH_PORT="${CH_PORT:-21002}"
CH="curl -s 127.0.0.1:${CH_PORT}/?max_threads=8"
ch() { echo "$1" | $CH --data-binary @- ; }

echo "[$(date -Is)] reclaim stale tables (empty hits_idx, prior-session cold split) ..."
ch "DROP TABLE IF EXISTS clickbench.hits_idx"
ch "DROP VIEW  IF EXISTS clickbench.hits_cold_10"
ch "DROP TABLE IF EXISTS clickbench.hits_cold_10_base"

build_hot() {  # $1=tag(p01/p05/p10)  $2=et  $3=wid  $4=uid  $5=cid  $6=ed  $7=expected_N
    local tag="$1" et="$2" wid="$3" uid="$4" cid="$5" ed="$6" want="$7"
    echo "[$(date -Is)] building hits_hot_${tag} (hot = tuple >= B) ..."
    ch "DROP TABLE IF EXISTS clickbench.hits_hot_${tag}"
    ch "CREATE TABLE clickbench.hits_hot_${tag} ENGINE = MergeTree ORDER BY tuple()
        SETTINGS index_granularity = 8192 AS
        SELECT * FROM clickbench.hits_100m
        WHERE (EventTime, WatchID, UserID, CounterID, EventDate)
              >= (toDateTime('${et}'), ${wid}, ${uid}, ${cid}, toDate('${ed}'))"
    local got
    got=$(ch "SELECT count() FROM clickbench.hits_hot_${tag}")
    if [ "$got" = "$want" ]; then echo "  OK hits_hot_${tag}: $got rows (== N)"; else echo "  FAIL hits_hot_${tag}: $got != $want"; exit 1; fi
}

build_hot p01 "$B01_EventTime" "$B01_WatchID" "$B01_UserID" "$B01_CounterID" "$B01_EventDate" 1000000
build_hot p05 "$B05_EventTime" "$B05_WatchID" "$B05_UserID" "$B05_CounterID" "$B05_EventDate" 5000000
build_hot p10 "$B10_EventTime" "$B10_WatchID" "$B10_UserID" "$B10_CounterID" "$B10_EventDate" 10000000

echo "=== nesting check (cumulative: p01 ⊂ p05 ⊂ p10) — boundary EventTimes must be ordered ==="
ch "SELECT 'p01' t, count() FROM clickbench.hits_hot_p01 UNION ALL SELECT 'p05', count() FROM clickbench.hits_hot_p05 UNION ALL SELECT 'p10', count() FROM clickbench.hits_hot_p10 ORDER BY t FORMAT TSV"
echo "=== disk ==="; df -h / | tail -1
echo "[$(date -Is)] hot CH tables ready."
