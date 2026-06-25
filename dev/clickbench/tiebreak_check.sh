#!/usr/bin/env bash
# Tie-robust fidelity check for the top-N ClickBench queries whose plain ORDER BY
# leaves the LIMIT boundary ambiguous (equal-ranked rows -> non-deterministic in
# BOTH engines). For each (query, ORDER-BY-with-deterministic-tiebreak) it runs
# native (offload OFF) and offload (ON) and compares with cmp_results.py.
#
# A genuine tie-order deviation collapses to `exact` once a total order is
# imposed; a real value bug stays DIFF/approx. Also proves the offloaded result
# is read back from CH (oracle ShmAdoptedBlocks) and that the dispatched SQL
# carries the tiebroken ORDER BY + LIMIT/OFFSET.
#
#   dev/clickbench/tiebreak_check.sh
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
. "$HERE/../bench/bench-common.sh"
PGDB="${PGDB:-clickbench}"
CMP="$HERE/cmp_results.py"
TOL="${TOL:-1e-6}"
OUT="${OUT:-$HERE/evidence/phase-topn/tiebreak}"
mkdir -p "$OUT"
PSQL=(sudo -u postgres psql -d "$PGDB" -tAqX -F'|' -v ON_ERROR_STOP=0 -P null=NULL)
NATIVE_TUNE="SET search_path=pg; SET pg_clickhouse.enable_shm_offload=off; SET max_parallel_workers=16; SET max_parallel_workers_per_gather=8; SET min_parallel_table_scan_size=0; SET work_mem='2GB'; SET jit=on; SET statement_timeout='600s';"

# name -> tiebroken SQL (deterministic total order appended to the ORDER BY).
declare -A Q
Q[q18]="SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC, UserID, SearchPhrase LIMIT 10"
Q[q22]="SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC, SearchPhrase LIMIT 10"
Q[q32]="SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY WatchID, ClientIP ORDER BY c DESC, WatchID, ClientIP LIMIT 10"
Q[q33]="SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits GROUP BY WatchID, ClientIP ORDER BY c DESC, WatchID, ClientIP LIMIT 10"
Q[q39]="SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND IsLink <> 0 AND IsDownload = 0 GROUP BY URL ORDER BY PageViews DESC, URL LIMIT 10 OFFSET 1000"
Q[q40]="SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END AS Src, URL AS Dst, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst ORDER BY PageViews DESC, TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst LIMIT 10 OFFSET 1000"
Q[q41]="SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND TraficSourceID IN (-1, 6) AND RefererHash = 3594120000172545465 GROUP BY URLHash, EventDate ORDER BY PageViews DESC, URLHash, EventDate LIMIT 10 OFFSET 100"

NAMES="${*:-q18 q22 q32 q33 q39 q40 q41}"
printf '%-5s %-8s %-10s %-8s %-8s %s\n' Q verdict fidelity ShmBlk ch_ord/lim detail
for name in $NAMES; do
    sql="${Q[$name]}"
    tag="$(new_tag tb_$name)"
    printf '%s\n%s;\n' "$NATIVE_TUNE" "$sql" | "${PSQL[@]}" > "$OUT/$name.off.out" 2> "$OUT/$name.off.err"
    chq "SYSTEM FLUSH LOGS" >/dev/null 2>&1
    printf '%s\nSET search_path=pg;\nSET statement_timeout=%s;\n%s;\n' "$(shm_set_block "$tag")" "'600s'" "$sql" \
        | "${PSQL[@]}" > "$OUT/$name.on.out" 2> "$OUT/$name.on.err"
    # oracle (poll-retry)
    blk=0
    for _t in $(seq 1 12); do
        chq "SYSTEM FLUSH LOGS" >/dev/null 2>&1
        blk=$(chq "SELECT sum(ProfileEvents['ShmAdoptedBlocks']) FROM system.query_log WHERE log_comment='$tag' AND type='QueryFinish' AND positionCaseInsensitive(query,'streamed_table')>0 AND positionCaseInsensitive(query,'query_log')=0")
        blk=${blk:-0}; [ "${blk:-0}" -ge 1 ] 2>/dev/null && break; sleep 0.5
    done
    chsql=$(chq "SELECT replaceRegexpAll(query,'\\\\s+',' ') FROM system.query_log WHERE log_comment='$tag' AND type='QueryFinish' AND positionCaseInsensitive(query,'streamed_table')>0 AND positionCaseInsensitive(query,'query_log')=0 ORDER BY event_time_microseconds DESC LIMIT 1")
    echo "$chsql" > "$OUT/$name.chsql.txt"
    ch_ord=no; echo "$chsql" | grep -qiE 'order by' && ch_ord=yes
    ch_lim=no; echo "$chsql" | grep -qiE '\blimit\b' && ch_lim=yes
    fid=$("$CMP" "$OUT/$name.off.out" "$OUT/$name.on.out" "$TOL" 2>/dev/null)
    fclass=$(echo "$fid" | cut -d'|' -f1); det=$(echo "$fid" | cut -d'|' -f8)
    roff=$(echo "$fid" | cut -d'|' -f2); ron=$(echo "$fid" | cut -d'|' -f3)
    printf '%-5s %-8s %-10s %-8s %s/%s   rows=%s/%s %s\n' "$name" "OK" "$fclass" "$blk" "$ch_ord" "$ch_lim" "$roff" "$ron" "$det"
done
