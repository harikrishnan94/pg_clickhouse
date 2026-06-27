#!/usr/bin/env bash
# Hot/Cold ClickBench-100M — Unit 0 step 3 (PG side): fill the hot heap tables in PostgreSQL.
#
# Creates pg.hits_hot_p01/p05/p10 (LIKE pg.hits — the canonical 105-col ClickBench heap, plain
# heap, no index) and fills each from its CH counterpart via the repo's validated CH->PG path:
#   COPY ... FROM PROGRAM 'curl ... "SELECT * FROM clickbench.hits_hot_pXX FORMAT TabSeparated"'
# CH TabSeparated is byte-compatible with PG text COPY (same \t / \N / backslash escaping — see
# clickbench-pg.sql header), so PG and CH hold identical hot rows. These heaps are the source the
# standalone producer (clickhouse_stream_relation) streams during the merged query.
set -euo pipefail
CH_PORT="${CH_PORT:-21002}"
PSQL=(sudo -u postgres psql -d clickbench -X -q -v ON_ERROR_STOP=1)
psql_t() { sudo -u postgres psql -d clickbench -tAqX -v ON_ERROR_STOP=1 -c "$1"; }

build_pg_hot() {  # $1=tag  $2=expected_N
    local tag="$1" want="$2"
    echo "[$(date -Is)] PG hits_hot_${tag}: create (LIKE pg.hits) + COPY from CH ..."
    "${PSQL[@]}" >/dev/null <<SQL
SET search_path=pg;
DROP TABLE IF EXISTS pg.hits_hot_${tag};
CREATE TABLE pg.hits_hot_${tag} (LIKE pg.hits);
COPY pg.hits_hot_${tag} FROM PROGRAM
  'curl -s --data-binary "SELECT * FROM clickbench.hits_hot_${tag} FORMAT TabSeparated" http://127.0.0.1:${CH_PORT}/';
SQL
    local got
    got=$(psql_t "SELECT count(*) FROM pg.hits_hot_${tag};")
    if [ "$got" = "$want" ]; then echo "  OK pg.hits_hot_${tag}: $got rows (== N)"; else echo "  FAIL pg.hits_hot_${tag}: $got != $want"; exit 1; fi
}

build_pg_hot p01 1000000
build_pg_hot p05 5000000
build_pg_hot p10 10000000

echo "=== PG hot tables ==="
psql_t "select c.relname, c.reltuples::bigint, pg_size_pretty(pg_total_relation_size(c.oid)) from pg_class c join pg_namespace n on n.oid=c.relnamespace where n.nspname='pg' and c.relname like 'hits_hot_%' order by 1;"
echo "=== disk ==="; df -h / | tail -1
echo "[$(date -Is)] PG hot tables ready."
