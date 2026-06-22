# TPC-H SF=10 benchmark harness, wired to a dedicated, isolated ClickHouse server.
#
# Bring the server up first (writes the manifest this Makefile reads):
#   RUN_ID=tpchcb ../bench/ch-bench-server.sh start
# then:
#   make ch    # load ClickHouse tpch_sf10 (SF=10 + lineitem_f64) via the manifest TCP port
#   make pg    # load PostgreSQL tpch_sf10 (heap + FDW import) via the manifest HTTP port
#   make sanity# prove native / FDW / SHM-offload modes (on==off oracle)
#   make run   # full 22-query, 3-mode matrix -> Markdown table
#   make clean # remove result/

RUN_ID    ?= tpchcb
CH_BIN    ?= /home/ubuntu/ClickHouse/build/reldeb/programs/clickhouse
BENCH     := ../bench/ch-bench-server.sh
PG_DB     := tpch_sf10
PSQL      := sudo -u postgres psql

# Resolve the dedicated server's ports from its manifest (empty if not started).
CH_HOST      := $(shell RUN_ID=$(RUN_ID) $(BENCH) env 2>/dev/null | sed -n 's/^CH_HOST=//p')
CH_TCP_PORT  := $(shell RUN_ID=$(RUN_ID) $(BENCH) env 2>/dev/null | sed -n 's/^CH_TCP_PORT=//p')
CH_HTTP_PORT := $(shell RUN_ID=$(RUN_ID) $(BENCH) env 2>/dev/null | sed -n 's/^CH_HTTP_PORT=//p')

.PHONY: check-server ch pg sanity run clean links

check-server:
	@test -n "$(CH_TCP_PORT)" || { \
	  echo "No dedicated server for RUN_ID=$(RUN_ID). Start it first:"; \
	  echo "  RUN_ID=$(RUN_ID) $(BENCH) start"; exit 1; }

ch: check-server tpch-ch.sql
	$(CH_BIN) client --host $(CH_HOST) --port $(CH_TCP_PORT) --queries-file tpch-ch.sql

pg: check-server tpch-pg.sql
	@$(PSQL) -tAc "SELECT 1 FROM pg_database WHERE datname='$(PG_DB)'" | grep -q 1 \
	  || $(PSQL) -c "CREATE DATABASE $(PG_DB)"
	$(PSQL) --quiet --no-psqlrc --set ON_ERROR_STOP=1 -d $(PG_DB) \
	  -v ch_host=$(CH_HOST) -v ch_http_port=$(CH_HTTP_PORT) -f tpch-pg.sql

sanity: check-server run.sh sanity.sh
	RUN_ID=$(RUN_ID) ./sanity.sh

run: check-server run.sh
	RUN_ID=$(RUN_ID) ./run.sh

clean:
	rm -rf result

links:
	@for i in $$(seq 1 22); do printf '  [Query %s] https://github.com/harikrishnan94/pg_clickhouse/blob/streamed-table-shm-offload/dev/tpch/queries/%s.sql\n' "$$i" "$$i"; done
