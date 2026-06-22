TPC-H Benchmark (SF=10)
=======================

This directory contains scripts to execute the [TPC-H] benchmark queries at
**scaling factor 10** with the same data in ClickHouse and PostgreSQL, comparing
three execution modes:

1. **native PostgreSQL** (heap tables, `pg_clickhouse.enable_shm_offload = off`),
2. **`pg_clickhouse` FDW pushdown** (querying the imported `ch` schema),
3. **`pg_clickhouse` SHM offload** (`enable_shm_offload = on` ->
   `streamed_table()`; see [`../bench/README.md`](../bench/README.md)).

Most TPC-H queries are multi-table joins; the SHM offload fires only on
single-table scan + filter + aggregate + `GROUP BY` + `HAVING`, so only **Q1**
and **Q6** fully offload via SHM. TPC-H is kept mainly for the native-vs-FDW
comparison at scale, plus SHM where applicable. `run.sh` records per query which
mode actually offloaded via the per-session `SHOW
pg_clickhouse.last_query_used_clickhouse` (correct under concurrency).

There are two `lineitem` tables: the stock `DECIMAL(15,2)` `lineitem` and a
`lineitem_f64` where only the numeric columns (`l_quantity`, `l_extendedprice`,
`l_discount`, `l_tax`) become `double precision` (integer keys stay `INTEGER`),
derived from `lineitem`. SHM offload of exact `Decimal` is byte-identical to
native; `Float64` is order-dependent, so it is compared with a tolerance.

`run.sh` runs each query in [queries](queries) three times per mode and reports
averaged `Execution Time`; `sanity.sh` proves correctness for the SHM-eligible
queries. An illustrative SF=1 native-vs-FDW table (the new harness adds a SHM
column):

```md
|    Query   | PostgreSQL | pg_clickhouse | Pushdown |
| ----------:| ----------:| -------------:|:--------:|
|  [Query 1] |    4693 ms |        268 ms |     ✔︎    |
|  [Query 2] |     458 ms |       3446 ms |          |
|  [Query 3] |     742 ms |        111 ms |     ✔︎    |
|  [Query 4] |     270 ms |        130 ms |     ✔︎    |
|  [Query 5] |     337 ms |       1460 ms |     ✔︎    |
|  [Query 6] |     764 ms |         53 ms |     ✔︎    |
|  [Query 7] |     619 ms |         96 ms |     ✔︎    |
|  [Query 8] |     342 ms |        156 ms |     ✔︎    |
|  [Query 9] |    3094 ms |        298 ms |     ✔︎    |
| [Query 10] |     581 ms |        197 ms |     ✔︎    |
| [Query 11] |     212 ms |         24 ms |          |
| [Query 12] |    1116 ms |         84 ms |     ✔︎    |
| [Query 13] |     958 ms |       1368 ms |          |
| [Query 14] |     181 ms |         73 ms |     ✔︎    |
| [Query 15] |    1118 ms |        557 ms |          |
| [Query 16] |     497 ms |       1714 ms |          |
| [Query 17] |    1846 ms |      32709 ms |          |
| [Query 18] |    5823 ms |      10649 ms |          |
| [Query 19] |      53 ms |        206 ms |     ✔︎    |
| [Query 20] |     421 ms |             - |          |
| [Query 21] |    1349 ms |       4434 ms |          |
| [Query 22] |     258 ms |       1415 ms |          |
```

## Setup & Execution

The harness uses a dedicated, isolated ClickHouse server (own data dir + unique
ports, patched `streamed_table` binary), co-located with PostgreSQL so the SHM
path works. Start it first; the `Makefile` reads its `manifest.env` for the
current `RUN_ID`. All `psql` runs as the `postgres` OS user (the login user has
no PostgreSQL role on this host).

```sh
# 1. Dedicated ClickHouse server (see ../bench/README.md).
RUN_ID=tpchcb ../bench/ch-bench-server.sh start

# 2. Load ClickHouse tpch_sf10 (SF=10 tables + lineitem_f64) via the manifest TCP port.
RUN_ID=tpchcb make ch

# 3. Load PostgreSQL tpch_sf10 (heap tables + FDW import of the ch schema, incl.
#    ch.lineitem_f64) via the manifest HTTP port. Run AFTER `make ch` so the
#    IMPORT picks up lineitem_f64. The postgres OS user needs curl + zstd.
RUN_ID=tpchcb make pg

# 4. Sanity-check native / FDW / SHM-offload (the on==off oracle).
RUN_ID=tpchcb make sanity

# 5. (Optional) full 22-query, 3-mode matrix -> result/RESULTS.md.
RUN_ID=tpchcb make run

# 6. Teardown.
RUN_ID=tpchcb ../bench/ch-bench-server.sh stop   # then rm -rf "$CH_DIR" to reclaim
```

`make ch` runs [tpch-ch.sql](tpch-ch.sql) (database `tpch_sf10`, the [ClickHouse
TPC-H] tables + `lineitem_f64`, SF=10 `.tbl.zst` data from the ClickHouse S3
buckets). `make pg` runs [tpch-pg.sql](tpch-pg.sql) (schema `pg` with the
[PostgreSQL TPC-H] tables + `lineitem_f64`, the FDW server `ch_bench` over
`http`, and the `ch` schema imported from ClickHouse). `make sanity` and
`make run` engage SHM offload per session via `pg_clickhouse.local_ch_server`,
`shm_min_rows=0`, `enable_shm_offload=on`, and a `session_settings` carrying the
experimental flag and a unique `log_comment`.

Q11's `HAVING` fraction is the spec-defined `0.0001 / SF`, set to `0.00001` for
SF=10 in [queries/11.sql](queries/11.sql).

### Cleanup

```sh
make clean
```

### Links

Use this command to create link references for the Markdown links to each
query in the table output:

```sh
make links
```

## Queries

The [queries](queries) duplicate those from the [pgtpc project]. The only
changes are to explicitly compare against dates, rather than timestamps, since
ClickHouse does not implicitly cast them for the comparison. The changes are
all from something like this (as in [Query 1](queries/1.sql)):

```sql
l_shipdate <= date '1998-12-01' - interval '90d'
```

To

```sql
l_shipdate <= date(date '1998-12-01' - interval '90d')
```

Reveal the complete diff below for details.

<details>
<summary>Complete Queries Diff</summary>

```diff
diff --git queries/1.sql b/tpch/queries/1.sql
index 20cecac..e4f841b 100644
--- queries/1.sql
+++ b/tpch/queries/1.sql
@@ -15,7 +15,7 @@ select
 from
 	lineitem
 where
-	l_shipdate <= date '1998-12-01' - interval '90d'
+	l_shipdate <= date(date '1998-12-01' - interval '90d')
 group by
 	l_returnflag,
 	l_linestatus
diff --git queries/10.sql b/tpch/queries/10.sql
index d063981..5cd8cd4 100644
--- queries/10.sql
+++ b/tpch/queries/10.sql
@@ -20,7 +20,7 @@ where
 	c_custkey = o_custkey
 	and l_orderkey = o_orderkey
 	and o_orderdate >= date '1993-10-01'
-	and o_orderdate < date '1993-10-01' + interval '3month'
+	and o_orderdate < date(date '1993-10-01' + interval '3month')
 	and l_returnflag = 'R'
 	and c_nationkey = n_nationkey
 group by
diff --git queries/12.sql b/tpch/queries/12.sql
index 83dd6b4..b2bbd38 100644
--- queries/12.sql
+++ b/tpch/queries/12.sql
@@ -25,7 +25,7 @@ where
 	and l_commitdate < l_receiptdate
 	and l_shipdate < l_commitdate
 	and l_receiptdate >= date '1994-01-01'
-	and l_receiptdate < date '1994-01-01' + interval '1y'
+	and l_receiptdate < date(date '1994-01-01' + interval '1y')
 group by
 	l_shipmode
 order by
diff --git queries/14.sql b/tpch/queries/14.sql
index b8949b6..e5642ff 100644
--- queries/14.sql
+++ b/tpch/queries/14.sql
@@ -14,4 +14,4 @@ from
 where
 	l_partkey = p_partkey
 	and l_shipdate >= date '1995-09-01'
-	and l_shipdate < date '1995-09-01' + interval '1month';
+	and l_shipdate < date(date '1995-09-01' + interval '1month');
diff --git queries/15.sql b/tpch/queries/15.sql
index c4fba55..f0e2166 100644
--- queries/15.sql
+++ b/tpch/queries/15.sql
@@ -8,7 +8,7 @@ create or replace view revenue0 (supplier_no, total_revenue) as
 		lineitem
 	where
 		l_shipdate >= date '1996-01-01'
-		and l_shipdate < date '1996-01-01' + interval '3month'
+		and l_shipdate < date(date '1996-01-01' + interval '3month')
 	group by
 		l_suppkey;

diff --git queries/20.sql b/tpch/queries/20.sql
index babc01c..29de2f3 100644
--- queries/20.sql
+++ b/tpch/queries/20.sql
@@ -32,7 +32,7 @@ where
 					l_partkey = ps_partkey
 					and l_suppkey = ps_suppkey
 					and l_shipdate >= date '1994-01-01'
-					and l_shipdate < date '1994-01-01' + interval '1' year
+					and l_shipdate < date(date '1994-01-01' + interval '1' year)
 			)
 	)
 	and s_nationkey = n_nationkey
diff --git queries/4.sql b/tpch/queries/4.sql
index 7f7011e..75df4ab 100644
--- queries/4.sql
+++ b/tpch/queries/4.sql
@@ -8,7 +8,7 @@ select
 from
 	orders
 where
-	o_orderdate >= date '1993-07-01'and o_orderdate < date '1993-07-01' + interval '3month'
+	o_orderdate >= date '1993-07-01'and o_orderdate < date(date '1993-07-01' + interval '3month')
 	and exists (select * from lineitem where l_orderkey = o_orderkey and l_commitdate < l_receiptdate)
 group by
 	o_orderpriority
diff --git queries/5.sql b/tpch/queries/5.sql
index b085c1b..2ccd73e 100644
--- queries/5.sql
+++ b/tpch/queries/5.sql
@@ -21,7 +21,7 @@ where
 	and n_regionkey = r_regionkey
 	and r_name = 'ASIA'
 	and o_orderdate >= date '1994-01-01'
-	and o_orderdate < date '1994-01-01' + interval '1year'
+	and o_orderdate < date(date '1994-01-01' + interval '1year')
 group by
 	n_name
 order by
diff --git queries/6.sql b/tpch/queries/6.sql
index 053d79e..0e06313 100644
--- queries/6.sql
+++ b/tpch/queries/6.sql
@@ -8,6 +8,6 @@ from
 	lineitem
 where
 	l_shipdate >= date '1994-01-01'
-	and l_shipdate < date '1994-01-01' + interval '1year'
+	and l_shipdate < date(date '1994-01-01' + interval '1year')
 	and l_discount between .06 - 0.01 and .06 + 0.01
 	and l_quantity < 24;
```

</details>

  [TPC-H]: https://www.tpc.org/tpch/
  [ClickHouse TPC-H]: https://clickhouse.com/docs/getting-started/example-datasets/tpch
  [chenv]: https://clickhouse.com/docs/interfaces/cli#environment-variable-options
    "ClickHouse Client Docs: Environment variable options"
  [PostgreSQL TPC-H]: https://github.com/Vonng/pgtpc/tree/master/tpch/ddl
  [pgenv]: https://www.postgresql.org/docs/current/libpq-envars.html
    "PostgreSQL libpq Docs: Environment Variables"
  [pgtpc project]: https://github.com/Vonng/pgtpc/tree/master/tpch
