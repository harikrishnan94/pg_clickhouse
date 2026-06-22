# Blocker: Decimal-over-SHM offload crashes the producer at TPC-H SF=10

**Status:** open, surfaced by the TPC-H SF=10 benchmark sanity. Not fixed here
(the task is to surface engine blockers, not change the offload engine).

## Summary

Offloading an aggregate over PostgreSQL **`NUMERIC`/`DECIMAL` columns** via the
SHM path (`pg_clickhouse.enable_shm_offload=on` ->
`streamed_table()`) over the full SF=10 `lineitem` (~60M rows) is **memory-unsafe
and data-dependent**. It manifests two ways, nondeterministically:

* the PostgreSQL **backend SIGSEGVs** mid-stream — `client backend ... was
  terminated by signal 11: Segmentation fault` — which takes the whole cluster
  into crash recovery; or
* the backend trips a wild allocation — `ERROR: invalid memory alloc request
  size 18446744073709551602` (i.e. `(size_t)(n - m)` underflow, ~2^64), the
  offload is declined, and a `/dev/shm/pgch_<pid>_*` object + control socket are
  **leaked** (the abort path does not clean up).

The **Float64 path is unaffected** and the **ClickBench workload** (no Decimal
columns) is **100% clean**, which localizes the bug to the Decimal-over-SHM wire
encoding/adoption added in the most recent commits
("Offload PostgreSQL numeric columns as ClickHouse Decimal over SHM" /
"Add zero-copy Decimal adoption to the SHM streamed_table ABI").

## Evidence (SF=10 `lineitem`, dedicated server, otherwise-idle host)

| # | Query (offloaded) | Columns streamed | Result |
|---|---|---|---|
| q6 (in sanity) | `sum(l_extendedprice*l_discount)` **WHERE** shipdate∈1994 ∧ discount∈[.05,.07] ∧ qty<24 | decimals, **filtered** | **OK**, `shm==native`, read 59,986,052 rows, 917 SHM blocks |
| D | `sum(l_extendedprice*l_discount)` **no WHERE** | decimals, full table | `ERROR: invalid memory alloc request size 18446744073709551602` |
| C | `sum(l_extendedprice*(1-l_discount))` no WHERE | decimals, full table | **SIGSEGV** (server closed the connection) |
| B | `sum(l_extendedprice*(1-l_discount)*(1+l_tax))` no WHERE | decimals, full table | `ERROR: invalid memory alloc request size 18446744073709551612` |
| q1agg | Q1 sums + `GROUP BY l_returnflag,l_linestatus` | 4 decimals + 2 char + date | **SIGSEGV** after ~2.49M rows streamed |
| — control: bpchar | `count(*) WHERE l_returnflag='N'` | one `CHAR(1)` | OK (so `bpchar` streaming is fine) |
| — control: GROUP BY char | `... count(*) GROUP BY l_returnflag,l_linestatus` | two `CHAR(1)` + count | OK (so GROUP-BY-on-char is fine) |
| E | `sum(l_extendedprice*(1-l_discount)*(1+l_tax))` no WHERE, **`lineitem_f64`** | Float64 | **OK**, offloaded |
| F | `sum(l_extendedprice*l_discount)` no WHERE, **`lineitem_f64`** | Float64 | **OK**, offloaded |

The same expression `sum(l_extendedprice*l_discount)` is correct **with** q6's
selective filter (D vs q6) and correct on **Float64** (F). So the trigger is a
specific **Decimal value** in the unfiltered set whose wire length is computed
incorrectly (the varying `~2^64-N` sizes are a length subtraction underflow),
and depending on heap layout it either faults or hits the bad `palloc`.

## Reproduce

```sh
RUN_ID=tpchcb ../bench/ch-bench-server.sh start
make ch && make pg
PROBE_DECIMAL_BUG=1 RUN_ID=tpchcb ./sanity.sh   # crashes the producer on the Decimal queries
```

or directly:

```sh
sudo -u postgres psql -d tpch_sf10 <<'SQL'
LOAD 'pg_clickhouse';
SET pg_clickhouse.local_ch_server='ch_bench';
SET pg_clickhouse.shm_min_rows=0;
SET pg_clickhouse.session_settings='allow_experimental_streamed_table_function 1';
SET pg_clickhouse.enable_shm_offload=on;
SET search_path=pg;
SELECT sum(l_extendedprice*l_discount) FROM lineitem;   -- SIGSEGV or invalid-alloc
SQL
```

## Impact on the benchmark

* `make sanity` (default) runs only the supported Decimal case (filtered q6) plus
  the whole Float64 path, so it is green and does not crash the shared cluster.
* The full Decimal Q1/Q18 SHM offload over SF=10 cannot be benchmarked until the
  encoding bug is fixed; `make run` will show those queries as not-offloaded /
  errored rather than crashing the harness around them.
* FDW pushdown and native PostgreSQL modes are unaffected.
