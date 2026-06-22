# SHM heap-offload verification

`verify_offload.sh` is an end-to-end test for the shared-memory heap-offload
path (`pg_clickhouse.enable_shm_offload`): a PostgreSQL heap relation's rows are
streamed into a co-located ClickHouse over POSIX shared memory and queried via
the `streamed_table()` table function.

For every query in the matrix it proves, **from ClickHouse's own system tables**
rather than the PostgreSQL plan, that the query was actually offloaded:

1. **Correctness** — the result with offload on is byte-identical to the result
   with offload off.
2. **Push-down really happened** — running the query adds exactly one new
   `streamed_table(...)` entry to `system.query_log`, that entry adopted at least
   one block from shared memory (`ProfileEvents['ShmAdoptedBlocks']`), and it read
   the full streamed table (`read_rows`). This is how *what pg_clickhouse executed*
   is cross-checked against *what ClickHouse executed*.
3. The exact query ClickHouse ran is printed for each case.

Two negative controls confirm that with the feature off, and for an ineligible
query (a bare `count(*)` that references no column), ClickHouse sees no
`streamed_table` query at all.

The script starts and stops its own ClickHouse server.

```sh
CH_BIN=/path/to/clickhouse PG_DB=shmdemo test/shm/verify_offload.sh
```

Defaults: `CH_BIN=/home/ubuntu/ClickHouse/build/reldeb/programs/clickhouse`,
`CH_DIR=/home/ubuntu/ClickHouse/tmp/chserver`, `PG_DB=shmdemo`. The PostgreSQL
must have `pg_clickhouse` installed; the script creates the extension, a
co-located `CREATE SERVER`, and the test tables. The library is preloaded into
the test database (`session_preload_libraries`) so the planner/executor hooks
install — required for offload to engage.
```
