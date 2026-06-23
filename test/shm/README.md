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

## Vectorized page reader: deform and visibility

`verify_columnar.sh` and `verify_visibility.sh` exercise the vectorized page
reader's two engines. Both expect an already-running co-located ClickHouse
(the `ch_bench` server) and compare against the blessed scalar scan:

```sh
RUN_ID=tpchcb dev/bench/ch-bench-server.sh start
test/shm/verify_columnar.sh     # column-major deform: group A / B / straddle / NULLs
test/shm/verify_visibility.sh   # visibility classify kernel + MVCC slow path
```

`verify_visibility.sh` is built to defeat the one trap specific to visibility:
**result-equivalence on all-visible data passes without ever running the
classifier or the slow path** (a `VACUUM (FREEZE)`'d relation takes the
all-visible page fast path and emits every tuple, so the not-all-visible
classifier and the MVCC oracle are never entered). Each of its cases therefore
*proves the intended path was taken*, via two independent instruments:

1. **`pg_visibility`** — proves the page-state precondition (e.g. the relation
   is genuinely *not* all-visible, so the classify path is the only one that can
   run). Independent of pg_clickhouse's own code. Fixtures set
   `autovacuum_enabled=false` so an insert-triggered autovacuum cannot silently
   mark them all-visible.
2. **The `shm_log_stream_stats` "shm visibility:" LOG line** — per-scan path and
   per-verdict counters (`pages_all_visible`, `pages_classified`,
   `visible_fast`, `invisible_fast`, `undecided`, `slow_visible`). Proves which
   classifier lane and the MVCC oracle actually executed.

Every case also asserts the offloaded result equals both the scalar scan
(`enable_shm_offload off`) and the scalar reference classifier
(`shm_vectorized_visibility off`) — kernel, reference, and PostgreSQL all agree
on the visible set. Coverage: each VISIBLE / INVISIBLE / UNDECIDED verdict, the
all-visible fast page path, concurrency (in-progress insert / delete / update,
multixact), a mixed page (visible + invisible + undecided in one scan), and a
block-flush straddle (> `rows_per_block`).
