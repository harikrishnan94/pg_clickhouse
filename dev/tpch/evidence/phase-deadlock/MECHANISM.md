# Q8/Q9/Q11/Q14 hash-join-BUILD deadlock — root-caused mechanism

Date 2026-06-25. Repro: `streamed_table(lineitem) ALL INNER JOIN streamed_table(part)`
(TPC-H Q14, SF10), offload ON. CH 26.6.1.1 (patched streamed_table), RUN_ID=tpchcb
@127.0.0.1:21002. PG tpch_sf10, schema pg. Ring depth K=4 (PGCH_SHM_RING_DEPTH_K).

## Symptom (canonical)
`Code: 781 SHM_PRODUCER_STALL '/pgch_..._2_2_0': no producer publication progress for
30009ms` — the **part** (build/right) side producer stalls. Dispatched CH SQL:
`r1(=UNION ALL of 7 lineitem streamed_table) ALL INNER JOIN r2(=UNION ALL of 4 part
streamed_table) ON l_partkey=p_partkey`, with the promo_revenue sum pushed over the
join. So **part is the build (right) side**.

## Evidence (3 independent classes, converging) — frozen instance, default 30s stall
raised to 600s for a wide observation window (DIAGNOSTIC ONLY; not a fix).

### 1. Liveness — read_rows frozen
`system.processes`: read_rows = **1048576** constant while elapsed climbs
26.9s → 174.3s (dl_q14_diag.liveness.log). 1048576 = 16 × 65536.

### 2. Thread state at the freeze (dl_q14_diag.gdb.key.txt, dl_q14_diag.producers.txt)
- CH consumer: executor master (HTTPHandler) blocked in
  `DB::Epoll::getManyReady(timeout=-1)` ← `PollingQueue::wait` ←
  `ExecutorTasks::processAsyncTasks` ← `PipelineExecutor::executeImpl(num_threads=11)`
  (PipelineExecutor.cpp:614). All 11 `QueryPipelineEx` worker threads parked in
  `pthread_cond_wait` ← `ExecutorTasks::tryGetTask` (no ready task). 4 source-owned
  `asyncWakeBridgeLoop` threads in `poll()`. => consumer waiting for a producer fd.
- PG producers: ALL 11 `pg_clickhouse shm stream` workers blocked in
  `publish_block` at `src/shm_producer.c:622` (the `pg_usleep(1000)` inside
  `while(slot->state != SHM_STATE_EMPTY)`). => producers waiting for the consumer to
  free a ring slot.

### 3. Ring slot states (parse_rings.py → dl_q14_diag.ringstates.txt)
All 44 slots (11 rings × 4) are PUBLISHED. Split by source:
- **part (build), rings `_2_2_0..3`**: all 16 slots `PUBLISHED seq1 retain_refcount=1`
  — drained by the consumer **and still RETAINED** (held in the hash table).
  16 × 65536 = 1048576 = read_rows exactly.
- **lineitem (probe), rings `_1_1_0..6`**: all 28 slots `PUBLISHED seq1 refcount=0`
  — published but **never drained** (the probe has not started).

## Mechanism (why it deadlocks)
The ClickHouse hash-join **build** stores right-side blocks for the lifetime of the
query. The build columns come from `streamed_table()` as **zero-copy adopted** columns
that alias a slot in the producer's bounded SHM ring; each retained column keeps its
`RetainToken` alive, so the consumer never transitions that slot back to EMPTY. The
ring has only **K=4 slots per producer**. Once the build has adopted+retained the
ring's worth of part blocks (4 per ring × 4 rings = 16 blocks = 1,048,576 rows), every
part slot is PUBLISHED+retained. part has ~2M rows (~31 blocks) > ring capacity, so the
part producers block in `publish_block` waiting for an EMPTY slot that the hash join
will never release. The build therefore never finishes → the probe never starts →
lineitem blocks are never drained → the consumer parks in epoll forever. True deadlock
(read_rows frozen; a larger stall budget does not help — verified 600s).

`materializeColumnsFromRightBlock` (HashJoin.cpp) un-wraps const/sparse/lowcardinality/
nullable but does NOT un-adopt; the single-chunk join-build squash is a passthrough
(Squashing.cpp:99-101 returns the chunk as-is) and `min_joined_block_size_rows`
(=DEFAULT_BLOCK_SIZE≈65536) / `_bytes`(512KB) are far below ring capacity so squashing
flushes per block without copying → the adopted column reaches the hash table and is
retained there.

## Why 3–6-way joins (Q3/Q5/Q7/Q10/Q12/Q19) do NOT deadlock
Their build (right) sides are small dimension tables (region 5, nation 25, supplier
100k, customer filtered, etc.) whose ENTIRE build fits within the retained ring
capacity (≤ n_producers × 4 × 65536 rows), so the build completes before any producer
needs a 5th slot per ring; the probe then runs and drains+releases the big fact table
(lineitem) block-by-block (refcount returns to 0 → EMPTY). The deadlock is triggered
only when the BUILD side exceeds ring capacity — part (2M) and partsupp (8M) on the
build side (Q8/Q9/Q11/Q14). Shape-specific, not arity.

## Fix direction (Phase B)
The hash table must OWN its build data — it cannot indefinitely retain a column backed
by a bounded streaming ring. Materialise (copy) adopted columns to owned memory on the
build side, in `materializeColumnsFromRightBlock` (the single chokepoint for both
`HashJoin::addBlockToJoin` and `ConcurrentHashJoin::addBlockToJoin`), via
`IColumn::convertToFullColumnIfAdopted()` (no-op for non-adopted columns). Then each
consumed build slot is released as the build advances; the probe side stays zero-copy.
This is NOT a producer-side workaround and does NOT raise the stall timeout.
