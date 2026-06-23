/*-------------------------------------------------------------------------
 *
 * shm_page_reader.h
 *      Vectorized, page-at-a-time columnar heap reader for the SHM-offload
 *      producer. Replaces the scalar table_scan_getnextslot + slot_getallattrs
 *      loop with a single integrated reader that:
 *        - skips per-tuple visibility work on all-visible pages, and otherwise
 *          classifies tuples with a branch-light hint-bit / xmin / xmax kernel,
 *          falling back to HeapTupleSatisfiesVisibility (MVCC) for any tuple it
 *          cannot decide;
 *        - deforms only the projected attributes, up to the highest needed
 *          attno, directly into the shared ShmColumnizer.
 *
 *      The visible-row set is exactly equal to what table_scan_getnextslot
 *      would yield under the same snapshot. The reader is fail-closed: when the
 *      relation, snapshot, or projection is not eligible it declines (the
 *      caller uses the scalar reader instead), and per-tuple it falls back to
 *      the exact MVCC oracle.
 *
 * Copyright (c) 2025-2026, ClickHouse, Inc.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_CLICKHOUSE_SHM_PAGE_READER_H
#define PG_CLICKHOUSE_SHM_PAGE_READER_H

#include "postgres.h"

#include "utils/relcache.h"
#include "utils/snapmgr.h"

#include "shm_offload.h"
#include "shm_producer.h"

/*
 * Whole-relation eligibility for the vectorized reader. Returns false (so the
 * caller uses the scalar reader) for a non-MVCC or recovery snapshot,
 * serializable isolation / SSI conflict tracking, a non-heap table AM, a
 * projected Decimal column, or a projected column with a fast-default missing
 * value. The decision is made once, before any block is published.
 */
extern bool pgch_vectorized_reader_eligible(Relation rel, Snapshot snapshot,
                                            const ShmOffloadColumn *cols, int ncols);

/*
 * Stream `rel` under `snapshot` with the vectorized page reader, projecting
 * `cols`, into SHM blocks of up to `rows_per_block` rows via `producer`, then
 * signal end-of-stream. Returns the number of rows streamed. Caller must have
 * confirmed pgch_vectorized_reader_eligible first.
 */
extern uint64 pgch_stream_relation_vectorized(Relation rel, Snapshot snapshot,
                                              const ShmOffloadColumn *cols, int ncols,
                                              ShmProducer *producer, size_t rows_per_block);

#endif /* PG_CLICKHOUSE_SHM_PAGE_READER_H */
