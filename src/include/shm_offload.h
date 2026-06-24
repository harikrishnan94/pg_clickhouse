/*-------------------------------------------------------------------------
 *
 * shm_offload.h
 *      Heap -> ClickHouse streamed_table() offload: GUCs, PostgreSQL-type to
 *      wire-type mapping, and the heap-scan columnizer that feeds the SHM
 *      producer.
 *
 * Copyright (c) 2025-2026, ClickHouse, Inc.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_CLICKHOUSE_SHM_OFFLOAD_H
#define PG_CLICKHOUSE_SHM_OFFLOAD_H

#include "postgres.h"

#include "nodes/pg_list.h"
#include "port/atomics.h"
#include "utils/relcache.h"
#include "utils/snapshot.h"

#include "shm_producer.h"
#include "shm_visibility.h"     /* PgchVisStats */

/*
 * Shared, cross-process heap-block work allocator for the parallel vectorized
 * reader. Lives in the streaming workers' shared DSM. Each worker atomically
 * claims the next chunk of blocks (next_block fetch-add) so the W workers cover
 * blocks [0, nblocks) exactly once, with no gaps or overlap, under one shared
 * snapshot -- the same exactly-once guarantee a parallel seq scan provides.
 *
 * Pass NULL to the reader for the single-threaded path: it then scans the whole
 * relation [0, nblocks) sequentially, byte-identical to the original loop.
 */
typedef struct ShmBlockCursor {
    pg_atomic_uint32 next_block;    /* next unclaimed heap block number */
} ShmBlockCursor;

/* Registers the pg_clickhouse.* SHM-offload GUCs. Called from the extension's
 * _PG_init (option.c) before MarkGUCPrefixReserved. */
extern void pgch_shm_offload_init(void);

/* Installs the planner/executor hooks + CustomScan methods + the
 * last_query_used_clickhouse observability GUC. Called from pgch_shm_offload_init. */
extern void pgch_register_customscan_and_hooks(void);

/* Set true by the CustomScan executor when a scan actually offloads to ClickHouse. */
extern bool pgch_this_query_used_ch;

/* GUCs (defined in shm_offload.c, registered by pgch_shm_offload_init). */
extern bool  pgch_enable_shm_offload;
extern char *pgch_local_ch_server;
extern int   pgch_shm_ring_depth_k;
extern int   pgch_shm_data_region_mb;
extern int   pgch_shm_min_rows;
extern bool  pgch_use_vectorized_reader;
extern bool  pgch_use_columnar_deform;
extern bool  pgch_use_vectorized_visibility;
extern bool  pgch_log_stream_stats;
extern bool  pgch_enable_jit_deform;
extern int   pgch_jit_row_threshold;
extern int   pgch_shm_stream_workers;
extern int   pgch_shm_rows_per_block;

/*
 * Hard cap on cooperating SHM streaming workers (the GUC's upper bound and the
 * auto/clamp ceiling). Stays comfortably below the producer's MAX_PARKED_CONNS
 * so every worker's control-socket connection can park alongside the consumer's.
 */
#define PGCH_SHM_MAX_STREAM_WORKERS 64

/*
 * One projected column to stream. `attno` is the 1-based heap attribute number;
 * `wire`/`ch_type`/`name` describe how it appears to ClickHouse. `pg_type` is
 * the source PostgreSQL type OID.
 */
typedef struct ShmOffloadColumn {
    AttrNumber attno;
    char       name[64];
    char       ch_type[64];      /* ClickHouse type string for the schema */
    ShmWireType wire;
    Oid        pg_type;
    int32      scale;            /* decimal scale for Decimal/DateTime64 wire types; 0 otherwise */
} ShmOffloadColumn;

/*
 * Map a PostgreSQL type OID (+ its atttypmod) to the ClickHouse wire type and
 * type string. `typmod` carries precision/scale for parametrized types such as
 * numeric; pass the column's atttypmod (or -1 if unknown). Returns true and
 * fills *out on success; false for any type outside the supported set (the
 * caller must then decline the offload, not error). An unconstrained `numeric`
 * (typmod -1, no fixed precision/scale) is declined so the offload fails closed.
 */
extern bool pgch_pg_type_to_ch_wire(Oid pg_type, int32 typmod, ShmOffloadColumn *out);

/*
 * Build the ShmOffloadColumn projection (wire types + names) for the 1-based
 * heap `attnos` of `rel` into a palloc'd array; returns the column count and
 * sets *out_cols. Raises ERROR if a projected column is no longer SHM-supported.
 */
extern int pgch_build_offload_columns(Relation rel, List *attnos, ShmOffloadColumn **out_cols);

/*
 * Build the comma-separated ClickHouse columns string ("name Type, ...") for a
 * streamed_table() call from the projected column list. Returns a palloc'd
 * string in the current memory context.
 */
extern char *pgch_build_shm_schema_string(const ShmOffloadColumn *cols, int ncols);

/*
 * Scan `rel` under `snapshot`, projecting `cols`, converting snapshot-visible
 * heap rows into SHM blocks of up to `rows_per_block` rows, publishing each via
 * `producer`, and finally publishing end-of-stream. Raises ERROR on conversion
 * failure (e.g. an unexpected NULL in a non-null column).
 *
 * If `out_stats` is non-NULL it is filled with the visibility-path counters
 * (zeroed for the scalar table-AM reader, which does no page-level classify).
 *
 * `bcursor` is the shared cross-process block allocator for the parallel
 * vectorized reader; pass NULL for a single-threaded whole-relation scan. The
 * end-of-stream marker is NOT published here -- the caller signals it (once,
 * after all cooperating workers finish) via shm_producer_signal_eos.
 */
extern uint64 pgch_stream_relation_to_shm(Relation rel, Snapshot snapshot,
                                          const ShmOffloadColumn *cols, int ncols,
                                          ShmProducer *producer, size_t rows_per_block,
                                          ShmBlockCursor *bcursor, PgchVisStats *out_stats);

/*
 * Per-stream columnizer shared by the scalar and vectorized heap readers. A
 * reader calls pgch_columnizer_begin, then pgch_columnizer_add_row once per
 * snapshot-visible row (with `values`/`isnulls` indexed by attno-1, covering at
 * least every projected attno), then pgch_columnizer_finish to flush the
 * trailing block. Output SHM blocks are identical regardless of which reader fed
 * it. End-of-stream is published separately by the caller (shm_producer_signal_eos),
 * once, after all cooperating producers have finished.
 */
typedef struct ShmColumnizer ShmColumnizer;

extern ShmColumnizer *pgch_columnizer_begin(const ShmOffloadColumn *cols, int ncols,
                                            ShmProducer *producer, size_t rows_per_block);
extern void pgch_columnizer_add_row(ShmColumnizer *cz,
                                    const Datum *values, const bool *isnulls);
extern uint64 pgch_columnizer_finish(ShmColumnizer *cz);

/*
 * Column-major (struct-of-arrays) batch fill: fill projected column `col` for
 * rows [dst_row, dst_row+nrows) from per-row source cursors, then call
 * pgch_columnizer_advance(nrows) once after every column is filled. Output is
 * byte-identical to pgch_columnizer_add_row. block_avail reports how many rows
 * fit before the next flush (split larger batches on this boundary).
 */
extern size_t pgch_columnizer_block_avail(const ShmColumnizer *cz);
extern size_t pgch_columnizer_cur_row(const ShmColumnizer *cz);
extern void *pgch_columnizer_fixed_base(ShmColumnizer *cz, int col);
extern void pgch_columnizer_fill_string(ShmColumnizer *cz, int col, size_t dst_row,
                                        char *const *cur, size_t nrows);
extern void pgch_columnizer_advance(ShmColumnizer *cz, size_t nrows);

#endif /* PG_CLICKHOUSE_SHM_OFFLOAD_H */
