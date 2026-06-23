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
#include "utils/relcache.h"

#include "shm_producer.h"

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
extern bool  pgch_log_stream_stats;

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
 */
extern uint64 pgch_stream_relation_to_shm(Relation rel, Snapshot snapshot,
                                          const ShmOffloadColumn *cols, int ncols,
                                          ShmProducer *producer, size_t rows_per_block);

/*
 * Per-stream columnizer shared by the scalar and vectorized heap readers. A
 * reader calls pgch_columnizer_begin, then pgch_columnizer_add_row once per
 * snapshot-visible row (with `values`/`isnulls` indexed by attno-1, covering at
 * least every projected attno), then pgch_columnizer_finish to flush the
 * trailing block and signal end-of-stream. Output SHM blocks are identical
 * regardless of which reader fed it.
 */
typedef struct ShmColumnizer ShmColumnizer;

extern ShmColumnizer *pgch_columnizer_begin(const ShmOffloadColumn *cols, int ncols,
                                            ShmProducer *producer, size_t rows_per_block);
extern void pgch_columnizer_add_row(ShmColumnizer *cz,
                                    const Datum *values, const bool *isnulls);
extern uint64 pgch_columnizer_finish(ShmColumnizer *cz);

#endif /* PG_CLICKHOUSE_SHM_OFFLOAD_H */
