/*-------------------------------------------------------------------------
 *
 * shm_deform.h
 *      C/C++ boundary for the templated, column-major (struct-of-arrays) heap
 *      deform engine used by the SHM-offload reader. The kernels + driver live
 *      in the C++ TU src/shm_deform.cpp; the C reader (shm_page_reader.c) builds
 *      a PgchDeformDesc once per scan and calls the extern "C" driver per
 *      sub-batch.
 *
 *      The driver allocates nothing and never raises a PostgreSQL error: all
 *      memory growth (string accumulation) and error reporting stay on the C
 *      side. Fixed-width column values are written directly into the columnizer's
 *      per-column staging buffers (dst_base); string columns are handed back to C
 *      via the PgchStringFill callback.
 *
 * Copyright (c) 2025-2026, ClickHouse, Inc.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_CLICKHOUSE_SHM_DEFORM_H
#define PG_CLICKHOUSE_SHM_DEFORM_H

#include "postgres.h"

#include "utils/datetime.h"     /* POSTGRES_EPOCH_JDATE, UNIX_EPOCH_JDATE */

#include "shm_producer.h"       /* ShmWireType */

#ifdef __cplusplus
extern "C"
{
#endif

/* PostgreSQL DATE epoch is 2000-01-01; ClickHouse Date epoch is 1970-01-01.
 * Shared by the C columnizer (write_fixed_value) and the C++ DATE kernel. */
#define PGCH_DATE_EPOCH_DIFF (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) /* 10957 */

/*
 * One heap attribute's deform descriptor, indexed by attno-1 over 0..max_attno-1.
 * POD, built once per scan by the C reader from the relation's TupleDesc + the
 * projection. `disp` is the attribute's constant byte offset within the tuple
 * data area assuming the tuple has no NULLs (valid for the fixed-width prefix;
 * unused for tail columns, which are reached by the per-row cursor walk).
 */
typedef struct PgchDeformCol
{
    int16       attlen;       /* >0 fixed-width, -1 varlena */
    char        attalign;     /* TYPALIGN_{CHAR,SHORT,INT,DOUBLE} */
    bool        nullable;     /* !attnotnull (a non-projected col may be NULL) */
    bool        is_needed;    /* projected column? */
    bool        is_string;    /* wire == SHM_WIRE_STRING */
    ShmWireType wire;         /* fixed-width fill dispatch */
    int         col_index;    /* columnizer column index (fill / str_fill); -1 if !is_needed */
    void       *dst_base;     /* ColBuf.fixed for fixed projected cols; NULL otherwise */
    uint32      disp;         /* constant no-NULL-layout byte offset (prefix cols) */
} PgchDeformCol;

/*
 * Per-scan deform plan handed to the driver. The fixed-width prefix differs by
 * group: group A (no NULLs) uses the full leading fixed-width run; group B
 * (NULLs present) uses the leading run of fixed-width AND NOT NULL columns
 * (<= group A's), since a NULL in any earlier column shifts later offsets.
 */
typedef struct PgchDeformDesc
{
    const PgchDeformCol *col;          /* [0 .. max_attno-1] */
    int                  max_attno;
    int                  prefix_len_a; /* group A: leading fixed-width run length */
    uint32               walk_start_off_a;
    int                  prefix_len_b; /* group B: leading fixed-width AND NOT NULL run */
    uint32               walk_start_off_b;
} PgchDeformDesc;

/*
 * String columns are filled back on the C side (StringInfo append allocates).
 * `cur[r]` is positioned at row r's varlena datum; `cz` is the opaque
 * ShmColumnizer*. Implemented by a thin C wrapper over pgch_columnizer_fill_string.
 */
typedef void (*PgchStringFill) (void *cz, int col_index, size_t dst_row,
                                char *const *cur, size_t nrows);

/*
 * Group A: a sub-batch of `n` NULL-free, full-natts tuples. `cur[r]` is the
 * tuple data start (htup + t_hoff); the driver mutates it in place (tail walk).
 * Fills rows [dst_row, dst_row+n) of every projected column. Caller guarantees
 * dst_row + n <= rows_per_block.
 */
extern void pgch_columnar_deform_simple(const PgchDeformDesc *desc,
                                        char **cur, size_t n, size_t dst_row,
                                        void *cz, PgchStringFill str_fill);

/*
 * Group B: a sub-batch of `n` tuples that have NULLs (in non-projected columns)
 * but full natts. `cur[r]` = data start, `bits[r]` = the NULL bitmap base
 * (htup + SizeofHeapTupleHeader). Projected columns are NOT NULL (never null);
 * only nullable non-projected columns take a per-row att_isnull check.
 */
extern void pgch_columnar_deform_nullable(const PgchDeformDesc *desc,
                                          char **cur, const bits8 **bits,
                                          size_t n, size_t dst_row,
                                          void *cz, PgchStringFill str_fill);

#ifdef __cplusplus
}
#endif

#endif /* PG_CLICKHOUSE_SHM_DEFORM_H */
