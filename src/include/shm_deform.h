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
    bool        is_decimal;   /* wire in {DECIMAL32,DECIMAL64,DECIMAL128}: a varlena
                               * (heap numeric) whose wire form is fixed-width */
    ShmWireType wire;         /* fixed-width fill dispatch */
    int         col_index;    /* columnizer column index (fill / str_fill); -1 if !is_needed */
    void       *dst_base;     /* ColBuf.fixed for fixed (incl. decimal) projected cols; NULL for strings */
    uint32      disp;         /* constant no-NULL-layout byte offset (prefix cols) */
    uint8       dec_scale;    /* decimal column scale S (digits after the point) */
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
 * Compiled deform plan (the "step program"). Built once per scan by
 * pgch_build_deform_plans from a PgchDeformDesc: it lowers the per-column walk +
 * fill into an ordered list of specialized steps, each a pre-instantiated kernel
 * (selected by attlen/attalign/wire) plus baked args. Runs of fixed columns with
 * statically-known offsets collapse into a constant `lead`/`disp`; only varlena
 * (and post-varlena / nullable) columns cost per-row cursor work. The hot path
 * (pgch_columnar_deform_run) just invokes each step over the whole sub-batch, so
 * the per-row inner loops are monomorphic with constants folded in.
 */

/* One cursor advance inside a WALK step. */
typedef struct PgchHop
{
    int16  attlen;     /* -1 varlena, >0 fixed-width */
    uint8  align;      /* numeric alignment: 1/2/4/8 */
    int16  null_bit;   /* group B: 0-based attno for att_isnull; -1 = always present */
} PgchHop;

typedef struct PgchStep PgchStep;

/*
 * A step kernel: processes the whole sub-batch (n rows). For FILL steps it reads
 * each row's value and writes the columnizer staging buffer; for WALK steps it
 * advances cur[] past skipped/nullable columns. `bits` is NULL for group A.
 */
typedef void (*PgchStepFn) (const PgchStep *st, char **cur, const bits8 **bits,
                            size_t n, size_t dst_row, void *cz, PgchStringFill str_fill);

typedef enum PgchStepKind
{
    PGCH_STEP_WALK = 1,
    PGCH_STEP_FILL_CONST,
    PGCH_STEP_FILL_WALK,
    PGCH_STEP_FILL_STRING,
    PGCH_STEP_FILL_DECIMAL      /* varlena-positioned, fixed-width filled (numeric) */
} PgchStepKind;

struct PgchStep
{
    PgchStepFn      run;        /* the specialized kernel (chosen at build time) */
    uint32          disp;       /* FILL_CONST: byte offset from data start; WALK: const lead */
    void           *dst_base;   /* fixed-fill target (ColBuf.fixed) */
    int             col_index;  /* columnizer column (string fill) */
    uint8           kind;       /* PgchStepKind, for diagnostics / test harness dumps */
    uint8           align;      /* string/decimal-fill alignment, or WALK final fixed-run alignment */
    uint8           dec_scale;  /* FILL_DECIMAL: column scale S */
    uint8           dec_width;  /* FILL_DECIMAL: wire width in bytes (4/8/16) */
    const PgchHop  *hops;       /* WALK: hop list (into the caller's hop buffer) */
    int             nhop;
};

typedef struct PgchDeformPlan
{
    const PgchStep *step;
    int             n_step;
} PgchDeformPlan;

/*
 * Build the group-A and group-B step plans from `desc`, once per scan. The
 * caller supplies the step/hop storage (scan-lived): each buffer must hold at
 * least (2*max_attno + 2) steps and max_attno hops. POD-only; allocates nothing.
 */
extern void pgch_build_deform_plans(const PgchDeformDesc *desc,
                                    PgchStep *step_a, PgchHop *hop_a, PgchDeformPlan *plan_a,
                                    PgchStep *step_b, PgchHop *hop_b, PgchDeformPlan *plan_b);

/*
 * Run a compiled plan over a sub-batch of `n` tuples. `cur[r]` is the tuple data
 * start (htup + t_hoff); the kernels mutate it in place. For the group-A plan
 * `bits` is NULL; for the group-B plan `bits[r]` is row r's NULL-bitmap base
 * (htup + SizeofHeapTupleHeader). Fills rows [dst_row, dst_row+n) of every
 * projected column; caller guarantees dst_row + n <= rows_per_block.
 */
extern void pgch_columnar_deform_run(const PgchDeformPlan *plan,
                                     char **cur, const bits8 **bits,
                                     size_t n, size_t dst_row,
                                     void *cz, PgchStringFill str_fill);

/*
 * Decimal-fill fault channel (C side, in shm_offload.c). The allocation-free C++
 * FILL_DECIMAL kernel cannot raise or detoast, so on a value it cannot convert
 * inline (a stored NaN/Inf, a compressed/external numeric, or an out-of-range
 * magnitude) it records the offending value via pgch_columnizer_note_dec_fault and
 * writes a zero placeholder. After each sub-batch (before the columnizer advances,
 * so faults are resolved before the block is published) the reader calls
 * pgch_columnizer_resolve_dec_faults, which detoasts + converts toasted values into
 * their slot and ereports the clean diagnostic for NaN/Inf / overflow. `status` is
 * a PgchDecConv value.
 */
extern void pgch_columnizer_note_dec_fault(void *cz, char *dst, const char *valptr,
                                           uint32 scale, uint32 width, int col_index,
                                           int status);
extern void pgch_columnizer_resolve_dec_faults(void *cz);

#ifdef __cplusplus
}
#endif

#endif /* PG_CLICKHOUSE_SHM_DEFORM_H */
