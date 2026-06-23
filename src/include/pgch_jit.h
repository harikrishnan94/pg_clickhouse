/*-------------------------------------------------------------------------
 *
 * pgch_jit.h
 *      C ABI boundary for the OPTIONAL LLVM-JIT deform module
 *      (pg_clickhouse_jit.so). The base extension does not link LLVM; it loads
 *      this module lazily on the first eligible scan via
 *      load_external_function("$libdir/pg_clickhouse_jit", "pgch_jit_get", ...)
 *      and falls back to the AOT step-plan driver (pgch_columnar_deform_run)
 *      whenever the module is absent or declines a plan.
 *
 *      The module JIT-compiles a *fused* row-at-a-time deform specialized to a
 *      scan's PgchDeformDesc (Strategy R from the experiment): the cursor lives
 *      in a register across the whole row, runs of fixed skips are folded to a
 *      constant offset, and group-B nullable hops use baked null-bit constants
 *      and a branchless select. Only fixed-width projected columns are
 *      supported (string fills stay on the AOT path); the module returns NULL
 *      for any plan it cannot handle, so the caller always has a safe fallback.
 *
 * Copyright (c) 2025-2026, ClickHouse, Inc.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_CLICKHOUSE_PGCH_JIT_H
#define PG_CLICKHOUSE_PGCH_JIT_H

#include "postgres.h"

#include "shm_deform.h"     /* PgchDeformDesc */

#ifdef __cplusplus
extern "C"
{
#endif

/*
 * Compiled fused-deform entry point. Mirrors pgch_columnar_deform_run's contract
 * minus the columnizer/string callback (fixed-only): `cur[r]` is row r's tuple
 * data start (mutated freely; the fused loop keeps its own register cursor),
 * `bits[r]` is row r's NULL-bitmap base (NULL for group A), and `dst_bases[k]`
 * is the columnizer fixed buffer for projected column index k (== PgchDeformCol
 * .col_index). Fills rows [dst_row, dst_row+n) of every projected fixed column.
 */
typedef void (*PgchJitDeform) (char **cur, const bits8 **bits, size_t n,
                               size_t dst_row, void *const *dst_bases);

/*
 * Resolve (compile or cache-hit) a fused deform for `desc`/`group_b`. Returns
 * NULL if the plan is ineligible (any projected column is not a JIT-emittable
 * fixed wire), if compilation fails, or if the per-process cache is full. The
 * returned function pointer is owned by the module's process-wide cache and is
 * valid for the lifetime of the backend. Implemented in pg_clickhouse_jit.so;
 * the type below is what the base extension dlsym's.
 */
typedef PgchJitDeform (*PgchJitGetFn) (const PgchDeformDesc *desc, bool group_b);
extern PGDLLEXPORT PgchJitDeform pgch_jit_get(const PgchDeformDesc *desc, bool group_b);

#ifdef __cplusplus
}
#endif

#endif /* PG_CLICKHOUSE_PGCH_JIT_H */
