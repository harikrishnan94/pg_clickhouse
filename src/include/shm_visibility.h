/*-------------------------------------------------------------------------
 *
 * shm_visibility.h
 *      C/C boundary for the struct-of-arrays heap-tuple visibility classify
 *      engine used by the SHM-offload reader. The kernel + partition live in
 *      the dedicated TU src/shm_visibility.c; the C reader (shm_page_reader.c)
 *      builds a PgchVisDesc once per scan (the snapshot bounds) and calls the
 *      driver per page over already-gathered SoA header fields.
 *
 *      This is the visibility analog of the columnar deform engine
 *      (shm_deform.h), but it is plain C: the classify kernel is monomorphic
 *      (its inputs are always fixed-width integers -- t_infomask plus raw
 *      t_xmin/t_xmax), so it needs no per-type template or constexpr
 *      specialization. Its speed comes from being branchless over unit-stride
 *      SoA inputs (autovectorizable at -O2), not from C++ machinery.
 *
 *      The driver allocates nothing and raises no PostgreSQL error: it is pure
 *      in-memory computation over caller-owned buffers. Everything that can
 *      allocate, error, touch clog, or take a lock (the exact MVCC oracle for
 *      the UNDECIDED residue) stays on the C reader side.
 *
 * Copyright (c) 2025-2026, ClickHouse, Inc.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_CLICKHOUSE_SHM_VISIBILITY_H
#define PG_CLICKHOUSE_SHM_VISIBILITY_H

#include "postgres.h"

#include "access/transam.h"     /* TransactionId */

/*
 * Three-way per-tuple visibility verdict. VISIBLE/INVISIBLE are definitive
 * (proven from hint bits + the two O(1) snapshot-bound comparisons, exactly as
 * HeapTupleSatisfiesMVCC would decide them); UNDECIDED is everything the fast
 * kernel cannot prove and routes to the exact MVCC oracle on the C side.
 */
typedef enum PgchVisVerdict
{
    PGCH_VIS_INVISIBLE = 0,
    PGCH_VIS_VISIBLE = 1,
    PGCH_VIS_UNDECIDED = 2,
} PgchVisVerdict;

/*
 * Per-scan classify descriptor: the only snapshot-derived constants the fast
 * kernel needs. Built once per scan from snapshot->xmin/xmax. The snapshot's
 * in-progress set (xip[]) is deliberately absent -- any xid that would require
 * scanning xip[] is exactly what the kernel reports UNDECIDED and hands to the
 * oracle. POD, mirrors PgchDeformDesc.
 */
typedef struct PgchVisDesc
{
    TransactionId snap_xmin;        /* snapshot->xmin: xid < this is visible */
    TransactionId snap_xmax;        /* snapshot->xmax: xid >= this is in-progress to us */
} PgchVisDesc;

/*
 * Per-stream visibility-path counters, surfaced in the shm_log_stream_stats LOG
 * line so tests can prove which path actually ran (a result match alone never
 * proves the classifier or the slow path was entered). Accumulated by the
 * vectorized page reader; left zeroed by the scalar table-AM reader.
 */
typedef struct PgchVisStats
{
    bool   used_vectorized;     /* true: vectorized page reader ran; false: scalar table-AM fallback */
    uint64 pages_total;         /* heap pages scanned */
    uint64 pages_all_visible;   /* PD_ALL_VISIBLE fast page path (zero per-tuple work) */
    uint64 pages_classified;    /* not-all-visible pages run through the classify path */
    uint64 tuples_gathered;     /* LP_NORMAL tuples classified (on classified pages) */
    uint64 n_visible_fast;      /* definite-VISIBLE verdicts from the kernel */
    uint64 n_invisible_fast;    /* definite-INVISIBLE verdicts from the kernel */
    uint64 n_undecided;         /* UNDECIDED verdicts handed to the MVCC oracle */
    uint64 n_slow_visible;      /* of those, how many the oracle judged visible */

    /*
     * Producer-phase split (benchmark instrumentation; filled only when the
     * shm_log_stream_stats GUC is set, else left zeroed). Index order matches
     * enum PgchPhase in shm_phase.h: [0]=READ [1]=DEFORM [2]=PUBLISH [3]=STALL.
     * By construction these partition the vectorized reader's timed region, so
     * sum(phase_cpu_ns) == the worker's getrusage CPU within clock noise (gate G1).
     */
    uint64 phase_cpu_ns[4];     /* CLOCK_THREAD_CPUTIME_ID ns per phase */
    uint64 phase_wall_ns[4];    /* CLOCK_MONOTONIC ns per phase */
} PgchVisStats;

/*
 * Classify + partition driver. Reads the SoA header fields (infomask/xmin/xmax)
 * of n tuples (n <= MaxHeapTuplesPerPage), writes one PgchVisVerdict per tuple
 * into verdict[], then compacts the VISIBLE and UNDECIDED tuple indices into
 * vis_idx[]/und_idx[] (with counts written to p_nvis and p_nund). INVISIBLE
 * tuples are simply absent from both lists.
 *
 * Allocates nothing, raises no error, takes no lock: pure computation over the
 * caller's buffers. verdict[], vis_idx[], und_idx[] must each hold >= n entries.
 */
extern void pgch_vis_classify(const PgchVisDesc *desc,
                              const uint16 *infomask,
                              const TransactionId *xmin,
                              const TransactionId *xmax,
                              size_t n,
                              uint8 *verdict,
                              uint16 *vis_idx, uint16 *und_idx,
                              uint32 *p_nvis, uint32 *p_nund);

#endif /* PG_CLICKHOUSE_SHM_VISIBILITY_H */
