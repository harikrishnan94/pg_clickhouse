/*-------------------------------------------------------------------------
 *
 * shm_visibility.c
 *      Struct-of-arrays heap-tuple visibility classify engine for the
 *      SHM-offload reader. See shm_visibility.h for the contract.
 *
 *      The driver pgch_vis_classify is the visibility analog of the columnar
 *      deform engine, but plain C: every input is a fixed-width integer
 *      (t_infomask, raw t_xmin/t_xmax), so the kernel is monomorphic and needs
 *      no per-type template. Two passes:
 *
 *        Pass 1 (vectorizable): a branch-free per-tuple classify. Each MVCC
 *          decision is reduced to a boolean predicate combined with bitwise
 *          ops (no short-circuit control flow), so the loop has no
 *          data-dependent branches and reads unit-stride SoA arrays -- the
 *          compiler autovectorizes it at -O2. It is a literal translation of
 *          the scalar reference pgch_classify_tuple (kept in shm_page_reader.c
 *          and cross-checked under assertions), so the visible set stays
 *          byte-for-byte identical to HeapTupleSatisfiesMVCC.
 *
 *        Pass 2 (branch-light): a stream compaction of verdict[] into the
 *          VISIBLE and UNDECIDED index lists, using the conditional-increment
 *          idiom (no branches). This makes the C reader's route step two tight
 *          loops and isolates the rare UNDECIDED residue for the slow path.
 *
 *      Allocation-free and error-free by construction: anything that can
 *      allocate, error, touch clog, or take a lock stays in the C reader.
 *
 * Copyright (c) 2025-2026, ClickHouse, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"    /* HEAP_* infomask bits, HEAP_XMAX_IS_LOCKED_ONLY */

#include "shm_visibility.h"

/*
 * Wraparound-safe transaction-id order, written branch-free so it vectorizes.
 *
 * These are TransactionIdPrecedes / TransactionIdFollowsOrEquals specialized to
 * normal xids (the signed-difference test, dropping the special-xid branch).
 * That specialization is exact wherever the result feeds a verdict: a tuple
 * whose comparison is consulted has HEAP_XMIN_COMMITTED (so a real committed,
 * hence normal, xmin) or HEAP_XMAX_COMMITTED (normal xmax), and snapshot
 * xmin/xmax are normal; a frozen/aborted/no-hint tuple takes a hint-bit branch
 * and its comparison is masked out of the verdict. The assertion cross-check in
 * shm_page_reader.c (against the macro-based scalar reference) makes this exact
 * equivalence checkable on every classified tuple in cassert builds.
 */
static inline int
t_before(TransactionId a, TransactionId b)
{
    return (int32) (a - b) < 0;
}

static inline int
t_afteq(TransactionId a, TransactionId b)
{
    return (int32) (a - b) >= 0;
}

void
pgch_vis_classify(const PgchVisDesc *desc,
                  const uint16 *restrict infomask,
                  const TransactionId *restrict xmin,
                  const TransactionId *restrict xmax,
                  size_t n,
                  uint8 *restrict verdict,
                  uint16 *restrict vis_idx, uint16 *restrict und_idx,
                  uint32 *p_nvis, uint32 *p_nund)
{
    TransactionId sxmin = desc->snap_xmin;
    TransactionId sxmax = desc->snap_xmax;
    uint32        nvis = 0;
    uint32        nund = 0;
    size_t        r;

    /* Pass 1: branch-free classify -> verdict[]. Autovectorizes at -O2. */
    for (r = 0; r < n; ++r)
    {
        uint16        im = infomask[r];
        TransactionId xn = xmin[r];
        TransactionId xx = xmax[r];
        uint16        xb = im & HEAP_XMIN_FROZEN;     /* {0, COMMITTED, INVALID, FROZEN} */

        /* ---- xmin side: is the inserter committed-and-visible to us? ---- */
        int notmoved = (im & HEAP_MOVED) == 0;        /* pre-9.0 encoding -> UNDECIDED */
        int frozen   = (xb == HEAP_XMIN_FROZEN);      /* committed before every snapshot */
        int xn_abort = (xb == HEAP_XMIN_INVALID);     /* inserter aborted */
        int xn_comm  = (xb == HEAP_XMIN_COMMITTED);   /* inserter committed (normal xid) */

        int xmin_vis = notmoved & (frozen | (xn_comm & t_before(xn, sxmin)));
        int xmin_inv = notmoved & (xn_abort | (xn_comm & t_afteq(xn, sxmax)));
        int xmin_und = !(xmin_vis | xmin_inv);        /* moved | no-hint | in [xmin,xmax) */

        /* ---- xmax side: has a committed-and-visible deleter removed it? ---- */
        int xx_inval  = (im & HEAP_XMAX_INVALID) != 0;
        int xx_lockon = HEAP_XMAX_IS_LOCKED_ONLY(im); /* locker, not a delete (wins over multi) */
        int xx_multi  = (im & HEAP_XMAX_IS_MULTI) != 0;
        int xx_comm   = (im & HEAP_XMAX_COMMITTED) != 0;
        int xx_strong = xx_inval | xx_lockon;         /* visible regardless of the xmax value */
        int xx_commE  = xx_comm & (!xx_strong) & (!xx_multi);  /* an effective committed deleter */
        int xx_range  = xx_commE & (!t_before(xx, sxmin)) & (!t_afteq(xx, sxmax)); /* xip[] scan */

        /* deleter committed & visible -> INVISIBLE; multixact / no hint / in-range
         * -> UNDECIDED; everything else (no deleter, locker-only, or deleter
         * in-progress to us) -> VISIBLE (the implicit else). */
        int xmax_inv = xx_commE & t_before(xx, sxmin);
        int xmax_und = (!xx_strong) & (xx_multi | (!xx_comm) | xx_range);

        /* ---- combine, preserving the scalar priority order ---- */
        uint8 xmax_code = xmax_und ? PGCH_VIS_UNDECIDED
                                   : (xmax_inv ? PGCH_VIS_INVISIBLE : PGCH_VIS_VISIBLE);

        verdict[r] = xmin_und ? PGCH_VIS_UNDECIDED
                              : (xmin_inv ? PGCH_VIS_INVISIBLE : xmax_code);
    }

    /* Pass 2: branch-light compaction of verdict[] -> index worklists. */
    for (r = 0; r < n; ++r)
    {
        uint8 v = verdict[r];

        vis_idx[nvis] = (uint16) r;
        nvis += (v == PGCH_VIS_VISIBLE);
        und_idx[nund] = (uint16) r;
        nund += (v == PGCH_VIS_UNDECIDED);
    }

    *p_nvis = nvis;
    *p_nund = nund;
}
