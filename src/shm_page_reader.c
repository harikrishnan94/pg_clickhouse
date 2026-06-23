/*-------------------------------------------------------------------------
 *
 * shm_page_reader.c
 *      Vectorized, page-at-a-time columnar heap reader for the SHM-offload
 *      producer. See shm_page_reader.h for the contract.
 *
 *      Design:
 *        - Own the seqscan: walk heap blocks 0..nblocks-1, read each with a
 *          BAS_BULKREAD strategy (matching a stock seqscan so the shared-buffer
 *          footprint is identical), share-lock it for the visibility pass, then
 *          deform the visible tuples under the pin only (page-mode contract:
 *          the pin blocks HOT pruning, so live tuple data is stable).
 *        - Visibility: an all-visible page (PD_ALL_VISIBLE, the in-sync shadow
 *          of the visibility map, which is what a page-mode seqscan trusts)
 *          emits every LP_NORMAL tuple with zero per-tuple work. Otherwise a
 *          branch-light kernel classifies each tuple from hint bits + the two
 *          O(1) comparisons against snapshot->xmin/xmax (no clog, no xip scan);
 *          undecided tuples fall back to HeapTupleSatisfiesVisibility (the exact
 *          MVCC oracle), which may set hint bits under the held share lock.
 *        - Deform: walk attributes 1..max_needed_attno, fetching/storing only
 *          the projected ones, then hand the row to the shared ShmColumnizer.
 *
 *      Correctness is gated on MVCC equivalence with table_scan_getnextslot;
 *      every case the kernel cannot prove is routed to the MVCC fallback.
 *
 * Copyright (c) 2025-2026, ClickHouse, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/tupmacs.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/pg_am_d.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/itemid.h"
#include "storage/itemptr.h"
#include "storage/off.h"
#include "storage/predicate.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include "shm_deform.h"
#include "shm_offload.h"
#include "shm_page_reader.h"
#include "shm_producer.h"

/*
 * Per-page visibility splice: visible tuples are partitioned into three buckets
 * as the collector emits them (the header is already hot), so the reader can
 * route each to the deform path optimized for it.
 *   A (cur_simple): NULL-free, full-natts  -> column-major C++ driver (no nulls)
 *   B (complex_tup): HEAP_HASNULL, full-natts -> column-major C++ driver (nulls)
 *   C (fallback_tup): short (natts < max_attno) -> row-major C path
 * With the columnar GUC off, every tuple is routed to C (exact row-major behavior).
 * The three arrays are scan-owned, each sized MaxHeapTuplesPerPage.
 */
typedef struct PgchVisSplit
{
    AttrNumber       max_attno;
    bool             columnar;       /* false -> route everything to fallback (C) */
    char           **cur_simple;     /* A: data-start (htup + t_hoff) */
    HeapTupleHeader *complex_tup;    /* B */
    HeapTupleHeader *fallback_tup;   /* C */
    int              na, nb, nc;
} PgchVisSplit;

static pg_attribute_always_inline void
pgch_route(HeapTupleHeader htup, PgchVisSplit *s)
{
    if (!s->columnar || HeapTupleHeaderGetNatts(htup) < s->max_attno)
        s->fallback_tup[s->nc++] = htup;                       /* group C (or GUC off: all) */
    else if (htup->t_infomask & HEAP_HASNULL)
        s->complex_tup[s->nb++] = htup;                        /* group B */
    else
        s->cur_simple[s->na++] = (char *) htup + htup->t_hoff; /* group A */
}

/* Three-way per-tuple visibility verdict from the branch-light kernel. */
typedef enum PgchVisVerdict
{
    PGCH_VIS_INVISIBLE = 0,
    PGCH_VIS_VISIBLE = 1,
    PGCH_VIS_UNDECIDED = 2,
} PgchVisVerdict;

/*
 * Precomputed, attno-indexed deform metadata (index = attno - 1, for
 * attnos 1..max_attno). Lifts the per-attribute fields out of the TupleDesc so
 * the deform inner loop avoids TupleDescAttr indirection.
 */
typedef struct PgchAttrMeta
{
    int16  attlen;       /* pg_attribute.attlen: >0 fixed, -1 varlena, -2 cstring */
    char   attalign;     /* pg_attribute.attalign: TYPALIGN_{CHAR,SHORT,INT,DOUBLE} */
    bool   attbyval;     /* pg_attribute.attbyval */
    bool   is_needed;    /* projected column? */
} PgchAttrMeta;

/* --------------------------------------------------------------------- */
/* Visibility kernel */
/* --------------------------------------------------------------------- */

/*
 * Classify one tuple from its hint bits and raw xmin/xmax against the snapshot,
 * using ONLY in-memory comparisons (no clog, no xip[] scan, no subtrans). Emits
 * a definitive VISIBLE/INVISIBLE verdict only for the cases the two O(1)
 * snapshot-bound checks resolve exactly as HeapTupleSatisfiesMVCC would; every
 * other case is UNDECIDED and routed to the MVCC fallback. Assumes a normal
 * MVCC snapshot (guaranteed by pgch_vectorized_reader_eligible).
 */
static pg_attribute_always_inline PgchVisVerdict
pgch_classify_tuple(uint16 infomask, TransactionId xmin, TransactionId xmax,
                    TransactionId snap_xmin, TransactionId snap_xmax)
{
    /* ---- xmin side: is the inserter committed-and-visible to us? ---- */
    if (infomask & HEAP_MOVED)
        return PGCH_VIS_UNDECIDED;          /* pre-9.0 upgrade encoding: fall back */

    if ((infomask & HEAP_XMIN_FROZEN) == HEAP_XMIN_FROZEN)
    {
        /* frozen: inserter committed before every snapshot -> xmin visible */
    }
    else if ((infomask & HEAP_XMIN_FROZEN) == HEAP_XMIN_INVALID)
    {
        return PGCH_VIS_INVISIBLE;          /* inserter aborted */
    }
    else if (infomask & HEAP_XMIN_COMMITTED)
    {
        if (TransactionIdPrecedes(xmin, snap_xmin))
        {
            /* committed before the snapshot's xmin -> visible so far */
        }
        else if (TransactionIdFollowsOrEquals(xmin, snap_xmax))
            return PGCH_VIS_INVISIBLE;      /* in progress to us (XidInMVCCSnapshot true) */
        else
            return PGCH_VIS_UNDECIDED;      /* in [xmin,xmax): would scan xip[] -> fall back */
    }
    else
        return PGCH_VIS_UNDECIDED;          /* no xmin hint (current xact / unhinted) */

    /* ---- xmax side: has a committed-and-visible deleter removed it? ---- */
    if (infomask & HEAP_XMAX_INVALID)
        return PGCH_VIS_VISIBLE;            /* no valid xmax */
    if (HEAP_XMAX_IS_LOCKED_ONLY(infomask))
        return PGCH_VIS_VISIBLE;            /* xmax is only a locker, not a delete */
    if (infomask & HEAP_XMAX_IS_MULTI)
        return PGCH_VIS_UNDECIDED;          /* multixact: resolve update xid -> fall back */
    if (infomask & HEAP_XMAX_COMMITTED)
    {
        if (TransactionIdPrecedes(xmax, snap_xmin))
            return PGCH_VIS_INVISIBLE;      /* deleter committed & visible -> deleted */
        else if (TransactionIdFollowsOrEquals(xmax, snap_xmax))
            return PGCH_VIS_VISIBLE;        /* deleter in progress to us -> still visible */
        else
            return PGCH_VIS_UNDECIDED;      /* in [xmin,xmax): scan xip[] -> fall back */
    }
    return PGCH_VIS_UNDECIDED;              /* no xmax hint -> fall back */
}

/* Splice every LP_NORMAL tuple on an all-visible page into the buckets. The
 * header deref here is the same one the deform needs next (hot in cache). */
static void
pgch_collect_all_visible(Page page, PgchVisSplit *s)
{
    OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
    OffsetNumber off;

    for (off = FirstOffsetNumber; off <= maxoff; off = OffsetNumberNext(off))
    {
        ItemId lp = PageGetItemId(page, off);

        if (ItemIdIsNormal(lp))
            pgch_route((HeapTupleHeader) PageGetItem(page, lp), s);
    }
}

/*
 * Splice the snapshot-visible LP_NORMAL tuples on a not-all-visible page into
 * the buckets. Three passes: gather header fields into parallel (SoA) arrays,
 * run the branch-light classifier over them, then route VISIBLE tuples (and
 * MVCC-oracle-confirmed UNDECIDED ones). The buffer must be share-locked for the
 * whole call (the fallback may set hint bits, which dirties the buffer).
 */
static void
pgch_collect_with_visibility(Relation rel, Buffer buf, BlockNumber blk, Page page,
                             Snapshot snapshot, PgchVisSplit *s)
{
    OffsetNumber    maxoff = PageGetMaxOffsetNumber(page);
    OffsetNumber    off;
    TransactionId   snap_xmin = snapshot->xmin;
    TransactionId   snap_xmax = snapshot->xmax;
    int             m = 0;
    int             j;

    /* SoA scratch, sized for the worst case (one page's worth of tuples). */
    OffsetNumber    soa_off[MaxHeapTuplesPerPage];
    HeapTupleHeader soa_htup[MaxHeapTuplesPerPage];
    uint16          soa_im[MaxHeapTuplesPerPage];
    TransactionId   soa_xmin[MaxHeapTuplesPerPage];
    TransactionId   soa_xmax[MaxHeapTuplesPerPage];
    uint8           verdict[MaxHeapTuplesPerPage];

    /* Pass A: gather header pointer + hint bits + raw xmin/xmax per LP_NORMAL. */
    for (off = FirstOffsetNumber; off <= maxoff; off = OffsetNumberNext(off))
    {
        ItemId          lp = PageGetItemId(page, off);
        HeapTupleHeader htup;

        if (!ItemIdIsNormal(lp))
            continue;

        htup = (HeapTupleHeader) PageGetItem(page, lp);
        soa_off[m] = off;
        soa_htup[m] = htup;
        soa_im[m] = htup->t_infomask;
        soa_xmin[m] = HeapTupleHeaderGetRawXmin(htup);
        soa_xmax[m] = HeapTupleHeaderGetRawXmax(htup);
        m++;
    }

    /* Pass B: branch-light classification over the flat arrays. */
    for (j = 0; j < m; j++)
        verdict[j] = (uint8) pgch_classify_tuple(soa_im[j], soa_xmin[j], soa_xmax[j],
                                                 snap_xmin, snap_xmax);

    /* Pass C: route VISIBLE; resolve UNDECIDED via the exact MVCC oracle. */
    for (j = 0; j < m; j++)
    {
        if (verdict[j] == PGCH_VIS_VISIBLE)
        {
            pgch_route(soa_htup[j], s);
        }
        else if (verdict[j] == PGCH_VIS_UNDECIDED)
        {
            ItemId        lp = PageGetItemId(page, soa_off[j]);
            HeapTupleData loctup;

            loctup.t_data = soa_htup[j];
            loctup.t_len = ItemIdGetLength(lp);
            loctup.t_tableOid = RelationGetRelid(rel);
            ItemPointerSet(&loctup.t_self, blk, soa_off[j]);

            if (HeapTupleSatisfiesVisibility(&loctup, snapshot, buf))
                pgch_route(soa_htup[j], s);
        }
        /* PGCH_VIS_INVISIBLE: skip */
    }
}

/* --------------------------------------------------------------------- */
/* Bounded columnar deform */
/* --------------------------------------------------------------------- */

/* A projected attribute in the leading fixed-width run: its byte offset is a
 * per-scan constant (valid only for a tuple with no NULLs), so it is fetched
 * with a direct load and no per-tuple offset walk. */
typedef struct PgchFixedFetch
{
    uint32 off;        /* constant byte offset within the data area */
    int    idx;        /* attno - 1 (index into values[]/isnull[]) */
    int16  attlen;
    bool   attbyval;
} PgchFixedFetch;

/*
 * Per-scan deform plan. The leading run of fixed-width attributes (attnos
 * 1..prefix_len) has constant offsets when the tuple has no NULLs, so projected
 * columns there are direct loads (fixed[]). Anything at/after the first varlena
 * (or after a NULL) needs the offset walk, started at walk_start_off for the
 * fast path or from scratch in the generic path.
 */
typedef struct PgchDeformPlan
{
    int             max_attno;
    PgchAttrMeta   *meta;            /* [0..max_attno-1]: generic walk + fast-path tail */
    PgchFixedFetch *fixed;           /* projected attrs in the fixed prefix */
    int             n_fixed;
    int             prefix_len;      /* length of the leading fixed-width run */
    uint32          walk_start_off;  /* data offset of attr[prefix_len] (no-NULL layout) */
    bool            has_tail;        /* a projected attr lies at/after prefix_len */
} PgchDeformPlan;

/*
 * Generic offset walk over attributes 1..max_attno (mirrors
 * slot_deform_heap_tuple), fetching/storing ONLY projected attributes and
 * stopping at max_attno. `hasnull` is a compile-time-constant template so the
 * all-NOT-NULL instantiation drops the NULL-bitmap work. Used for tuples with
 * NULLs or short (missing-attr) tuples; the fast path handles the common case.
 *
 * For a projected attribute that is NULL or physically absent, isnull[i] is set
 * true and the columnizer raises the fail-closed "NULL not supported" error --
 * byte-for-byte the same diagnostic as the scalar path.
 */
static pg_attribute_always_inline void
pgch_deform_generic(const HeapTupleHeaderData *htup, const PgchAttrMeta *restrict meta,
                    int max_attno, int natts_present,
                    Datum *restrict values, bool *restrict isnull, bool hasnull)
{
    const char  *tp = (const char *) htup + htup->t_hoff;
    const bits8 *bp = htup->t_bits;
    uint32       off = 0;
    int          natts = Min(natts_present, max_attno);
    int          i;

    for (i = 0; i < natts; i++)
    {
        const PgchAttrMeta *a = &meta[i];

        if (hasnull && att_isnull(i, bp))
        {
            /* NULL occupies no data bytes; do not advance the offset. */
            if (a->is_needed)
                isnull[i] = true;
            continue;
        }

        if (a->attlen == -1)
            off = att_align_pointer(off, a->attalign, -1, tp + off);
        else
            off = att_align_nominal(off, a->attalign);

        if (a->is_needed)
        {
            values[i] = fetch_att(tp + off, a->attbyval, a->attlen);
            isnull[i] = false;
        }

        off = att_addlength_pointer(off, a->attlen, tp + off);
    }

    for (i = natts; i < max_attno; i++)
        if (meta[i].is_needed)
            isnull[i] = true;
}

/*
 * Fast path for a tuple with no NULLs and all projected attrs present: direct
 * loads of the fixed-prefix columns (no per-tuple offset arithmetic for the
 * non-projected prefix attrs), then a short walk only across the tail
 * (varlena-and-after region) up to max_attno.
 */
static pg_attribute_always_inline void
pgch_deform_fast(const HeapTupleHeaderData *htup, const PgchDeformPlan *restrict plan,
                 Datum *restrict values, bool *restrict isnull)
{
    const char *tp = (const char *) htup + htup->t_hoff;
    int         k;

    for (k = 0; k < plan->n_fixed; k++)
    {
        const PgchFixedFetch *f = &plan->fixed[k];

        values[f->idx] = fetch_att(tp + f->off, f->attbyval, f->attlen);
        isnull[f->idx] = false;
    }

    if (plan->has_tail)
    {
        const PgchAttrMeta *meta = plan->meta;
        uint32              off = plan->walk_start_off;
        int                 i;

        for (i = plan->prefix_len; i < plan->max_attno; i++)
        {
            const PgchAttrMeta *a = &meta[i];

            if (a->attlen == -1)
                off = att_align_pointer(off, a->attalign, -1, tp + off);
            else
                off = att_align_nominal(off, a->attalign);

            if (a->is_needed)
            {
                values[i] = fetch_att(tp + off, a->attbyval, a->attlen);
                isnull[i] = false;
            }

            off = att_addlength_pointer(off, a->attlen, tp + off);
        }
    }
}

static void
pgch_deform_needed(const HeapTupleHeaderData *htup, const PgchDeformPlan *plan,
                   Datum *values, bool *isnull)
{
    int natts_present = HeapTupleHeaderGetNatts(htup);

    if (!(htup->t_infomask & HEAP_HASNULL) && natts_present >= plan->max_attno)
        pgch_deform_fast(htup, plan, values, isnull);
    else
        pgch_deform_generic(htup, plan->meta, plan->max_attno, natts_present,
                            values, isnull, (htup->t_infomask & HEAP_HASNULL) != 0);
}

/* --------------------------------------------------------------------- */
/* Eligibility + the page reader */
/* --------------------------------------------------------------------- */

bool
pgch_vectorized_reader_eligible(Relation rel, Snapshot snapshot,
                                const ShmOffloadColumn *cols, int ncols)
{
    TupleDesc tupdesc;
    int       c;

    /* The visibility kernel assumes a normal, non-recovery MVCC snapshot. */
    if (!IsMVCCSnapshot(snapshot) || snapshot->takenDuringRecovery)
        return false;

    /* SSI predicate-locking and catalog/decoding contexts are not reproduced
     * here; let the blessed table-AM scan handle them. */
    if (IsolationIsSerializable())
        return false;
    if (TransactionIdIsValid(CheckXidAlive))
        return false;
    if (CheckForSerializableConflictOutNeeded(rel, snapshot))
        return false;

    /* The reader speaks heap page format only. */
    if (rel->rd_rel->relam != HEAP_TABLE_AM_OID)
        return false;

    tupdesc = RelationGetDescr(rel);
    for (c = 0; c < ncols; c++)
    {
        ShmWireType        w = cols[c].wire;
        Form_pg_attribute  att;

        /* Decimals stay on the scalar producer (separate Decimal-over-SHM
         * blocker); the deform is otherwise byte-identical. */
        if (w == SHM_WIRE_DECIMAL32 || w == SHM_WIRE_DECIMAL64 || w == SHM_WIRE_DECIMAL128)
            return false;

        /* A fast-default (ALTER TABLE ADD COLUMN ... DEFAULT) leaves older
         * tuples physically short; the value comes from attmissingval, which
         * the bounded deform does not synthesize. Decline so the scalar path
         * (slot_getallattrs -> getmissingattr) handles it. */
        att = TupleDescAttr(tupdesc, cols[c].attno - 1);
        if (att->atthasmissing)
            return false;
    }

    return true;
}

/*
 * C string-fill callback handed to the C++ deform driver. String columns can't
 * be filled in the (allocation-free) C++ kernels because StringInfo append may
 * repalloc; the driver positions the per-row cursors at the varlena datum and
 * calls back here. Signature matches PgchStringFill.
 */
static void
pgch_str_fill_cb(void *cz, int col_index, size_t dst_row, char *const *cur, size_t nrows)
{
    pgch_columnizer_fill_string((ShmColumnizer *) cz, col_index, dst_row, cur, nrows);
}

/*
 * Build the per-column deform descriptor for the C++ driver, once per scan.
 * `col` is sized max_attno (indexed by attno-1). Computes the group-A prefix
 * (leading fixed-width run) and the group-B prefix (leading fixed-width AND
 * NOT NULL run) and each prefix column's constant byte offset.
 */
static void
pgch_build_deform_desc(Relation rel, ShmColumnizer *cz, const ShmOffloadColumn *cols,
                       const int *col_of, AttrNumber max_attno,
                       PgchDeformCol *col, PgchDeformDesc *desc)
{
    TupleDesc tupdesc = RelationGetDescr(rel);
    uint32    cum = 0;
    int       pa = 0;
    int       pb;
    int       i;

    for (i = 0; i < max_attno; i++)
    {
        Form_pg_attribute att = TupleDescAttr(tupdesc, i);
        int               cix = col_of[i];

        col[i].attlen = att->attlen;
        col[i].attalign = att->attalign;
        col[i].nullable = !att->attnotnull;
        col[i].is_needed = (cix >= 0);
        col[i].disp = 0;
        if (cix >= 0)
        {
            col[i].wire = cols[cix].wire;
            col[i].is_string = (cols[cix].wire == SHM_WIRE_STRING);
            col[i].col_index = cix;
            col[i].dst_base = col[i].is_string ? NULL : pgch_columnizer_fixed_base(cz, cix);
        }
        else
        {
            col[i].wire = SHM_WIRE_STRING;     /* unused */
            col[i].is_string = false;
            col[i].col_index = -1;
            col[i].dst_base = NULL;
        }
    }

    /* Group-A prefix: leading run of fixed-width columns, constant offsets. */
    for (i = 0; i < max_attno; i++)
    {
        if (col[i].attlen <= 0)            /* first varlena/cstring ends the run */
            break;
        cum = (uint32) att_align_nominal(cum, col[i].attalign);
        col[i].disp = cum;
        cum += (uint32) col[i].attlen;
        pa = i + 1;
    }
    desc->col = col;
    desc->max_attno = max_attno;
    desc->prefix_len_a = pa;
    desc->walk_start_off_a = cum;

    /* Group-B prefix: leading run of fixed-width AND NOT NULL columns (<= A). */
    for (pb = 0; pb < pa && !col[pb].nullable; pb++)
        /* advance */ ;
    desc->prefix_len_b = pb;
    desc->walk_start_off_b = (pb < pa) ? col[pb].disp : desc->walk_start_off_a;
}

uint64
pgch_stream_relation_vectorized(Relation rel, Snapshot snapshot,
                                const ShmOffloadColumn *cols, int ncols,
                                ShmProducer *producer, size_t rows_per_block)
{
    ShmColumnizer       *cz;
    TupleDesc            tupdesc = RelationGetDescr(rel);
    BufferAccessStrategy strategy;
    BlockNumber          nblocks;
    BlockNumber          blk;
    AttrNumber           max_attno = 0;
    PgchAttrMeta        *meta;
    PgchDeformPlan       plan;          /* group C / GUC-off row-major path */
    PgchDeformCol       *col;           /* group A/B C++ driver descriptor */
    PgchDeformDesc       desc;
    uint32               cum;
    Datum               *values;
    bool                *isnull;
    int                 *col_of;        /* attno-1 -> projected column index, else -1 */
    char               **cur_simple;    /* group A cursors; reused as group B scratch */
    HeapTupleHeader     *complex_tup;   /* group B tuples (HEAP_HASNULL, full natts) */
    HeapTupleHeader     *fallback_tup;  /* group C tuples (short) / all when GUC off */
    const bits8        **bits;          /* group B NULL-bitmap pointers */
    PgchVisSplit         s;
    int                  c;
    int                  i;

    for (c = 0; c < ncols; c++)
        if (cols[c].attno > max_attno)
            max_attno = cols[c].attno;

    /* Build attno-indexed deform metadata for the row-major path (group C / off). */
    meta = (PgchAttrMeta *) palloc0(sizeof(PgchAttrMeta) * max_attno);
    for (i = 0; i < max_attno; i++)
    {
        Form_pg_attribute att = TupleDescAttr(tupdesc, i);

        meta[i].attlen = att->attlen;
        meta[i].attalign = att->attalign;
        meta[i].attbyval = att->attbyval;
        meta[i].is_needed = false;
    }
    for (c = 0; c < ncols; c++)
        meta[cols[c].attno - 1].is_needed = true;

    /* Row-major plan: constant-offset fixed prefix + tail walk (pgch_deform_fast). */
    plan.max_attno = max_attno;
    plan.meta = meta;
    plan.fixed = (PgchFixedFetch *) palloc(sizeof(PgchFixedFetch) * ncols);
    plan.n_fixed = 0;
    cum = 0;
    plan.prefix_len = 0;
    for (i = 0; i < max_attno; i++)
    {
        if (meta[i].attlen <= 0)        /* first varlena/cstring ends the run */
            break;
        cum = (uint32) att_align_nominal(cum, meta[i].attalign);
        if (meta[i].is_needed)
        {
            PgchFixedFetch *f = &plan.fixed[plan.n_fixed++];

            f->off = cum;
            f->idx = i;
            f->attlen = meta[i].attlen;
            f->attbyval = meta[i].attbyval;
        }
        cum += (uint32) meta[i].attlen;
        plan.prefix_len++;
    }
    plan.walk_start_off = cum;
    plan.has_tail = (max_attno > plan.prefix_len);

    /* attno-1 -> projected column index. */
    col_of = (int *) palloc(sizeof(int) * max_attno);
    for (i = 0; i < max_attno; i++)
        col_of[i] = -1;
    for (c = 0; c < ncols; c++)
        col_of[cols[c].attno - 1] = c;

    values = (Datum *) palloc(sizeof(Datum) * max_attno);
    isnull = (bool *) palloc(sizeof(bool) * max_attno);
    col = (PgchDeformCol *) palloc0(sizeof(PgchDeformCol) * max_attno);
    cur_simple = (char **) palloc(sizeof(char *) * MaxHeapTuplesPerPage);
    complex_tup = (HeapTupleHeader *) palloc(sizeof(HeapTupleHeader) * MaxHeapTuplesPerPage);
    fallback_tup = (HeapTupleHeader *) palloc(sizeof(HeapTupleHeader) * MaxHeapTuplesPerPage);
    bits = (const bits8 **) palloc(sizeof(bits8 *) * MaxHeapTuplesPerPage);

    cz = pgch_columnizer_begin(cols, ncols, producer, rows_per_block);

    /* C++ driver descriptor (needs cz for the per-column output buffers). */
    pgch_build_deform_desc(rel, cz, cols, col_of, max_attno, col, &desc);

    s.max_attno = max_attno;
    s.columnar = pgch_use_columnar_deform;
    s.cur_simple = cur_simple;
    s.complex_tup = complex_tup;
    s.fallback_tup = fallback_tup;

    /* Match a stock seqscan: BAS_BULKREAD keeps a large scan from evicting the
     * shared-buffer working set. */
    strategy = GetAccessStrategy(BAS_BULKREAD);
    nblocks = RelationGetNumberOfBlocks(rel);

    for (blk = 0; blk < nblocks; blk++)
    {
        Buffer buf;
        Page   page;
        int    done;
        int    j;
        int    r;

        CHECK_FOR_INTERRUPTS();

        buf = ReadBufferExtended(rel, MAIN_FORKNUM, blk, RBM_NORMAL, strategy);
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buf);

        /* Splice visible tuples into the A/B/C buckets (under the share lock). */
        s.na = s.nb = s.nc = 0;
        if (PageIsAllVisible(page) && !snapshot->takenDuringRecovery)
            pgch_collect_all_visible(page, &s);
        else
            pgch_collect_with_visibility(rel, buf, blk, page, snapshot, &s);

        /* Visibility decisions (and any hint-bit writes by the fallback) are
         * done; drop the lock and deform the visible tuples under the pin. */
        LockBuffer(buf, BUFFER_LOCK_UNLOCK);

        /* Group A: NULL-free tuples, column-major C++ driver. (Emitting A then
         * B then C reorders within the page, which is fine: the offload feeds
         * order-independent aggregates; the oracle compares results.) */
        for (done = 0; done < s.na; )
        {
            size_t dst = pgch_columnizer_cur_row(cz);
            size_t avail = pgch_columnizer_block_avail(cz);
            int    navail = (int) Min((size_t) (s.na - done), avail);

            pgch_columnar_deform_simple(&desc, cur_simple + done, (size_t) navail,
                                        dst, cz, pgch_str_fill_cb);
            pgch_columnizer_advance(cz, (size_t) navail);
            done += navail;
        }

        /* Group B: tuples with NULLs, NULL-aware column-major C++ driver. cur_simple
         * is reused as the per-sub-batch cursor scratch (group A is done). */
        for (done = 0; done < s.nb; )
        {
            size_t dst = pgch_columnizer_cur_row(cz);
            size_t avail = pgch_columnizer_block_avail(cz);
            int    navail = (int) Min((size_t) (s.nb - done), avail);

            for (r = 0; r < navail; r++)
            {
                HeapTupleHeader h = complex_tup[done + r];

                cur_simple[r] = (char *) h + h->t_hoff;
                bits[r] = (const bits8 *) ((char *) h + SizeofHeapTupleHeader);
            }
            pgch_columnar_deform_nullable(&desc, cur_simple, bits, (size_t) navail,
                                          dst, cz, pgch_str_fill_cb);
            pgch_columnizer_advance(cz, (size_t) navail);
            done += navail;
        }

        /* Group C: short/missing-attr tuples (and everything when the GUC is
         * off) via the proven row-major path. */
        for (j = 0; j < s.nc; j++)
        {
            pgch_deform_needed((const HeapTupleHeaderData *) fallback_tup[j], &plan,
                               values, isnull);
            pgch_columnizer_add_row(cz, values, isnull);
        }

        ReleaseBuffer(buf);
    }

    FreeAccessStrategy(strategy);
    pfree(bits);
    pfree(fallback_tup);
    pfree(complex_tup);
    pfree(cur_simple);
    pfree(col);
    pfree(isnull);
    pfree(values);
    pfree(col_of);
    pfree(plan.fixed);
    pfree(meta);

    return pgch_columnizer_finish(cz);
}
