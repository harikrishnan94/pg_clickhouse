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

#include "shm_offload.h"
#include "shm_page_reader.h"
#include "shm_producer.h"

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

/* Collect every LP_NORMAL offset on an all-visible page (no per-tuple work). */
static int
pgch_collect_all_visible(Page page, OffsetNumber *vis)
{
    OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
    OffsetNumber off;
    int          n = 0;

    for (off = FirstOffsetNumber; off <= maxoff; off = OffsetNumberNext(off))
    {
        ItemId lp = PageGetItemId(page, off);

        if (ItemIdIsNormal(lp))
            vis[n++] = off;
    }
    return n;
}

/*
 * Collect the snapshot-visible LP_NORMAL offsets on a not-all-visible page.
 * Three passes: gather header fields into parallel (SoA) arrays, run the
 * branch-light classifier over them, then resolve UNDECIDED tuples through the
 * MVCC oracle. The buffer must be share-locked for the whole call (the fallback
 * may set hint bits, which dirties the buffer and requires the lock).
 */
static int
pgch_collect_with_visibility(Relation rel, Buffer buf, BlockNumber blk, Page page,
                             Snapshot snapshot, OffsetNumber *vis)
{
    OffsetNumber  maxoff = PageGetMaxOffsetNumber(page);
    OffsetNumber  off;
    TransactionId snap_xmin = snapshot->xmin;
    TransactionId snap_xmax = snapshot->xmax;
    int           n = 0;
    int           m = 0;
    int           j;

    /* SoA scratch, sized for the worst case (one page's worth of tuples). */
    OffsetNumber  soa_off[MaxHeapTuplesPerPage];
    uint16        soa_im[MaxHeapTuplesPerPage];
    TransactionId soa_xmin[MaxHeapTuplesPerPage];
    TransactionId soa_xmax[MaxHeapTuplesPerPage];
    uint8         verdict[MaxHeapTuplesPerPage];

    /* Pass A: gather hint bits + raw xmin/xmax for each LP_NORMAL tuple. */
    for (off = FirstOffsetNumber; off <= maxoff; off = OffsetNumberNext(off))
    {
        ItemId          lp = PageGetItemId(page, off);
        HeapTupleHeader htup;

        if (!ItemIdIsNormal(lp))
            continue;

        htup = (HeapTupleHeader) PageGetItem(page, lp);
        soa_off[m] = off;
        soa_im[m] = htup->t_infomask;
        soa_xmin[m] = HeapTupleHeaderGetRawXmin(htup);
        soa_xmax[m] = HeapTupleHeaderGetRawXmax(htup);
        m++;
    }

    /* Pass B: branch-light classification over the flat arrays. */
    for (j = 0; j < m; j++)
        verdict[j] = (uint8) pgch_classify_tuple(soa_im[j], soa_xmin[j], soa_xmax[j],
                                                 snap_xmin, snap_xmax);

    /* Pass C: emit VISIBLE; resolve UNDECIDED via the exact MVCC oracle. */
    for (j = 0; j < m; j++)
    {
        if (verdict[j] == PGCH_VIS_VISIBLE)
        {
            vis[n++] = soa_off[j];
        }
        else if (verdict[j] == PGCH_VIS_UNDECIDED)
        {
            ItemId        lp = PageGetItemId(page, soa_off[j]);
            HeapTupleData loctup;

            loctup.t_data = (HeapTupleHeader) PageGetItem(page, lp);
            loctup.t_len = ItemIdGetLength(lp);
            loctup.t_tableOid = RelationGetRelid(rel);
            ItemPointerSet(&loctup.t_self, blk, soa_off[j]);

            if (HeapTupleSatisfiesVisibility(&loctup, snapshot, buf))
                vis[n++] = soa_off[j];
        }
        /* PGCH_VIS_INVISIBLE: skip */
    }

    return n;
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
    PgchDeformPlan       plan;
    uint32               cum;
    Datum               *values;
    bool                *isnull;
    OffsetNumber        *vis;
    int                  c;
    int                  i;

    for (c = 0; c < ncols; c++)
        if (cols[c].attno > max_attno)
            max_attno = cols[c].attno;

    /* Build attno-indexed deform metadata for attnos 1..max_attno. */
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

    /* Build the deform plan: precompute constant offsets for the leading
     * fixed-width run so projected prefix columns are direct loads (no per-tuple
     * walk over the non-projected prefix attrs), and the tail (varlena region
     * up to max_attno) is walked from its constant start offset. */
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

    values = (Datum *) palloc(sizeof(Datum) * max_attno);
    isnull = (bool *) palloc(sizeof(bool) * max_attno);
    vis = (OffsetNumber *) palloc(sizeof(OffsetNumber) * MaxHeapTuplesPerPage);

    cz = pgch_columnizer_begin(cols, ncols, producer, rows_per_block);

    /* Match a stock seqscan: BAS_BULKREAD keeps a large scan from evicting the
     * shared-buffer working set. */
    strategy = GetAccessStrategy(BAS_BULKREAD);
    nblocks = RelationGetNumberOfBlocks(rel);

    for (blk = 0; blk < nblocks; blk++)
    {
        Buffer buf;
        Page   page;
        int    n;
        int    k;

        CHECK_FOR_INTERRUPTS();

        buf = ReadBufferExtended(rel, MAIN_FORKNUM, blk, RBM_NORMAL, strategy);
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buf);

        if (PageIsAllVisible(page) && !snapshot->takenDuringRecovery)
            n = pgch_collect_all_visible(page, vis);
        else
            n = pgch_collect_with_visibility(rel, buf, blk, page, snapshot, vis);

        /* Visibility decisions (and any hint-bit writes by the fallback) are
         * done; drop the lock and deform the visible tuples under the pin. */
        LockBuffer(buf, BUFFER_LOCK_UNLOCK);

        for (k = 0; k < n; k++)
        {
            ItemId          lp = PageGetItemId(page, vis[k]);
            HeapTupleHeader htup = (HeapTupleHeader) PageGetItem(page, lp);

            pgch_deform_needed((const HeapTupleHeaderData *) htup, &plan, values, isnull);
            pgch_columnizer_add_row(cz, values, isnull);
        }

        ReleaseBuffer(buf);
    }

    FreeAccessStrategy(strategy);
    pfree(vis);
    pfree(isnull);
    pfree(values);
    pfree(plan.fixed);
    pfree(meta);

    return pgch_columnizer_finish(cz);
}
