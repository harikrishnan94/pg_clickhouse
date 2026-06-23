/*-------------------------------------------------------------------------
 *
 * shm_deform.cpp
 *      Templated, column-major (struct-of-arrays) heap deform engine for the
 *      SHM-offload reader, organized as a per-scan COMPILED STEP PLAN.
 *
 *      pgch_build_deform_plans lowers a scan's projection into an ordered list
 *      of specialized steps (one per projected column, plus the cursor walks
 *      between them). Each step is a pre-instantiated kernel selected by the
 *      column's (attlen, attalign, wire): the value transform (Identity/DateToCh/
 *      BoolToU8) and the fixed-width size/alignment are compile-time constants,
 *      so the per-row inner loops are monomorphic -- no PgchDeformCol field
 *      loads, no attlen sign-test, and only the data-dependent varlena/NULL
 *      branches survive. This is the C++-template analog of a JIT-compiled
 *      deform (slot_compile_deform), but the variants are compiled ahead of time
 *      and merely SELECTED once per scan.
 *
 *      Structural wins over the old per-column two-pass walk:
 *        - runs of fixed columns with statically-known offsets collapse into a
 *          constant `lead`/`disp` (no per-row work);
 *        - the reposition + per-skip align/addlength passes fuse into one
 *          row-major walk per segment (group A: unrolled up to 4 varlena hops);
 *        - only varlena (and, group B, nullable) columns cost a per-row cursor
 *          advance -- the irreducible VARSIZE_ANY header gather.
 *
 *      The kernels allocate nothing and never raise a PostgreSQL error: fixed
 *      values are stored into the columnizer's per-column buffers (dst_base);
 *      string columns are handed back to C (StringInfo append) via PgchStringFill.
 *      Compiled -fno-exceptions -fno-rtti; all kernels are trivial-destructor, so
 *      a PG longjmp from the string callback is safe.
 *
 * Copyright (c) 2025-2026, ClickHouse, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

extern "C"
{
#include "access/tupmacs.h"     /* att_isnull, TYPALIGN_*; alignment helpers */
#include "varatt.h"             /* VARSIZE_ANY, VARATT_NOT_PAD_BYTE */
}

#include "shm_deform.h"

namespace
{

/* Alignment-safe typed load; lowers to a single load under -O2. */
template <typename T>
[[gnu::always_inline]] static inline T
load(const char *p)
{
    T v;
    __builtin_memcpy(&v, p, sizeof(T));
    return v;
}

/* Per-wire value transforms (stateless functors). */
struct Identity { template <typename T> T operator() (T v) const { return v; } };
struct DateToCh { uint16 operator() (int32 v) const { return (uint16) (v + PGCH_DATE_EPOCH_DIFF); } };
struct BoolToU8 { uint8 operator() (uint8 v) const { return v ? 1 : 0; } };

/* TYPALIGN_{CHAR,SHORT,INT,DOUBLE} char -> numeric alignment. */
static inline uint8
align_bytes(char a)
{
    switch (a)
    {
        case TYPALIGN_DOUBLE: return 8;
        case TYPALIGN_INT:    return 4;
        case TYPALIGN_SHORT:  return 2;
        default:              return 1;   /* TYPALIGN_CHAR */
    }
}

/* ----------------------------------------------------------------------- */
/* Specialized leaf kernels                                                */
/* ----------------------------------------------------------------------- */

/*
 * FILL_CONST: a projected fixed column in the constant-offset region (the
 * prefix). cur stays at the tuple data start; the value is at a per-scan
 * constant displacement. Contiguous store side autovectorizes.
 */
template <typename Src, typename Dst, typename Xform>
static void
k_fill_const(const PgchStep *st, char **__restrict cur, const bits8 **,
             size_t n, size_t dst_row, void *, PgchStringFill)
{
    Dst   *__restrict dst = (Dst *) st->dst_base;
    uint32 disp = st->disp;

    for (size_t r = 0; r < n; ++r)
        dst[dst_row + r] = Xform() (load<Src>(cur[r] + disp));
}

/*
 * FILL_WALK: a projected fixed column reached by the cursor walk (after a
 * varlena, or in group B after a possibly-NULL column). Aligns the per-row
 * cursor (constant mask), reads, and advances past the column so the next step
 * resumes from its end. alignof(Src) == the PG attalign for every wire here.
 */
template <typename Src, typename Dst, typename Xform>
static void
k_fill_walk(const PgchStep *st, char **__restrict cur, const bits8 **,
            size_t n, size_t dst_row, void *, PgchStringFill)
{
    Dst *__restrict dst = (Dst *) st->dst_base;

    for (size_t r = 0; r < n; ++r)
    {
        char *c = (char *) TYPEALIGN(alignof(Src), (uintptr_t) cur[r]);
        dst[dst_row + r] = Xform() (load<Src>(c));
        cur[r] = c + sizeof(Src);
    }
}

/*
 * FILL_STRING: a projected varlena column. Position each row's cursor at the
 * datum (att_align_pointer: align only for a 4-byte header), hand the batch to
 * the C string fill, then advance past each datum (att_addlength_pointer).
 */
static void
k_fill_string(const PgchStep *st, char **cur, const bits8 **,
              size_t n, size_t dst_row, void *cz, PgchStringFill sf)
{
    uint8 al = st->align;

    for (size_t r = 0; r < n; ++r)
    {
        char *c = cur[r];
        if (!VARATT_NOT_PAD_BYTE(c))               /* 4-byte-header form must align */
            c = (char *) TYPEALIGN(al, (uintptr_t) c);
        cur[r] = c;
    }
    sf(cz, st->col_index, dst_row, cur, n);
    for (size_t r = 0; r < n; ++r)
        cur[r] += VARSIZE_ANY(cur[r]);
}

/*
 * WALK (group A, unrolled): advance each row's cursor by a constant `lead`, then
 * through Nhop skipped VARLENA columns of uniform alignment Align. No final
 * align (the following FILL aligns). Nhop and Align are compile-time, so the hop
 * loop is fully unrolled and the only surviving branch is VARATT_NOT_PAD_BYTE.
 */
template <int Align, int Nhop>
static void
k_walk_uv(const PgchStep *st, char **__restrict cur, const bits8 **,
          size_t n, size_t, void *, PgchStringFill)
{
    uint32 lead = st->disp;

    for (size_t r = 0; r < n; ++r)
    {
        char *c = cur[r] + lead;
        for (int j = 0; j < Nhop; ++j)
        {
            if (VARATT_NOT_PAD_BYTE(c))
                c += VARSIZE_ANY(c);
            else
            {
                c = (char *) TYPEALIGN(Align, (uintptr_t) c);
                c += VARSIZE_ANY(c);
            }
        }
        cur[r] = c;
    }
}

/* WALK (group A, generic): a mixed varlena/fixed skip run of any length. */
static void
k_walk_generic(const PgchStep *st, char **cur, const bits8 **,
               size_t n, size_t, void *, PgchStringFill)
{
    const PgchHop *h = st->hops;
    int            m = st->nhop;
    uint32         lead = st->disp;

    for (size_t r = 0; r < n; ++r)
    {
        char *c = cur[r] + lead;
        for (int j = 0; j < m; ++j)
        {
            if (h[j].attlen < 0)
            {
                if (VARATT_NOT_PAD_BYTE(c))
                    c += VARSIZE_ANY(c);
                else
                {
                    c = (char *) TYPEALIGN(h[j].align, (uintptr_t) c);
                    c += VARSIZE_ANY(c);
                }
            }
            else
                c = (char *) TYPEALIGN(h[j].align, (uintptr_t) c) + h[j].attlen;
        }
        cur[r] = c;
    }
}

/* WALK (group B, generic + NULL-aware): a NULL column occupies no bytes. */
static void
k_walk_nullable(const PgchStep *st, char **cur, const bits8 **bits,
                size_t n, size_t, void *, PgchStringFill)
{
    const PgchHop *h = st->hops;
    int            m = st->nhop;
    uint32         lead = st->disp;

    for (size_t r = 0; r < n; ++r)
    {
        char        *c = cur[r] + lead;
        const bits8 *bp = bits[r];

        for (int j = 0; j < m; ++j)
        {
            if (h[j].null_bit >= 0 && att_isnull(h[j].null_bit, bp))
                continue;                          /* NULL: no data bytes, no advance */
            if (h[j].attlen < 0)
            {
                if (VARATT_NOT_PAD_BYTE(c))
                    c += VARSIZE_ANY(c);
                else
                {
                    c = (char *) TYPEALIGN(h[j].align, (uintptr_t) c);
                    c += VARSIZE_ANY(c);
                }
            }
            else
                c = (char *) TYPEALIGN(h[j].align, (uintptr_t) c) + h[j].attlen;
        }
        cur[r] = c;
    }
}

/* ----------------------------------------------------------------------- */
/* Dispatch: bind a column's (wire, attlen, attalign) to a kernel instance  */
/* ----------------------------------------------------------------------- */

static PgchStepFn
pick_fill_const(ShmWireType w)
{
    switch (w)
    {
        case SHM_WIRE_UINT8:   return k_fill_const<uint8,  uint8,  BoolToU8>;
        case SHM_WIRE_INT16:   return k_fill_const<int16,  int16,  Identity>;
        case SHM_WIRE_INT32:   return k_fill_const<int32,  int32,  Identity>;
        case SHM_WIRE_INT64:   return k_fill_const<int64,  int64,  Identity>;
        case SHM_WIRE_FLOAT32: return k_fill_const<float4, float4, Identity>;
        case SHM_WIRE_FLOAT64: return k_fill_const<float8, float8, Identity>;
        case SHM_WIRE_DATE:    return k_fill_const<int32,  uint16, DateToCh>;
        default:               Assert(false); pg_unreachable();
    }
}

static PgchStepFn
pick_fill_walk(ShmWireType w)
{
    switch (w)
    {
        case SHM_WIRE_UINT8:   return k_fill_walk<uint8,  uint8,  BoolToU8>;
        case SHM_WIRE_INT16:   return k_fill_walk<int16,  int16,  Identity>;
        case SHM_WIRE_INT32:   return k_fill_walk<int32,  int32,  Identity>;
        case SHM_WIRE_INT64:   return k_fill_walk<int64,  int64,  Identity>;
        case SHM_WIRE_FLOAT32: return k_fill_walk<float4, float4, Identity>;
        case SHM_WIRE_FLOAT64: return k_fill_walk<float8, float8, Identity>;
        case SHM_WIRE_DATE:    return k_fill_walk<int32,  uint16, DateToCh>;
        default:               Assert(false); pg_unreachable();
    }
}

/* Unrolled all-varlena walk for align in {1,2,4,8} and nhop in 1..4; else NULL. */
static PgchStepFn
pick_walk_uv(uint8 align, int nhop)
{
#define WUV(A) \
    switch (nhop) { case 1: return k_walk_uv<A,1>; case 2: return k_walk_uv<A,2>; \
                    case 3: return k_walk_uv<A,3>; case 4: return k_walk_uv<A,4>; }
    switch (align)
    {
        case 1: WUV(1); break;
        case 2: WUV(2); break;
        case 4: WUV(4); break;
        case 8: WUV(8); break;
    }
#undef WUV
    return nullptr;
}

/* ----------------------------------------------------------------------- */
/* Plan builder (once per scan)                                             */
/* ----------------------------------------------------------------------- */

/* Emit a WALK step for the accumulated hop run, choosing the unrolled kernel
 * when the run is all-varlena of uniform alignment and <= 4 hops, else generic.
 * Emits nothing when there is no cursor movement (lead == 0 and no hops). */
static void
emit_walk(PgchStep *step, int *ns, uint32 lead,
          const PgchHop *hops, int m, bool nullable_group)
{
    if (lead == 0 && m == 0)
        return;

    PgchStep *s = &step[(*ns)++];
    s->disp = lead;
    s->hops = hops;
    s->nhop = m;
    s->dst_base = nullptr;
    s->col_index = -1;
    s->align = 0;

    if (nullable_group)
    {
        s->run = k_walk_nullable;
        return;
    }
    if (m >= 1 && m <= 4)
    {
        bool  uniform = true;
        uint8 a = hops[0].align;

        for (int j = 0; j < m; ++j)
            if (hops[j].attlen >= 0 || hops[j].align != a)
            {
                uniform = false;
                break;
            }
        if (uniform)
        {
            PgchStepFn fn = pick_walk_uv(a, m);
            if (fn) { s->run = fn; return; }
        }
    }
    s->run = k_walk_generic;
}

/* Build one group's plan. `prefix_len`/`walk_start_off` select the group; for
 * group B, nullable non-projected columns become NULL-checked hops. */
static void
build_plan(const PgchDeformDesc *d, int prefix_len, uint32 walk_start_off,
           bool nullable_group, PgchStep *step, PgchHop *hop, PgchDeformPlan *plan)
{
    const PgchDeformCol *col = d->col;
    int    ns = 0;
    int    nh = 0;
    int    i;

    /* Prefix: projected fixed columns at constant displacement (cur unmoved). */
    for (i = 0; i < prefix_len; ++i)
        if (col[i].is_needed)
        {
            PgchStep *s = &step[ns++];
            s->run = pick_fill_const(col[i].wire);     /* prefix is fixed-width, never string */
            s->disp = col[i].disp;
            s->dst_base = col[i].dst_base;
            s->col_index = col[i].col_index;
            s->hops = nullptr; s->nhop = 0; s->align = 0;
        }

    /* Tail: alternate a fused walk (to the next projected column) and its fill. */
    if (prefix_len < d->max_attno)
    {
        uint32 lead = walk_start_off;
        int    hop_start = nh;

        for (i = prefix_len; i < d->max_attno; ++i)
        {
            const PgchDeformCol *c = &col[i];

            if (!c->is_needed)
            {
                hop[nh].attlen = c->attlen;
                hop[nh].align = align_bytes(c->attalign);
                hop[nh].null_bit = (nullable_group && c->nullable) ? (int16) i : -1;
                nh++;
                continue;
            }

            /* projected column (group B: NOT NULL): walk to it, then fill. */
            emit_walk(step, &ns, lead, &hop[hop_start], nh - hop_start, nullable_group);

            PgchStep *s = &step[ns++];
            if (c->is_string)
            {
                s->run = k_fill_string;
                s->align = align_bytes(c->attalign);
                s->col_index = c->col_index;
                s->dst_base = nullptr; s->disp = 0; s->hops = nullptr; s->nhop = 0;
            }
            else
            {
                s->run = pick_fill_walk(c->wire);
                s->dst_base = c->dst_base;
                s->col_index = c->col_index;
                s->disp = 0; s->align = 0; s->hops = nullptr; s->nhop = 0;
            }

            /* the fill advanced cur past this column; start a fresh segment */
            lead = 0;
            hop_start = nh;
        }
        /* any skipped columns after the last projected one are irrelevant */
    }

    plan->step = step;
    plan->n_step = ns;
}

}                               /* anonymous namespace */

extern "C" void
pgch_build_deform_plans(const PgchDeformDesc *desc,
                        PgchStep *step_a, PgchHop *hop_a, PgchDeformPlan *plan_a,
                        PgchStep *step_b, PgchHop *hop_b, PgchDeformPlan *plan_b)
{
    /*
     * Load-bearing invariant: the FILL kernels never consult the NULL bitmap for
     * projected columns, because the planner gate (shm_customscan.c declines any
     * query that references a nullable column) guarantees every projected column
     * is NOT NULL. The columnar path therefore has no per-row fail-closed check
     * (unlike the row-major columnizer). If phase-2 ever admits NULL projections,
     * the FILL kernels must gain a NULL check before this assert is relaxed.
     */
#ifdef USE_ASSERT_CHECKING
    for (int i = 0; i < desc->max_attno; ++i)
        Assert(!(desc->col[i].is_needed && desc->col[i].nullable));
#endif

    build_plan(desc, desc->prefix_len_a, desc->walk_start_off_a, /*nullable=*/false,
               step_a, hop_a, plan_a);
    build_plan(desc, desc->prefix_len_b, desc->walk_start_off_b, /*nullable=*/true,
               step_b, hop_b, plan_b);
}

extern "C" void
pgch_columnar_deform_run(const PgchDeformPlan *plan, char **cur, const bits8 **bits,
                         size_t n, size_t dst_row, void *cz, PgchStringFill str_fill)
{
    const PgchStep *step = plan->step;
    int             ns = plan->n_step;

    for (int s = 0; s < ns; ++s)
        step[s].run(&step[s], cur, bits, n, dst_row, cz, str_fill);
}
