/*-------------------------------------------------------------------------
 *
 * shm_deform.cpp
 *      Templated, column-major (struct-of-arrays) heap deform engine for the
 *      SHM-offload reader. One driver `deform_impl<bool Nullable>` serves both
 *      group A (NULL-free tuples) and group B (tuples with NULLs in
 *      non-projected columns); `if constexpr (Nullable)` removes the NULL path
 *      from group A's instantiation entirely, so the generated code matches a
 *      hand-rolled per-group implementation. Group C (short/missing-attr tuples)
 *      is handled by the C row-major path, not here.
 *
 *      The driver allocates nothing and never raises a PostgreSQL error: fixed
 *      column values are stored directly into the columnizer's per-column
 *      buffers (PgchDeformCol.dst_base) by type-monomorphic templated kernels;
 *      string columns are handed back to C (StringInfo append) via PgchStringFill.
 *      Compiled with -fno-exceptions -fno-rtti; all kernels/functors are
 *      trivial-destructor, so a PG longjmp from the string callback is safe.
 *
 * Copyright (c) 2025-2026, ClickHouse, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

extern "C"
{
#include "access/tupmacs.h"     /* att_align_pointer/_nominal, att_addlength_pointer, att_isnull */
#include "varatt.h"             /* VARSIZE_ANY (used by att_addlength_pointer) */
}

#include "shm_deform.h"

namespace
{

/* Alignment-safe typed load. PG stores fixed attrs naturally aligned, but the
 * memcpy is free under -O2 and lowers to a single load. */
template <typename T>
[[gnu::always_inline]] static inline T
load(const char *p)
{
    T v;
    __builtin_memcpy(&v, p, sizeof(T));
    return v;
}

/* Per-wire value transforms (stateless functors). */
struct Identity
{
    template <typename T> T operator() (T v) const { return v; }
};
struct DateToCh                                     /* PG 2000-epoch day -> CH 1970-epoch day */
{
    uint16 operator() (int32 v) const { return (uint16) (v + PGCH_DATE_EPOCH_DIFF); }
};
struct BoolToU8
{
    uint8 operator() (uint8 v) const { return v ? 1 : 0; }
};

/*
 * The reusable kernel: one instantiation per (Src, Dst, Xform). Rows-inner,
 * type-monomorphic; the contiguous store side autovectorizes, the per-row load
 * is a gather (scalar on NEON). `disp` is the constant prefix displacement (0 for
 * tail columns, where `cur` is already positioned).
 */
template <typename Src, typename Dst, typename Xform>
[[gnu::always_inline]] static inline void
fill_fixed_col(Dst *__restrict dst, size_t dst_row,
               char *const *__restrict cur, uint32 disp, size_t n)
{
    for (size_t r = 0; r < n; ++r)
        dst[dst_row + r] = Xform() (load<Src>(cur[r] + disp));
}

/* Wire -> kernel instantiation. Decimals are declined upstream; strings take the
 * callback path, so they never reach here. */
static inline void
dispatch_fixed(const PgchDeformCol &c, char *const *cur, size_t dst_row,
               uint32 disp, size_t n)
{
    switch (c.wire)
    {
        case SHM_WIRE_UINT8:
            fill_fixed_col<uint8, uint8, BoolToU8>((uint8 *) c.dst_base, dst_row, cur, disp, n);
            break;
        case SHM_WIRE_INT16:
            fill_fixed_col<int16, int16, Identity>((int16 *) c.dst_base, dst_row, cur, disp, n);
            break;
        case SHM_WIRE_INT32:
            fill_fixed_col<int32, int32, Identity>((int32 *) c.dst_base, dst_row, cur, disp, n);
            break;
        case SHM_WIRE_INT64:
            fill_fixed_col<int64, int64, Identity>((int64 *) c.dst_base, dst_row, cur, disp, n);
            break;
        case SHM_WIRE_FLOAT32:
            fill_fixed_col<float4, float4, Identity>((float4 *) c.dst_base, dst_row, cur, disp, n);
            break;
        case SHM_WIRE_FLOAT64:
            fill_fixed_col<float8, float8, Identity>((float8 *) c.dst_base, dst_row, cur, disp, n);
            break;
        case SHM_WIRE_DATE:
            fill_fixed_col<int32, uint16, DateToCh>((uint16 *) c.dst_base, dst_row, cur, disp, n);
            break;
        default:
            /* Contract violation: the C reader must only present these wires.
             * No ereport here (C++ frame) -- trap in debug, unreachable in prod. */
            Assert(false);
            pg_unreachable();
    }
}

/*
 * The unified column-major driver. Group A == deform_impl<false>, group B ==
 * deform_impl<true>. `bits` is unused (and may be NULL) when Nullable is false.
 */
template <bool Nullable>
static void
deform_impl(const PgchDeformDesc *d, char **cur, const bits8 **bits,
            size_t n, size_t dst_row, void *cz, PgchStringFill str_fill)
{
    const PgchDeformCol *col = d->col;
    int     prefix_len = Nullable ? d->prefix_len_b : d->prefix_len_a;
    uint32  walk_start_off = Nullable ? d->walk_start_off_b : d->walk_start_off_a;
    int     i;
    size_t  r;

    /* Prefix: projected fixed-width columns at a constant displacement. The
     * non-projected prefix columns are skipped entirely (no cursor advance). */
    for (i = 0; i < prefix_len; ++i)
        if (col[i].is_needed)
            dispatch_fixed(col[i], cur, dst_row, col[i].disp, n);

    if (prefix_len >= d->max_attno)
        return;

    /* Position each row's cursor at the start of the tail (varlena) region. */
    for (r = 0; r < n; ++r)
        cur[r] += walk_start_off;

    /* Tail: advance the per-row cursor through columns prefix_len..max_attno-1,
     * filling projected ones. */
    for (i = prefix_len; i < d->max_attno; ++i)
    {
        const PgchDeformCol &c = col[i];

        if constexpr (Nullable)
        {
            if (c.nullable)
            {
                /* Nullable => non-projected (projected cols are NOT NULL): no
                 * fill, per-row skip of NULLs. */
                if (c.attlen == -1)
                {
                    for (r = 0; r < n; ++r)
                        if (!att_isnull(i, bits[r]))
                        {
                            cur[r] = (char *) att_align_pointer((uintptr_t) cur[r], c.attalign, -1, cur[r]);
                            cur[r] = (char *) att_addlength_pointer((uintptr_t) cur[r], -1, cur[r]);
                        }
                }
                else
                {
                    for (r = 0; r < n; ++r)
                        if (!att_isnull(i, bits[r]))
                            cur[r] = (char *) att_align_nominal((uintptr_t) cur[r], c.attalign) + c.attlen;
                }
                continue;
            }
        }

        /* Present for every row (group A, or a NOT NULL group-B column). */
        if (c.attlen == -1)
            for (r = 0; r < n; ++r)
                cur[r] = (char *) att_align_pointer((uintptr_t) cur[r], c.attalign, -1, cur[r]);
        else
            for (r = 0; r < n; ++r)
                cur[r] = (char *) att_align_nominal((uintptr_t) cur[r], c.attalign);

        if (c.is_needed)
        {
            if (c.is_string)
                str_fill(cz, c.col_index, dst_row, cur, n);
            else
                dispatch_fixed(c, cur, dst_row, 0, n);
        }

        if (c.attlen == -1)
            for (r = 0; r < n; ++r)
                cur[r] = (char *) att_addlength_pointer((uintptr_t) cur[r], -1, cur[r]);
        else
            for (r = 0; r < n; ++r)
                cur[r] += c.attlen;
    }
}

}                               /* anonymous namespace */

extern "C" void
pgch_columnar_deform_simple(const PgchDeformDesc *desc, char **cur,
                            size_t n, size_t dst_row, void *cz, PgchStringFill str_fill)
{
    deform_impl<false>(desc, cur, nullptr, n, dst_row, cz, str_fill);
}

extern "C" void
pgch_columnar_deform_nullable(const PgchDeformDesc *desc, char **cur, const bits8 **bits,
                              size_t n, size_t dst_row, void *cz, PgchStringFill str_fill)
{
    deform_impl<true>(desc, cur, bits, n, dst_row, cz, str_fill);
}
