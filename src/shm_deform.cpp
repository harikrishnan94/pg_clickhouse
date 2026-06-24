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
#include "shm_numeric.h"        /* pgch_numeric_to_decimal_wire_core (shared C/C++ converter) */

namespace
{

/* Wire width in bytes for a Decimal wire tag (4 / 8 / 16). */
static inline uint32
dec_width_of(ShmWireType w)
{
    switch (w)
    {
        case SHM_WIRE_DECIMAL32:  return 4;
        case SHM_WIRE_DECIMAL64:  return 8;
        case SHM_WIRE_DECIMAL128: return 16;
        default:                  return 0;
    }
}

/* ----------------------------------------------------------------------- */
/* Numeric -> Decimal fast converter (width-specialized accumulator)        */
/* ----------------------------------------------------------------------- */

/*
 * Accumulator traits for the inline numeric->Decimal converter. The accumulator
 * is chosen by WIRE WIDTH, not just for capacity: a uint64 divide-by-constant
 * (the end-of-loop rescale) strength-reduces to a reciprocal-multiply on x86-64,
 * whereas an __int128 divide-by-constant stays a __udivti3 libcall -- so the
 * common TPC-H Decimal32/64 path uses uint64 and Decimal128 uses __int128.
 *
 *   max_limbs : NBASE^max_limbs < 2^(bits(Acc)-1), so the guard-free Horner
 *               accumulation provably cannot wrap Acc (an over-limb value takes
 *               the guarded fallback converter instead).
 *   max_exp   : 10^max_exp < 2^(bits(Acc)-1), so the rescaled magnitude cannot
 *               wrap Acc. (The Decimal *wire width* fit -- e.g. a Decimal32 value
 *               held in a uint64 accumulator -- is enforced separately by the
 *               explicit signed-width `limit` check, not by max_exp.)
 *
 * 256 later: add DecAcc<u256> { using S = i256; max_limbs = 19; max_exp = 76; }
 * plus a portable uint64[4] limb struct exposing * + / << > and signed negate
 * AND a fast small-constant divide (a generic struct operator/ will not
 * strength-reduce). _BitInt(256) is rejected by the C++ TU, so it must be a
 * struct. The guarded fallback / scalar / resolver paths are __int128-bound and
 * would also need widening; this seam reduces that work, it is not "drop-in".
 */
template <typename Acc> struct DecAcc;

template <> struct DecAcc<uint64_t>             /* Decimal32 (Width 4) + Decimal64 (Width 8) */
{
    using S = int64_t;                          /* signed store/negate type */
    static constexpr int max_limbs = 4;         /* NBASE^4 = 10^16 < 2^63 */
    static constexpr int max_exp   = 18;        /* 10^18        < 2^63; uint64 /const reciprocal-multiplies */
};

template <> struct DecAcc<unsigned __int128>    /* Decimal128 (Width 16) */
{
    using S = __int128;
    static constexpr int max_limbs = 9;         /* NBASE^9 = 10^36 < 2^127 */
    static constexpr int max_exp   = 38;        /* 10^38        < 2^127 */
};

/* 10^0 .. 10^N as a constexpr (.rodata) table; no first-call `inited` guard. */
template <typename Acc, int N>
struct Pow10Tbl
{
    Acc v[N + 1];
    constexpr Pow10Tbl() : v{}
    {
        Acc p = 1;
        for (int i = 0; i <= N; ++i) { v[i] = p; p *= 10; }
    }
};

template <typename Acc>
[[gnu::always_inline]] static inline Acc
dec_pow10(unsigned e)
{
    static constexpr Pow10Tbl<Acc, DecAcc<Acc>::max_exp> t{};
    return t.v[e];
}

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
/* PG timestamp (int64 us since 2000-01-01) -> CH DateTime64(6) ticks (int64 us
 * since 1970-01-01 UTC). Mirrors write_fixed_value's SHM_WIRE_DATETIME64 case. */
struct TimestampToCh { int64 operator() (int64 v) const { return v + PGCH_TS_EPOCH_DIFF_US; } };

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

/*
 * Power-of-two alignment of a cursor that is `align`-aligned and then advanced by
 * a constant `width` bytes: min(align, largest power of two dividing width). Used
 * by the plan builder to fold a run of consecutive skipped fixed-width hops into a
 * single hop -- once the cursor is A-aligned, a later fixed column whose alignment
 * is already satisfied adds no realignment, so the per-hop TYPEALIGN+addlen
 * collapses to one TYPEALIGN + a summed constant. Holds for any attlen (no
 * attlen-multiple-of-align assumption): width's low bit tracks the true residue.
 */
static inline uint8
run_align_after(uint8 align, uint32 width)
{
    uint32 lowbit = width & (0u - width);   /* largest pow2 dividing width (width > 0) */
    return (uint8) (lowbit < align ? lowbit : align);
}

template <int Align, int Nhop>
static inline char *
walk_uv_one(char *c)
{
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
    return c;
}

static inline char *
walk_generic_one(char *c, const PgchHop *h, int m)
{
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
    return c;
}

static inline char *
walk_nullable_one(char *c, const PgchHop *h, int m, const bits8 *bp)
{
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
    return c;
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
    uint32 disp = st->disp;

    for (size_t r = 0; r < n; ++r)
    {
        char *c = (char *) TYPEALIGN(alignof(Src), (uintptr_t) (cur[r] + disp));
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
 * Hot converter: a SHORT (1-byte-header), non-external numeric varlena -> the
 * ClickHouse Decimal wire value, round(value * 10^scale) as a two's-complement
 * little-endian integer of `Width` bytes, written to `out`. `*consumed` receives
 * the on-disk datum size so the caller advances the cursor without re-decoding
 * the header (no second VARSIZE_ANY).
 *
 * A value whose magnitude exceeds the cheap precondition (corrupt data, or a
 * legitimate value needing more limbs than the chosen accumulator holds) is NOT
 * an error: it delegates to the general guarded converter (the documented 256
 * swap point). A stored NaN/Inf returns SPECIAL; an out-of-wire-range magnitude
 * returns OVERFLOW. Allocation-free; never raises.
 */
template <typename Acc, uint32 Width>
[[gnu::always_inline]] static inline PgchDecConv
dec_convert_short(const char *p, uint32 scale, char *out, uint32 *consumed)
{
    using L = DecAcc<Acc>;

    uint32      totlen      = (uint32) VARSIZE_SHORT(p);    /* = (header byte >> 1) */
    const char *payload     = p + VARHDRSZ_SHORT;
    uint32      payload_len = totlen - VARHDRSZ_SHORT;

    *consumed = totlen;                                     /* kernel advances by this; no VARSIZE_ANY */

    uint16 n_header;
    __builtin_memcpy(&n_header, payload, sizeof(uint16));
    if ((n_header & PGCH_NUMERIC_SIGN_MASK) == PGCH_NUMERIC_SPECIAL) [[unlikely]]
        return PGCH_DECCONV_SPECIAL;                        /* NaN / +Inf / -Inf */

    int sign_neg, weight, nhdr;
    if (n_header & PGCH_NUMERIC_SHORT)                      /* short numeric header (2 bytes) */
    {
        nhdr     = 2;
        sign_neg = (n_header & PGCH_NUMERIC_SHORT_SIGN_MASK) != 0;
        int w    = n_header & PGCH_NUMERIC_SHORT_WEIGHT_MASK;
        if (n_header & PGCH_NUMERIC_SHORT_WEIGHT_SIGN)
            w |= ~PGCH_NUMERIC_SHORT_WEIGHT_MASK;           /* sign-extend 7-bit weight */
        weight   = w;
    }
    else                                                    /* long numeric header (4 bytes) */
    {
        int16 w16;
        nhdr     = 4;
        sign_neg = (n_header & PGCH_NUMERIC_SIGN_MASK) == PGCH_NUMERIC_NEG;
        __builtin_memcpy(&w16, payload + sizeof(uint16), sizeof(int16));
        weight   = w16;
    }

    const char *digp    = payload + nhdr;
    int         ndigits = ((int) payload_len - nhdr) / 2;   /* signed subtraction (robust off the short path) */

    /* One branch: fast path vs the general guarded fallback (rare; 256 swap point). */
    int hi_exp = PGCH_DEC_DIGITS * (weight + 1) + (int) scale;
    if (ndigits > L::max_limbs || hi_exp > L::max_exp) [[unlikely]]
        return pgch_numeric_to_decimal_wire_core(p, scale, Width, out);

    /* Guard-free Horner: imul-by-constant + add (no per-limb pow10/divide/guard). */
    Acc H = 0;
    for (int i = 0; i < ndigits; ++i)
    {
        uint16 d16;
        __builtin_memcpy(&d16, digp + (size_t) i * 2, sizeof(uint16));
        H = H * (Acc) PGCH_NBASE + (Acc) d16;
    }

    /*
     * Single end-of-loop rescale: one multiply, or one constant-divisor divide.
     * On uint64 the divide strength-reduces to a reciprocal-multiply; on __int128
     * it stays one __udivti3 (still one divide instead of N per-limb divides).
     */
    int texp = PGCH_DEC_DIGITS * (weight - ndigits + 1) + (int) scale;   /* = hi_exp - 4*ndigits */
    Acc mag;
    if (texp >= 0)
    {
        mag = H * dec_pow10<Acc>((unsigned) texp);          /* texp <= hi_exp <= max_exp */
    }
    else
    {
        switch (-texp)                                      /* valid numeric(P,S): -texp in {1,2,3} */
        {
            case 1:  mag = (H + 5)   / 10;   break;
            case 2:  mag = (H + 50)  / 100;  break;         /* the scale-2 TPC-H hot constant */
            case 3:  mag = (H + 500) / 1000; break;
            default:                                        /* corrupt-data safety only */
            {
                unsigned s = (unsigned) -texp;
                if (s > (unsigned) L::max_exp)
                    mag = 0;                                /* 10^s >> H: rounds to 0 */
                else
                {
                    Acc q = dec_pow10<Acc>(s);
                    mag = (H + q / 2) / q;
                }
                break;
            }
        }
    }

    /* Defensive (cannot fire for valid data within the precondition) + the
     * signed-width range check (this is what enforces e.g. Decimal32-in-uint64). */
    Acc limit = (Acc) 1 << (8u * Width - 1);                /* 2^(bits-1) */
    if (sign_neg ? (mag > limit) : (mag > limit - 1)) [[unlikely]]
        return PGCH_DECCONV_OVERFLOW;

    typename L::S sv = sign_neg ? -(typename L::S) mag : (typename L::S) mag;
    __builtin_memcpy(out, &sv, Width);                      /* low Width bytes, two's-complement LE */
    return PGCH_DECCONV_OK;
}

/*
 * FILL_DECIMAL: a projected heap numeric (varlena, attlen = -1) whose ClickHouse
 * wire form is a fixed-width Decimal (Width = 4/8/16). The common case is a SHORT
 * varlena (numeric(P<=38,S) datums are tiny), converted inline into the column's
 * fixed dst_base by dec_convert_short with no detoast and no second header
 * decode. The cold branch handles the 4-byte / compressed / external forms: a
 * plain 4-byte-uncompressed numeric is converted inline via the general core
 * converter (NOT a fault -- e.g. a column with STORAGE PLAIN), while a genuinely
 * compressed/external datum is recorded as a deferred fault (resolved on the C
 * side before the block is published). NaN/Inf and out-of-range also fault.
 */
template <typename Acc, uint32 Width>
static void
k_fill_decimal(const PgchStep *st, char **cur, const bits8 **,
               size_t n, size_t dst_row, void *cz, PgchStringFill)
{
    uint8  al    = st->align;
    uint32 scale = st->dec_scale;
    char  *base  = (char *) st->dst_base;

    for (size_t r = 0; r < n; ++r)
    {
        char *c   = cur[r];
        uint8 b   = (uint8) *c;
        char *dst = base + (dst_row + r) * Width;

        if ((b & 1) && b != 0x01) [[likely]]                /* plain short, not external */
        {
            uint32      consumed;
            PgchDecConv rc = dec_convert_short<Acc, Width>(c, scale, dst, &consumed);

            if (rc != PGCH_DECCONV_OK) [[unlikely]]
            {
                __builtin_memset(dst, 0, Width);
                pgch_columnizer_note_dec_fault(cz, dst, c, scale, Width,
                                               st->col_index, (int) rc);
            }
#ifdef USE_ASSERT_CHECKING
            else
            {
                /* Keep the fast path under the oracle: cross-check every OK row
                 * against the general per-limb converter. ref is sized to the
                 * compile-time Width (256-safe; no [16] stack smash). */
                char        ref[Width];
                PgchDecConv rrc = pgch_numeric_to_decimal_wire_core(c, scale, Width, ref);

                Assert(rrc == PGCH_DECCONV_OK && memcmp(dst, ref, Width) == 0);
            }
#endif
            cur[r] = c + consumed;
        }
        else [[unlikely]]                                   /* 4B-uncompressed / compressed / external */
        {
            char *cc = c;
            if (!VARATT_NOT_PAD_BYTE(cc))                   /* pad-safe align (NOT a blind TYPEALIGN) */
                cc = (char *) TYPEALIGN(al, (uintptr_t) cc);

            if (VARATT_IS_EXTERNAL(cc) || VARATT_IS_COMPRESSED(cc))
            {
                __builtin_memset(dst, 0, Width);            /* truly toasted: resolver detoasts + converts */
                pgch_columnizer_note_dec_fault(cz, dst, cc, scale, Width,
                                               st->col_index, (int) PGCH_DECCONV_TOASTED);
            }
            else
            {
                /* Plain 4-byte-uncompressed numeric: convert inline (not a fault). */
                PgchDecConv rc = pgch_numeric_to_decimal_wire_core(cc, scale, Width, dst);
                if (rc != PGCH_DECCONV_OK)
                {
                    __builtin_memset(dst, 0, Width);
                    pgch_columnizer_note_dec_fault(cz, dst, cc, scale, Width,
                                                   st->col_index, (int) rc);
                }
            }
            cur[r] = cc + VARSIZE_ANY(cc);
        }
    }
}

/* Bind a Decimal wire tag to its (accumulator, width) kernel instantiation. The
 * accumulator is uint64 for Decimal32/64 (its constant-divisor rescale becomes a
 * reciprocal-multiply) and __int128 for Decimal128. */
static PgchStepFn
pick_fill_decimal(ShmWireType w)
{
    switch (w)
    {
        case SHM_WIRE_DECIMAL32:  return k_fill_decimal<uint64_t, 4>;
        case SHM_WIRE_DECIMAL64:  return k_fill_decimal<uint64_t, 8>;
        case SHM_WIRE_DECIMAL128: return k_fill_decimal<unsigned __int128, 16>;
        /* case SHM_WIRE_DECIMAL256: return k_fill_decimal<u256, 32>;  // 256 later */
        default:                  Assert(false); pg_unreachable();
    }
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
        c = walk_uv_one<Align, Nhop>(c);
        if (st->align)
            c = (char *) TYPEALIGN(st->align, (uintptr_t) c);
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
    uint8          final_align = st->align;

    if (final_align)
    {
        for (size_t r = 0; r < n; ++r)
        {
            char *c = cur[r] + lead;

            c = walk_generic_one(c, h, m);
            cur[r] = (char *) TYPEALIGN(final_align, (uintptr_t) c);
        }
    }
    else
    {
        for (size_t r = 0; r < n; ++r)
        {
            char *c = cur[r] + lead;

            cur[r] = walk_generic_one(c, h, m);
        }
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
        c = walk_nullable_one(c, h, m, bp);
        if (st->align)
            c = (char *) TYPEALIGN(st->align, (uintptr_t) c);
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
        case SHM_WIRE_DATETIME64: return k_fill_const<int64, int64, TimestampToCh>;
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
        case SHM_WIRE_DATETIME64: return k_fill_walk<int64, int64, TimestampToCh>;
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
          const PgchHop *hops, int m, bool nullable_group, uint8 final_align)
{
    if (lead == 0 && m == 0 && final_align == 0)
        return;

    PgchStep *s = &step[(*ns)++];
    s->kind = PGCH_STEP_WALK;
    s->disp = lead;
    s->hops = hops;
    s->nhop = m;
    s->dst_base = nullptr;
    s->col_index = -1;
    s->align = final_align;

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

/*
 * Try to form a statically-addressable fixed-width tail run starting at a
 * projected fixed column. The run is safe only while every later column's
 * alignment is <= the first column's alignment; otherwise offsets would depend
 * on the post-varlena cursor address. In group B, nullable skipped columns also
 * terminate the run because NULLs occupy no bytes.
 */
static bool
tail_fixed_run(const PgchDeformCol *col, int max_attno, int start, bool nullable_group,
               int *run_end, uint8 *base_align, uint32 *run_width)
{
    const PgchDeformCol *first = &col[start];
    uint8               ba;
    uint32              off = 0;
    int                 j;

    if (!first->is_needed || first->is_string || first->is_decimal || first->attlen <= 0)
        return false;

    ba = align_bytes(first->attalign);
    for (j = start; j < max_attno; ++j)
    {
        const PgchDeformCol *c = &col[j];
        uint8               a;

        if (c->attlen <= 0 || c->is_string || c->is_decimal)
            break;
        if (nullable_group && c->nullable && !c->is_needed)
            break;

        a = align_bytes(c->attalign);
        if (a > ba)
            break;

        off = (uint32) att_align_nominal(off, c->attalign);
        off += (uint32) c->attlen;
    }

    *run_end = j;
    *base_align = ba;
    *run_width = off;
    return j > start;
}

static void
emit_tail_fixed_fills(PgchStep *step, int *ns, const PgchDeformCol *col,
                      int start, int run_end)
{
    uint32 off = 0;

    for (int j = start; j < run_end; ++j)
    {
        const PgchDeformCol *c = &col[j];

        off = (uint32) att_align_nominal(off, c->attalign);
        if (c->is_needed)
        {
            PgchStep *s = &step[(*ns)++];

            s->kind = PGCH_STEP_FILL_CONST;
            s->run = pick_fill_const(c->wire);
            s->disp = off;
            s->dst_base = c->dst_base;
            s->col_index = c->col_index;
            s->hops = nullptr;
            s->nhop = 0;
            s->align = 0;
        }
        off += (uint32) c->attlen;
    }
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
            s->kind = PGCH_STEP_FILL_CONST;
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
        int    run_idx = -1;        /* open fixed-run hop being folded into, or -1 */
        uint8  run_align = 1;       /* pow2 alignment of the cursor at the run's end */

        for (i = prefix_len; i < d->max_attno; ++i)
        {
            const PgchDeformCol *c = &col[i];

            if (!c->is_needed)
            {
                uint8 a = align_bytes(c->attalign);
                bool  null_hop = (nullable_group && c->nullable);

                /*
                 * Fold a run of consecutive skipped fixed-width hops into ONE hop.
                 * Once the cursor is A-aligned, a later fixed column whose alignment
                 * is already satisfied (a <= run_align) needs no realignment, so
                 *   TYPEALIGN(A,c)+L1 ... TYPEALIGN(ak,c)+Lk == TYPEALIGN(A,c)+(L1+..+Lk).
                 * The walk kernels interpret the merged hop unchanged. Varlena and
                 * (group B) nullable hops are data-dependent and break the run.
                 */
                if (c->attlen > 0 && !null_hop && run_idx >= 0 && a <= run_align)
                {
                    hop[run_idx].attlen += c->attlen;
                    run_align = run_align_after(hop[run_idx].align, (uint32) hop[run_idx].attlen);
                }
                else
                {
                    hop[nh].attlen = c->attlen;
                    hop[nh].align = a;
                    hop[nh].null_bit = null_hop ? (int16) i : -1;
                    if (c->attlen > 0 && !null_hop)
                    {
                        run_idx = nh;
                        run_align = run_align_after(a, (uint32) c->attlen);
                    }
                    else
                    {
                        run_idx = -1;   /* varlena / nullable breaks the fixed run */
                        run_align = 1;
                    }
                    nh++;
                }
                continue;
            }

            /* projected column (group B: NOT NULL): walk to it, then fill. */
            if (!c->is_string && c->attlen > 0 && nh > hop_start)
            {
                int    run_end;
                uint8  base_align;
                uint32 run_width;

                if (tail_fixed_run(col, d->max_attno, i, nullable_group,
                                   &run_end, &base_align, &run_width))
                {
                    emit_walk(step, &ns, lead, &hop[hop_start], nh - hop_start,
                              nullable_group, base_align);
                    emit_tail_fixed_fills(step, &ns, col, i, run_end);

                    lead = run_width;           /* cur is the fixed-run base */
                    hop_start = nh;
                    run_idx = -1; run_align = 1; /* new segment: close the fold */
                    i = run_end - 1;
                    continue;
                }
            }

            uint32 fill_disp = 0;

            if (!c->is_string && c->attlen > 0 && nh == hop_start)
                fill_disp = lead;
            else
                emit_walk(step, &ns, lead, &hop[hop_start], nh - hop_start, nullable_group, 0);

            PgchStep *s = &step[ns++];
            if (c->is_string)
            {
                s->kind = PGCH_STEP_FILL_STRING;
                s->run = k_fill_string;
                s->align = align_bytes(c->attalign);
                s->col_index = c->col_index;
                s->dst_base = nullptr; s->disp = 0; s->hops = nullptr; s->nhop = 0;
            }
            else if (c->is_decimal)
            {
                /* Varlena-positioned like a string, but filled fixed-width inline.
                 * The width is baked into the kernel template; dec_width is kept
                 * for diagnostics / the scalar path. */
                s->kind = PGCH_STEP_FILL_DECIMAL;
                s->run = pick_fill_decimal(c->wire);
                s->align = align_bytes(c->attalign);
                s->col_index = c->col_index;
                s->dst_base = c->dst_base;
                s->dec_scale = c->dec_scale;
                s->dec_width = (uint8) dec_width_of(c->wire);
                s->disp = 0; s->hops = nullptr; s->nhop = 0;
            }
            else
            {
                s->kind = PGCH_STEP_FILL_WALK;
                s->run = pick_fill_walk(c->wire);
                s->dst_base = c->dst_base;
                s->col_index = c->col_index;
                s->disp = fill_disp; s->align = 0; s->hops = nullptr; s->nhop = 0;
            }

            /* the fill advanced cur past this column; start a fresh segment */
            lead = 0;
            hop_start = nh;
            run_idx = -1; run_align = 1; /* close the fold */
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
