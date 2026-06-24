/*-------------------------------------------------------------------------
 *
 * shm_numeric.h
 *      Direct PostgreSQL numeric -> ClickHouse Decimal wire converter, shared by
 *      the C columnizer (shm_offload.c) and the C++ column-major deform kernels
 *      (shm_deform.cpp). Allocation-free, never raises a PostgreSQL error, and
 *      reads the heap varlena header directly (no detoast for the common short /
 *      4-byte-uncompressed forms).
 *
 *      We replicate the stable on-disk NumericData / NumericChoice layout in this
 *      TU -- the same technique the type map already uses for the numeric typmod
 *      bit-layout -- because the internal layout is not exported by the installed
 *      utils/numeric.h. The replicated constants are cross-checked at runtime by
 *      a text-based reference oracle under USE_ASSERT_CHECKING (see shm_offload.c)
 *      and end-to-end by the on==off byte-identical sanity oracle.
 *
 * Copyright (c) 2025-2026, ClickHouse, Inc.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_CLICKHOUSE_SHM_NUMERIC_H
#define PG_CLICKHOUSE_SHM_NUMERIC_H

#include "postgres.h"

#include "varatt.h"

#include <stdint.h>
#include <string.h>

#ifdef WORDS_BIGENDIAN
#error "pg_clickhouse SHM Decimal wire encoding assumes a little-endian host"
#endif

#ifdef __cplusplus
extern "C"
{
#endif

/*
 * On-disk numeric layout (PostgreSQL >= 14; identical through PG 18). Mirrors the
 * private definitions in src/backend/utils/adt/numeric.c. NBASE-10000 build:
 * NumericDigit is int16, four decimal digits per limb.
 */
#define PGCH_NUMERIC_SIGN_MASK          0xC000
#define PGCH_NUMERIC_POS                0x0000
#define PGCH_NUMERIC_NEG                0x4000
#define PGCH_NUMERIC_SHORT              0x8000
#define PGCH_NUMERIC_SPECIAL            0xC000

#define PGCH_NUMERIC_SHORT_SIGN_MASK    0x2000
#define PGCH_NUMERIC_SHORT_WEIGHT_SIGN  0x0040
#define PGCH_NUMERIC_SHORT_WEIGHT_MASK  0x003F

#define PGCH_NBASE                      10000
#define PGCH_DEC_DIGITS                 4       /* decimal digits per NBASE limb */

/* Verdict of the inline converter; it never raises. */
typedef enum PgchDecConv
{
    PGCH_DECCONV_OK = 0,
    PGCH_DECCONV_SPECIAL,       /* stored NaN / +Inf / -Inf: no Decimal equivalent */
    PGCH_DECCONV_TOASTED,       /* compressed-inline / external: needs detoast (rare) */
    PGCH_DECCONV_OVERFLOW       /* magnitude does not fit the signed wire width */
} PgchDecConv;

/* 10^e for e in [0,38] (10^38 < 2^127), computed once per TU. */
static inline unsigned __int128
pgch_pow10_u128(unsigned e)
{
    static unsigned __int128 tbl[39];
    static int               inited = 0;

    if (!inited)
    {
        unsigned __int128 v = 1;
        int               i;

        for (i = 0; i < 39; i++)
        {
            tbl[i] = v;
            v *= 10;
        }
        inited = 1;
    }
    return tbl[e];
}

/*
 * Convert a heap numeric varlena `vp` to its ClickHouse Decimal wire value:
 * round(value * 10^scale) as a two's-complement little-endian integer of `width`
 * bytes (4 / 8 / 16 for Decimal32 / 64 / 128), written to `out`.
 *
 * No detoast: the common 1-byte (short) and 4-byte-uncompressed varlena forms are
 * read in place; compressed-inline / external forms return PGCH_DECCONV_TOASTED so
 * the caller can detoast on the cold path. A stored NaN/Infinity returns
 * PGCH_DECCONV_SPECIAL. A magnitude that does not fit the signed `width` (only
 * possible for corrupt data, since a value stored in numeric(P,S) whose width was
 * chosen from P always fits) returns PGCH_DECCONV_OVERFLOW. Allocation-free; never
 * raises a PostgreSQL error.
 */
static inline PgchDecConv
pgch_numeric_to_decimal_wire_core(const void *vp, uint32 scale, uint32 width,
                                  char *out)
{
    /* Launder away const for the varatt macros (read-only use); no -Wcast-qual. */
    char              *p = (char *) (uintptr_t) vp;
    const char        *payload;     /* -> NumericChoice (header word + digits) */
    uint32             payload_len;  /* bytes of NumericChoice (numeric hdr + digits) */
    uint16             n_header;
    int                num_hdr_bytes;
    int                sign_neg;
    int                weight;
    int                ndigits;
    const char        *digp;
    int                i;
    unsigned __int128  mag = 0;
    unsigned __int128  limit;

    /* 1. Varlena form. EXTERNAL must be tested before SHORT (an external datum
     * also reports as a 1-byte header). */
    if (VARATT_IS_EXTERNAL(p))
        return PGCH_DECCONV_TOASTED;
    if (VARATT_IS_SHORT(p))
    {
        payload     = p + VARHDRSZ_SHORT;                 /* 1-byte varlena header */
        payload_len = (uint32) VARSIZE_SHORT(p) - VARHDRSZ_SHORT;
    }
    else if (VARATT_IS_COMPRESSED(p))
        return PGCH_DECCONV_TOASTED;
    else
    {
        payload     = p + VARHDRSZ;                       /* 4-byte varlena header */
        payload_len = (uint32) VARSIZE(p) - VARHDRSZ;
    }

    /* 2. Numeric header word (alignment-safe load). */
    memcpy(&n_header, payload, sizeof(uint16));

    if ((n_header & PGCH_NUMERIC_SIGN_MASK) == PGCH_NUMERIC_SPECIAL)
        return PGCH_DECCONV_SPECIAL;                       /* NaN / +Inf / -Inf */

    if ((n_header & 0x8000) != 0)
    {
        /* Short header: 2-byte numeric header, packed sign + dscale + weight. */
        int w;

        num_hdr_bytes = (int) sizeof(uint16);
        sign_neg      = (n_header & PGCH_NUMERIC_SHORT_SIGN_MASK) != 0;
        w             = n_header & PGCH_NUMERIC_SHORT_WEIGHT_MASK;
        if (n_header & PGCH_NUMERIC_SHORT_WEIGHT_SIGN)
            w |= ~PGCH_NUMERIC_SHORT_WEIGHT_MASK;          /* sign-extend 7-bit weight */
        weight = w;
    }
    else
    {
        /* Long header: uint16 sign/dscale + int16 weight, then digits. */
        int16 w16;

        num_hdr_bytes = (int) (sizeof(uint16) + sizeof(int16));
        sign_neg      = (n_header & PGCH_NUMERIC_SIGN_MASK) == PGCH_NUMERIC_NEG;
        memcpy(&w16, payload + sizeof(uint16), sizeof(int16));
        weight = w16;
    }

    digp    = payload + num_hdr_bytes;
    ndigits = (int) (((int) payload_len - num_hdr_bytes) / (int) sizeof(int16));

    /*
     * 3. Accumulate magnitude = round(|value| * 10^scale).
     *
     * value = sum_i d_i * NBASE^(weight - i), so the i-th base-10000 limb d_i has
     * decimal exponent e_i = DEC_DIGITS*(weight - i) + scale. For a value stored
     * in numeric(P,S) with dscale <= S (and trimmed of trailing zero limbs), at
     * most ONE limb has e_i < 0 (the last), with e_i in {-1,-2,-3}; its
     * contribution divides exactly. Every overflow is checked so corrupt on-disk
     * data cannot wrap the accumulator.
     */
    for (i = 0; i < ndigits; i++)
    {
        int16             d16;
        unsigned          d;
        int               e = PGCH_DEC_DIGITS * (weight - i) + (int) scale;
        unsigned __int128 term;

        memcpy(&d16, digp + (size_t) i * sizeof(int16), sizeof(int16));
        d = (unsigned) (uint16) d16;            /* 0 .. 9999 */

        if (e >= 0)
        {
            unsigned __int128 p10;

            if (e > 38)
                return PGCH_DECCONV_OVERFLOW;   /* beyond any 128-bit magnitude */
            p10 = pgch_pow10_u128((unsigned) e);
            if (d != 0 && p10 > (~(unsigned __int128) 0) / d)
                return PGCH_DECCONV_OVERFLOW;
            term = (unsigned __int128) d * p10;
        }
        else if (-e <= 38)
        {
            /* Trailing sub-scale limb: round to nearest (exact for valid data). */
            uint64 q = (uint64) pgch_pow10_u128((unsigned) (-e));

            term = (unsigned __int128) (((uint64) d + q / 2) / q);
        }
        else
        {
            /* A limb more than 38 decimal places below the target scale. Cannot
             * occur for a value stored in numeric(P,S) (dscale <= S <= 38, so the
             * lowest limb has e >= -3); guards corrupt on-disk data against an
             * out-of-range pow10 lookup. Its rounded contribution is 0. */
            term = 0;
        }

        if (mag > (~(unsigned __int128) 0) - term)
            return PGCH_DECCONV_OVERFLOW;
        mag += term;
    }

    /* 4. Range check against the signed wire width. */
    limit = ((unsigned __int128) 1) << (8u * width - 1);    /* 2^(bits-1) */
    if (sign_neg)
    {
        if (mag > limit)                /* most-negative is exactly -2^(bits-1) */
            return PGCH_DECCONV_OVERFLOW;
    }
    else
    {
        if (mag > limit - 1)
            return PGCH_DECCONV_OVERFLOW;
    }

    /* 5. Apply sign and write the low `width` bytes (two's-complement, little-endian). */
    {
        __int128 sv = sign_neg ? -(__int128) mag : (__int128) mag;

        memcpy(out, &sv, width);
    }
    return PGCH_DECCONV_OK;
}

#ifdef __cplusplus
}
#endif

#endif                          /* PG_CLICKHOUSE_SHM_NUMERIC_H */
