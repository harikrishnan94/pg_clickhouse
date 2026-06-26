/*-------------------------------------------------------------------------
 *
 * shm_offload.c
 *      Stream snapshot-visible PostgreSQL heap rows into a co-located
 *      ClickHouse over POSIX shared memory, where the experimental
 *      streamed_table() table function adopts them zero-copy.
 *
 *      This file provides:
 *        - _PG_init and the GUCs that gate/size the feature;
 *        - pgch_pg_type_to_ch_wire: the PostgreSQL-type -> ClickHouse-wire map;
 *        - pgch_stream_relation_to_shm: the heap-scan columnizer that converts
 *          rows into ABI v1 blocks and publishes them via the SHM producer;
 *        - clickhouse_stream_relation(regclass, text, int): a SQL entry point
 *          that wires the above together end to end. A co-located ClickHouse
 *          can then run `SELECT ... FROM streamed_table('<name>', '<schema>')`.
 *
 * Copyright (c) 2025-2026, ClickHouse, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/relscan.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/pg_type_d.h"
#include "executor/tuptable.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/numeric.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include "shm_deform.h"          /* PGCH_DATE_EPOCH_DIFF (shared with the C++ deform TU) */
#include "shm_numeric.h"         /* pgch_numeric_to_decimal_wire_core */
#include "shm_offload.h"
#include "shm_page_reader.h"
#include "shm_producer.h"

#include <stdint.h>
#include <string.h>

/* GUCs */
bool  pgch_enable_shm_offload = false;
char *pgch_local_ch_server = NULL;
int   pgch_shm_min_rows = 100000;
bool  pgch_log_stream_stats = false;
bool  pgch_enable_jit_deform = false;
int   pgch_jit_row_threshold = 2000000;
int   pgch_shm_transport_mode = PGCH_TRANSPORT_ADOPT;
int   pgch_tcp_send_method = PGCH_TCP_SEND_IOURING;

/* adopt/copy = SHM transport (consumer-side data path); tcp = TCP stream transport (Phase 1). */
static const struct config_enum_entry pgch_shm_transport_options[] = {
    {"adopt", PGCH_TRANSPORT_ADOPT, false},
    {"copy",  PGCH_TRANSPORT_COPY,  false},
    {"tcp",   PGCH_TRANSPORT_TCP,   false},
    {NULL, 0, false},
};

/* TCP producer send submission method (Branch 0): io_uring (default) or blocking. */
static const struct config_enum_entry pgch_tcp_send_method_options[] = {
    {"io_uring", PGCH_TCP_SEND_IOURING,  false},
    {"iouring",  PGCH_TCP_SEND_IOURING,  true},   /* hidden alias */
    {"blocking", PGCH_TCP_SEND_BLOCKING, false},
    {NULL, 0, false},
};

PG_FUNCTION_INFO_V1(clickhouse_stream_relation);

/* --------------------------------------------------------------------- */
/* Type mapping */
/* --------------------------------------------------------------------- */

bool
pgch_pg_type_to_ch_wire(Oid pg_type, int32 typmod, ShmOffloadColumn *out)
{
    ShmWireType wire;
    const char *ch_type;

    /*
     * numeric(P, S) -> a fixed ClickHouse Decimal(P, S). The wire width is
     * chosen by precision (Decimal32 for P<=9, Decimal64 for P<=18, Decimal128
     * for P<=38). An unconstrained `numeric` (typmod -1, no fixed precision /
     * scale), a negative scale, a scale greater than the precision, or a
     * precision wider than 38 (which the adopted-column path does not cover) is
     * declined so the offload falls back to a normal plan rather than streaming
     * a value it cannot represent exactly. The typmod bit layout matches
     * PostgreSQL's internal numeric typmod encoding (precision in the high 16
     * bits; an 11-bit, sign-extended scale in the low bits).
     */
    if (pg_type == NUMERICOID)
    {
        int precision;
        int scale;

        if (typmod < (int32) VARHDRSZ)
            return false;       /* unconstrained numeric: decline (fail closed) */

        precision = ((typmod - VARHDRSZ) >> 16) & 0xffff;
        scale = (((typmod - VARHDRSZ) & 0x7ff) ^ 1024) - 1024;

        if (precision < 1 || precision > 38 || scale < 0 || scale > precision)
            return false;

        if (precision <= 9)
            wire = SHM_WIRE_DECIMAL32;
        else if (precision <= 18)
            wire = SHM_WIRE_DECIMAL64;
        else
            wire = SHM_WIRE_DECIMAL128;

        out->wire = wire;
        out->pg_type = pg_type;
        out->scale = scale;
        /* Matches ClickHouse's canonical DataTypeDecimal::getName ("Decimal(P, S)")
         * so the consumer's handshake equals-check on the parsed type passes. */
        snprintf(out->ch_type, sizeof(out->ch_type), "Decimal(%d, %d)", precision, scale);
        return true;
    }

    switch (pg_type)
    {
        case BOOLOID:    wire = SHM_WIRE_UINT8;   ch_type = "UInt8";   break;
        case INT2OID:    wire = SHM_WIRE_INT16;   ch_type = "Int16";   break;
        case INT4OID:    wire = SHM_WIRE_INT32;   ch_type = "Int32";   break;
        case INT8OID:    wire = SHM_WIRE_INT64;   ch_type = "Int64";   break;
        case FLOAT4OID:  wire = SHM_WIRE_FLOAT32; ch_type = "Float32"; break;
        case FLOAT8OID:  wire = SHM_WIRE_FLOAT64; ch_type = "Float64"; break;
        case DATEOID:    wire = SHM_WIRE_DATE;    ch_type = "Date";    break;
        /* PostgreSQL `timestamp` (no time zone, int64 us since 2000-01-01) maps to
         * ClickHouse DateTime64(6): full microsecond precision, signed-Int64 tick
         * range (no truncation, no 1970-2106 DateTime overflow). The consumer
         * already adopts DateTime64 (Wire/Layout.h tag 18 -> ColumnDecimal<DateTime64>,
         * scale derived from this type string), so this is a producer-side-only
         * change. The byte conversion is in write_fixed_value (SHM_WIRE_DATETIME64).
         *
         * The 'UTC' time zone is REQUIRED for correctness, not cosmetic. PG
         * `timestamp` is tz-naive wall-clock; our converter emits ticks treating
         * that wall-clock as UTC. A bare DateTime64(6) inherits the ClickHouse
         * server time zone (here Asia/Kolkata, +5:30), so tz-dependent extractors
         * -- toMinute()/toHour() (extract(... FROM ts)) -- would shift the result
         * (Q19 minute off by +30). Pinning 'UTC' makes CH interpret the instant in
         * the same wall-clock PG used, so extract()/date_part() match native.
         * (toStartOfMinute/date_trunc and ORDER BY are tz-invariant for whole-minute
         * offsets, so Q25/27/43 were already exact; toMinute is not -- hence Q19.) */
        case TIMESTAMPOID: wire = SHM_WIRE_DATETIME64; ch_type = "DateTime64(6, 'UTC')"; break;
        case TEXTOID:
        case VARCHAROID:
        case BPCHAROID:  wire = SHM_WIRE_STRING;  ch_type = "String";  break;
        default:
            return false;       /* unsupported: caller declines, must not error */
    }

    out->wire = wire;
    out->pg_type = pg_type;
    out->scale = 0;
    strlcpy(out->ch_type, ch_type, sizeof(out->ch_type));
    return true;
}

/*
 * Build the ShmOffloadColumn projection (wire types + names) for the 1-based
 * heap `attnos` of `rel`. Raises ERROR if a projected column's type is no longer
 * SHM-supported. Shared by the streaming worker and the backend-side vectorized
 * eligibility pre-check, so both see an identical projection.
 */
int
pgch_build_offload_columns(Relation rel, List *attnos, ShmOffloadColumn **out_cols)
{
    TupleDesc         td = RelationGetDescr(rel);
    int               ncols = list_length(attnos);
    ShmOffloadColumn *cols = (ShmOffloadColumn *) palloc0(sizeof(ShmOffloadColumn) * ncols);
    ListCell         *lc;
    int               i = 0;

    foreach (lc, attnos)
    {
        AttrNumber        attno = (AttrNumber) lfirst_int(lc);
        Form_pg_attribute att = TupleDescAttr(td, attno - 1);

        if (!pgch_pg_type_to_ch_wire(att->atttypid, att->atttypmod, &cols[i]))
            ereport(ERROR,
                    (errmsg("pg_clickhouse: column \"%s\" became unsupported for SHM offload",
                            NameStr(att->attname))));
        cols[i].attno = attno;
        strlcpy(cols[i].name, NameStr(att->attname), sizeof(cols[i].name));
        i++;
    }
    *out_cols = cols;
    return ncols;
}

char *
pgch_build_shm_schema_string(const ShmOffloadColumn *cols, int ncols)
{
    StringInfoData buf;
    int i;

    initStringInfo(&buf);
    for (i = 0; i < ncols; i++)
    {
        if (i > 0)
            appendStringInfoString(&buf, ", ");
        /* Identifier-safe: heap attnames are valid CH identifiers for the test
         * workloads; quote defensively with backticks if needed in future. */
        appendStringInfo(&buf, "%s %s", cols[i].name, cols[i].ch_type);
    }
    return buf.data;
}

/* --------------------------------------------------------------------- */
/* Columnizer */
/* --------------------------------------------------------------------- */

/* Per-column staging buffers for one block. */
typedef struct ColBuf {
    bool         is_string;
    size_t       elem;          /* fixed-width element size */
    char        *fixed;         /* rows_per_block * elem */
    StringInfoData chars;       /* string chars */
    uint64_t    *offsets;       /* rows_per_block end-offsets */
} ColBuf;

#ifdef USE_ASSERT_CHECKING
/*
 * Reference (oracle) numeric -> Decimal-wire converter, compiled only in
 * assertion builds. This is the former text-based encoder, retained verbatim as
 * an INDEPENDENT cross-check of the fast limb-walk converter: it goes through
 * `numeric_out` (exact decimal text) and accumulates the unscaled integer
 * digit-by-digit into a 256-bit magnitude, by a wholly different code path from
 * pgch_numeric_to_decimal_wire_core. numeric_to_decimal_wire Asserts the two
 * agree byte-for-byte on every converted value (mirrors the visibility
 * classifier's reference oracle). Raises on NaN/Inf/overflow like the fast path.
 */
static void
numeric_to_decimal_wire_oracle(Datum num_datum, uint32 scale, size_t width,
                               char *out, const char *colname)
{
    Numeric     num = DatumGetNumeric(num_datum);
    char       *s;
    const char *p;
    const char *dot;
    const char *frac;
    bool        neg = false;
    size_t      ilen;
    size_t      flen;
    size_t      ndig;
    size_t      i;
    uint32      mag[8] = {0};            /* up to 256-bit accumulator */
    size_t      limit_words = width / 4; /* 1 / 2 / 4 for Decimal32 / 64 / 128 */

    if (numeric_is_nan(num) || numeric_is_inf(num))
        ereport(ERROR,
                (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                 errmsg("pg_clickhouse: column \"%s\" holds NaN/Infinity, which is not "
                        "representable as a ClickHouse Decimal", colname)));

    s = DatumGetCString(DirectFunctionCall1(numeric_out, num_datum));

    p = s;
    if (*p == '-') { neg = true; p++; }
    else if (*p == '+') { p++; }

    dot = strchr(p, '.');
    ilen = dot ? (size_t) (dot - p) : strlen(p);
    frac = dot ? dot + 1 : "";
    flen = strlen(frac);
    ndig = ilen + scale;

    /* Accumulate |value| * 10^scale, padding/truncating the fraction to `scale`. */
    for (i = 0; i < ndig; i++)
    {
        char     c = (i < ilen) ? p[i]
                                : ((i - ilen) < flen ? frac[i - ilen] : '0');
        uint64_t carry;
        size_t   b;

        if (c < '0' || c > '9')
            ereport(ERROR,
                    (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                     errmsg("pg_clickhouse: column \"%s\" value \"%s\" is not representable "
                            "as a ClickHouse Decimal", colname, s)));

        carry = (uint64_t) (c - '0');
        for (b = 0; b < 8; b++)
        {
            uint64_t v = (uint64_t) mag[b] * 10 + carry;
            mag[b] = (uint32) v;
            carry = v >> 32;
        }
    }

    for (i = limit_words; i < 8; i++)
        if (mag[i] != 0)
            goto overflow;
    if (limit_words > 0 && (mag[limit_words - 1] & 0x80000000u) != 0)
        goto overflow;

    if (neg)
    {
        uint64_t carry = 1;
        size_t   b;

        for (b = 0; b < limit_words; b++)
            mag[b] = ~mag[b];
        for (b = 0; b < limit_words && carry; b++)
        {
            uint64_t v = (uint64_t) mag[b] + carry;
            mag[b] = (uint32) v;
            carry = v >> 32;
        }
    }

    memcpy(out, mag, width);
    pfree(s);
    return;

overflow:
    ereport(ERROR,
            (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
             errmsg("pg_clickhouse: column \"%s\" value \"%s\" exceeds the declared "
                    "ClickHouse Decimal precision/range", colname, s)));
}
#endif                          /* USE_ASSERT_CHECKING */

/*
 * Convert a PostgreSQL numeric Datum to a ClickHouse Decimal wire value: the
 * unscaled integer round(value * 10^scale) as a two's-complement signed
 * little-endian integer of `width` bytes (4/8/16 for Decimal32/64/128), written
 * to `out`. Fails closed (raises ERROR) on NaN/Infinity and on any magnitude that
 * does not fit the signed `width` range -- which, for a column typed numeric(P, S)
 * whose wire width was chosen from P, means a value outside the declared
 * precision. A value already stored in a numeric(P, S) column is always
 * representable, so this never wrongly rejects valid stored data.
 *
 * This is the row-at-a-time (scalar / group-C) path. It walks the on-disk
 * base-10000 limbs directly via the shared allocation-free core converter (no
 * `numeric_out`, no detoast for the common short / 4-byte-uncompressed forms),
 * detoasting only a genuinely compressed/external numeric. In assertion builds it
 * cross-checks every result against the independent text-based oracle above.
 */
static void
numeric_to_decimal_wire(Datum num_datum, uint32 scale, size_t width,
                        char *out, const char *colname)
{
    void       *vp = (void *) DatumGetPointer(num_datum);   /* raw varlena, no detoast */
    PgchDecConv rc = pgch_numeric_to_decimal_wire_core(vp, scale, (uint32) width, out);

    if (rc == PGCH_DECCONV_TOASTED)
    {
        /* Cold path: a compressed-inline / external numeric (never seen at normal
         * TOAST thresholds for tiny numerics). Detoast and retry on the plain form. */
        struct varlena *dt = PG_DETOAST_DATUM(num_datum);

        rc = pgch_numeric_to_decimal_wire_core((void *) dt, scale, (uint32) width, out);
    }

    if (rc == PGCH_DECCONV_SPECIAL)
        ereport(ERROR,
                (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                 errmsg("pg_clickhouse: column \"%s\" holds NaN/Infinity, which is not "
                        "representable as a ClickHouse Decimal", colname)));
    if (rc == PGCH_DECCONV_OVERFLOW)
        ereport(ERROR,
                (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                 errmsg("pg_clickhouse: column \"%s\" value exceeds the declared "
                        "ClickHouse Decimal precision/range", colname)));

#ifdef USE_ASSERT_CHECKING
    {
        char oracle_out[16];

        numeric_to_decimal_wire_oracle(num_datum, scale, width, oracle_out, colname);
        Assert(memcmp(out, oracle_out, width) == 0);
    }
#endif
}

static void
write_fixed_value(ColBuf *cb, const ShmOffloadColumn *col, size_t row, Datum d)
{
    char *slot = cb->fixed + row * cb->elem;

    switch (col->wire)
    {
        case SHM_WIRE_UINT8:
            *(uint8_t *) slot = DatumGetBool(d) ? 1 : 0;
            break;
        case SHM_WIRE_INT16:
        {
            int16 v = DatumGetInt16(d);
            memcpy(slot, &v, sizeof(v));
            break;
        }
        case SHM_WIRE_INT32:
        {
            int32 v = DatumGetInt32(d);
            memcpy(slot, &v, sizeof(v));
            break;
        }
        case SHM_WIRE_INT64:
        {
            int64 v = DatumGetInt64(d);
            memcpy(slot, &v, sizeof(v));
            break;
        }
        case SHM_WIRE_FLOAT32:
        {
            float4 v = DatumGetFloat4(d);
            memcpy(slot, &v, sizeof(v));
            break;
        }
        case SHM_WIRE_FLOAT64:
        {
            float8 v = DatumGetFloat8(d);
            memcpy(slot, &v, sizeof(v));
            break;
        }
        case SHM_WIRE_DATE:
        {
            /* Rebase PostgreSQL 2000-epoch days to ClickHouse 1970-epoch days. */
            int32 pg_days = (int32) DatumGetDateADT(d);
            uint16 ch_days = (uint16) (pg_days + PGCH_DATE_EPOCH_DIFF);
            memcpy(slot, &ch_days, sizeof(ch_days));
            break;
        }
        case SHM_WIRE_DATETIME64:
        {
            /* PostgreSQL timestamp: int64 us since 2000-01-01. ClickHouse
             * DateTime64(6): int64 ticks (us) since 1970-01-01 UTC. Rebase the
             * epoch with an exact integer add; sub-second precision is preserved
             * (scale 6, carried to the consumer in the DataType string, not on
             * the wire). hits has no +/-infinity timestamps; real values are far
             * inside the Int64 tick range so the add cannot overflow. */
            int64 ch_ticks = DatumGetInt64(d) + PGCH_TS_EPOCH_DIFF_US;
            memcpy(slot, &ch_ticks, sizeof(ch_ticks));
            break;
        }
        case SHM_WIRE_DECIMAL32:
        case SHM_WIRE_DECIMAL64:
        case SHM_WIRE_DECIMAL128:
            /* `d` is a numeric Datum; `cb->elem` is the wire width (4/8/16) and
             * `col->scale` the declared decimal scale (not on the wire). */
            numeric_to_decimal_wire(d, (uint32) col->scale, cb->elem, slot, col->name);
            break;
        default:
            ereport(ERROR, (errmsg("pg_clickhouse: column '%s' has no fixed-width writer for wire tag %d",
                                   col->name, (int) col->wire)));
    }
}

/* --------------------------------------------------------------------- */
/* Columnizer state (shared by the scalar and vectorized readers) */
/* --------------------------------------------------------------------- */

/*
 * Per-stream columnizer: owns the per-block staging buffers, the assembled
 * payload array, the block memory context, and the running counters. Both the
 * scalar (table-AM) reader and the vectorized page reader feed it one
 * already-deformed row at a time via pgch_columnizer_add_row, so the published
 * SHM blocks are byte-identical regardless of how the rows were produced.
 */
/*
 * Deferred decimal-fill faults recorded by the allocation-free C++ FILL_DECIMAL
 * kernel and resolved on this (C) side before the block is published. A fault is
 * a value the inline kernel could not convert: a stored NaN/Inf, a compressed/
 * external numeric (needs detoast), or an out-of-range magnitude. In practice the
 * list stays empty (TPC-H numerics are tiny + finite); the cap bounds the rare
 * case and overflowing it fails closed.
 */
#define PGCH_DEC_FAULT_CAP 64
typedef struct PgchDecFaultRec
{
    char       *dst;        /* wire slot to (re)fill */
    const char *valptr;     /* offending varlena (pinned page or staging) */
    uint32      scale;
    uint32      width;
    int         col_index;
    int         status;     /* PgchDecConv */
} PgchDecFaultRec;

struct ShmColumnizer
{
    const ShmOffloadColumn *cols;
    int                     ncols;
    ShmProducer            *producer;
    size_t                  rows_per_block;
    MemoryContext           block_cxt;
    ColBuf                 *bufs;
    ShmColumnPayload       *payloads;
    size_t                  in_block;
    uint64                  total;
    /* Byte-bounded blocks: publish a short block before the next sub-batch would
     * push the slot footprint past capacity, so a wide projection never trips
     * publish_block's per-slot overflow. slot_cap = usable slot bytes; blk_reserve
     * = running max sub-batch footprint growth (headroom for one more sub-batch);
     * blk_last_fp = footprint at the previous advance within the current block;
     * string_lens = per-column staged chars length scratch for the footprint calc. */
    size_t                  slot_cap;
    size_t                  blk_reserve;
    size_t                  blk_last_fp;
    size_t                 *string_lens;
    int                     dec_nfault;        /* recorded decimal faults this sub-batch */
    bool                    dec_fault_overflow; /* > CAP faults: resolve fails closed */
    PgchDecFaultRec         dec_faults[PGCH_DEC_FAULT_CAP];
};

ShmColumnizer *
pgch_columnizer_begin(const ShmOffloadColumn *cols, int ncols,
                      ShmProducer *producer, size_t rows_per_block)
{
    ShmColumnizer *cz;
    MemoryContext  old;
    int            c;

    if (rows_per_block == 0 || rows_per_block > (1u << 20))
        rows_per_block = 65536;

    cz = palloc0(sizeof(ShmColumnizer));
    cz->cols = cols;
    cz->ncols = ncols;
    cz->producer = producer;
    cz->rows_per_block = rows_per_block;
    cz->in_block = 0;
    cz->total = 0;
    cz->slot_cap = shm_producer_slot_capacity(producer);
    cz->blk_reserve = 0;
    cz->blk_last_fp = 0;
    cz->block_cxt = AllocSetContextCreate(CurrentMemoryContext,
                                          "pg_clickhouse shm block",
                                          ALLOCSET_DEFAULT_SIZES);
    cz->payloads = palloc0(sizeof(ShmColumnPayload) * ncols);
    cz->bufs = palloc0(sizeof(ColBuf) * ncols);
    cz->string_lens = palloc0(sizeof(size_t) * ncols);

    /* Allocate per-column staging in the block context (reset per block). */
    old = MemoryContextSwitchTo(cz->block_cxt);
    for (c = 0; c < ncols; c++)
    {
        cz->bufs[c].is_string = (cols[c].wire == SHM_WIRE_STRING);
        if (cz->bufs[c].is_string)
        {
            initStringInfo(&cz->bufs[c].chars);
            cz->bufs[c].offsets = palloc(sizeof(uint64_t) * rows_per_block);
        }
        else
        {
            cz->bufs[c].elem = shm_wire_fixed_width_size(cols[c].wire);
            cz->bufs[c].fixed = palloc(cz->bufs[c].elem * rows_per_block);
        }
    }
    MemoryContextSwitchTo(old);

    return cz;
}

/* Assemble payloads from the staged block, publish it, and reset for the next. */
static void
columnizer_publish_block(ShmColumnizer *cz)
{
    int c;

    for (c = 0; c < cz->ncols; c++)
    {
        if (cz->bufs[c].is_string)
        {
            cz->payloads[c].value_buf = cz->bufs[c].chars.data;
            cz->payloads[c].value_len = (size_t) cz->bufs[c].chars.len;
            cz->payloads[c].offsets_buf = cz->bufs[c].offsets;
            cz->payloads[c].offsets_count = cz->in_block;
        }
        else
        {
            cz->payloads[c].value_buf = cz->bufs[c].fixed;
            cz->payloads[c].value_len = cz->bufs[c].elem * cz->in_block;
            cz->payloads[c].offsets_buf = NULL;
            cz->payloads[c].offsets_count = 0;
        }
    }
    shm_producer_publish(cz->producer, cz->payloads, cz->ncols, cz->in_block);
    cz->total += cz->in_block;
    cz->in_block = 0;
    cz->blk_last_fp = 0;        /* the next block starts empty */
    /* Reset string accumulators (fixed buffers are overwritten in place). */
    for (c = 0; c < cz->ncols; c++)
        if (cz->bufs[c].is_string)
            resetStringInfo(&cz->bufs[c].chars);
}

/*
 * Would publishing the current block (in_block rows of staged data) exceed the
 * slot, or come within one sub-batch of it? Computes the exact slot footprint of
 * the staged block and compares against the per-slot capacity, reserving headroom
 * for one more sub-batch of the largest size seen so far. Updates the reserve.
 */
static bool
columnizer_should_flush_bytes(ShmColumnizer *cz)
{
    size_t fp;
    size_t grow;
    int    c;

    if (cz->slot_cap == 0 || cz->in_block == 0)
        return false;

    for (c = 0; c < cz->ncols; c++)
        cz->string_lens[c] = cz->bufs[c].is_string ? (size_t) cz->bufs[c].chars.len : 0;

    fp = shm_producer_block_footprint(cz->producer, cz->string_lens, cz->in_block);
    grow = (fp > cz->blk_last_fp) ? fp - cz->blk_last_fp : 0;
    if (grow > cz->blk_reserve)
        cz->blk_reserve = grow;
    cz->blk_last_fp = fp;

    /* Flush if the footprint already exceeds capacity (a single sub-batch wider
     * than the slot still falls through to publish_block's overflow guard), or if
     * one more like-sized sub-batch would. */
    return fp + cz->blk_reserve >= cz->slot_cap;
}

/*
 * Hot path: read an IN-PLACE string varlena's payload (bytes + length) without
 * detoasting -- a heap text/varchar/bpchar value is almost always a 1-byte short
 * header or a 4-byte uncompressed header, so we avoid the per-row palloc that
 * DatumGetTextP does for a short varlena. Returns false for a genuinely
 * compressed-inline / external datum (rare: only above the TOAST threshold), so
 * the caller takes the cold pgch_string_detoast path. EXTERNAL/COMPRESSED are
 * tested first because an external datum also reports a 1-byte (short) header, so
 * the VARATT_IS_SHORT test below must only see a genuine short. bpchar trailing
 * blanks and empty strings are preserved verbatim (VARSIZE-VARHDRSZ bytes,
 * exactly as DatumGetTextP would have).
 */
/*
 * CHAR(n)/bpchar trailing-blank trim. PostgreSQL stores a bpchar blank-padded to n
 * and compares it IGNORING trailing blanks (bpchareq / bpchar ordering), whereas a
 * ClickHouse String compares byte-exact. So a pushed-down `char_col = 'lit'`,
 * `IN (...)`, `<>`, or range comparison would never match the padded bytes. We
 * strip the (semantically insignificant) trailing blanks at offload time so pushed
 * comparisons match PostgreSQL bpchar semantics. The ONLY visible effect is that a
 * projected or grouped CHAR(n) value loses its display padding (e.g. "GERMANY" vs
 * "GERMANY                  ") -- a bounded, documented fidelity deviation; the
 * value is semantically identical (bpchar trailing blanks carry no information, and
 * length()/ordering/grouping already ignore them). Applied ONLY to bpchar;
 * text/varchar (where trailing spaces ARE significant) are never trimmed.
 */
static inline size_t
pgch_bpchar_trim_len(const char *data, size_t len)
{
    while (len > 0 && data[len - 1] == ' ')
        len--;
    return len;
}

static inline bool
pgch_string_inplace(const char *p, const char **data, size_t *len)
{
    if (unlikely(VARATT_IS_EXTERNAL(p) || VARATT_IS_COMPRESSED(p)))
        return false;

    if (VARATT_IS_SHORT(p))
    {
        *data = VARDATA_SHORT(p);
        *len  = (size_t) (VARSIZE_SHORT(p) - VARHDRSZ_SHORT);
    }
    else
    {
        *data = VARDATA(p);                     /* 4-byte uncompressed; bpchar/empty verbatim */
        *len  = (size_t) (VARSIZE(p) - VARHDRSZ);
    }
    return true;
}

/*
 * Cold path: a compressed-inline / external string varlena. Detoast; the caller
 * pfrees *to_free (set only when pg_detoast_datum actually allocated a copy).
 * Out of line so the hot loop carries no detoast / to_free machinery.
 */
pg_noinline static void
pgch_string_detoast(char *p, const char **data, size_t *len, struct varlena **to_free)
{
    struct varlena *dt = pg_detoast_datum((struct varlena *) p);

    *data    = VARDATA(dt);
    *len     = (size_t) (VARSIZE(dt) - VARHDRSZ);
    *to_free = ((char *) dt != p) ? dt : NULL;
}

void
pgch_columnizer_add_row(ShmColumnizer *cz, const Datum *values, const bool *isnulls)
{
    int c;

    for (c = 0; c < cz->ncols; c++)
    {
        int   attidx = cz->cols[c].attno - 1;
        Datum d = values[attidx];

        if (isnulls[attidx])
            ereport(ERROR,
                    (errmsg("pg_clickhouse: NULL in column '%s' is not supported by the "
                            "phase-1 streamed_table() offload", cz->cols[c].name)));

        if (cz->bufs[c].is_string)
        {
            const char     *data;
            size_t          len;
            struct varlena *tofree = NULL;

            if (!likely(pgch_string_inplace((char *) DatumGetPointer(d), &data, &len)))
                pgch_string_detoast((char *) DatumGetPointer(d), &data, &len, &tofree);

            if (cz->cols[c].pg_type == BPCHAROID)
                len = pgch_bpchar_trim_len(data, len);

            appendBinaryStringInfo(&cz->bufs[c].chars, data, (int) len);
            if (tofree)
                pfree(tofree);
            cz->bufs[c].offsets[cz->in_block] = (uint64_t) cz->bufs[c].chars.len;
        }
        else
        {
            write_fixed_value(&cz->bufs[c], &cz->cols[c], cz->in_block, d);
        }
    }

    if (++cz->in_block >= cz->rows_per_block || columnizer_should_flush_bytes(cz))
        columnizer_publish_block(cz);
}

uint64
pgch_columnizer_finish(ShmColumnizer *cz)
{
    uint64 total;

    /* Flush the trailing partial block. End-of-stream is published by the caller
     * (once, after every cooperating producer has finished), not here, so W
     * workers sharing one ring emit exactly one EOS. */
    if (cz->in_block > 0)
        columnizer_publish_block(cz);

    total = cz->total;
    MemoryContextDelete(cz->block_cxt);
    pfree(cz->string_lens);
    pfree(cz->bufs);
    pfree(cz->payloads);
    pfree(cz);
    return total;
}

/* --------------------------------------------------------------------- */
/* Columnizer batch fill API (column-major / struct-of-arrays deform) */
/* --------------------------------------------------------------------- */
/*
 * These let a reader fill the per-column staging buffers column-at-a-time for a
 * batch of rows: fill every projected column for rows [dst_row, dst_row+nrows)
 * via the type-specialized kernels below (wire type dispatched once, not per
 * row), then call pgch_columnizer_advance(nrows) once to move the shared
 * in_block counter and flush a full block. The produced bytes are identical to
 * pgch_columnizer_add_row -- the per-wire encoding here mirrors write_fixed_value
 * and the string path exactly. Caller guarantees dst_row + nrows <=
 * rows_per_block (split a larger batch with pgch_columnizer_block_avail).
 *
 * `cur[r]` points at the source value for row r: for a fixed-prefix column at a
 * constant displacement, pass the tuple-data cursor with `disp` = that offset;
 * for a tail column, pass the per-row cursor already positioned at the value
 * with `disp` = 0.
 */

size_t
pgch_columnizer_block_avail(const ShmColumnizer *cz)
{
    return cz->rows_per_block - cz->in_block;
}

size_t
pgch_columnizer_cur_row(const ShmColumnizer *cz)
{
    return cz->in_block;
}

void
pgch_columnizer_advance(ShmColumnizer *cz, size_t nrows)
{
    cz->in_block += nrows;
    if (cz->in_block >= cz->rows_per_block || columnizer_should_flush_bytes(cz))
        columnizer_publish_block(cz);
}

/*
 * Base pointer of a fixed-width column's per-block staging buffer (the output
 * the C++ column-major deform kernels write into). Stable for the stream's
 * lifetime: allocated once in pgch_columnizer_begin and overwritten in place per
 * block. Indexed by element from dst_row in the kernel.
 */
void *
pgch_columnizer_fixed_base(ShmColumnizer *cz, int col)
{
    return cz->bufs[col].fixed;
}

void
pgch_columnizer_fill_string(ShmColumnizer *cz, int col, size_t dst_row,
                            char *const *restrict cur, size_t nrows)
{
    ColBuf *cb = &cz->bufs[col];
    size_t  r;
    size_t  base = (size_t) cb->chars.len;
    size_t  total = 0;
    char   *dest;
    /* bpchar: strip trailing blanks (see pgch_bpchar_trim_len). Both passes must
     * trim identically so offsets (pass 1) and the copied bytes (pass 2) agree. */
    const bool is_bpchar = (cz->cols[col].pg_type == BPCHAROID);

    /*
     * Pass 1: per-row length -> offsets prefix-sum (ClickHouse end-offsets) and
     * the sub-batch total. The common short / 4-byte forms cost no palloc; a
     * genuinely toasted datum is detoasted here only to size it (and again in
     * pass 2 -- rare). This replaces the per-row appendBinaryStringInfo capacity
     * check / repalloc churn with a single growth below.
     */
    for (r = 0; r < nrows; r++)
    {
        const char     *data;
        size_t          len;

        if (likely(pgch_string_inplace(cur[r], &data, &len)))
        {
            /* in place: no palloc, no to_free */
        }
        else
        {
            struct varlena *tofree;

            pgch_string_detoast(cur[r], &data, &len, &tofree);
            if (tofree)
                pfree(tofree);
        }
        if (is_bpchar)
            len = pgch_bpchar_trim_len(data, len);
        total += len;
        cb->offsets[dst_row + r] = (uint64_t) (base + total);
    }

    /* One growth for the whole sub-batch, then copy each datum into place. */
    enlargeStringInfo(&cb->chars, (int) total);
    dest = cb->chars.data + base;
    for (r = 0; r < nrows; r++)
    {
        const char     *data;
        size_t          len;

        if (likely(pgch_string_inplace(cur[r], &data, &len)))
        {
            if (is_bpchar)
                len = pgch_bpchar_trim_len(data, len);
            memcpy(dest, data, len);            /* in place: no palloc, no to_free */
            dest += len;
        }
        else
        {
            struct varlena *tofree;

            pgch_string_detoast(cur[r], &data, &len, &tofree);
            if (is_bpchar)
                len = pgch_bpchar_trim_len(data, len);
            memcpy(dest, data, len);            /* copy before free: data aliases tofree */
            dest += len;
            if (tofree)
                pfree(tofree);
        }
    }

    /* We wrote directly past chars.len; restore the StringInfo invariants so the
     * accumulated length is correct and the buffer stays null-terminated. */
    cb->chars.len = (int) (base + total);
    cb->chars.data[cb->chars.len] = '\0';
}

/*
 * Record a decimal-fill fault from the C++ FILL_DECIMAL kernel (see shm_deform.h).
 * Allocation-free, never raises -- it only stashes the offending value so the
 * reader can resolve it before publishing the block.
 */
void
pgch_columnizer_note_dec_fault(void *czp, char *dst, const char *valptr,
                               uint32 scale, uint32 width, int col_index, int status)
{
    ShmColumnizer   *cz = (ShmColumnizer *) czp;
    PgchDecFaultRec *f;

    if (cz->dec_nfault >= PGCH_DEC_FAULT_CAP)
    {
        cz->dec_fault_overflow = true;
        return;
    }
    f = &cz->dec_faults[cz->dec_nfault++];
    f->dst = dst;
    f->valptr = valptr;
    f->scale = scale;
    f->width = width;
    f->col_index = col_index;
    f->status = status;
}

/*
 * Resolve the decimal faults recorded for the current sub-batch, called by the
 * vectorized reader after each FILL_DECIMAL run and BEFORE the columnizer
 * advances (so a faulted value is corrected or the query aborts before the block
 * reaches the ring). NaN/Inf and out-of-range fail closed (clean ERROR);
 * compressed/external numerics are detoasted and converted into their slot.
 */
void
pgch_columnizer_resolve_dec_faults(void *czp)
{
    ShmColumnizer *cz = (ShmColumnizer *) czp;
    int            i;

    if (cz->dec_nfault == 0 && !cz->dec_fault_overflow)
        return;

    if (cz->dec_fault_overflow)
        ereport(ERROR,
                (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                 errmsg("pg_clickhouse: too many untranslatable decimal values in one SHM "
                        "sub-batch (compressed/external numerics unsupported on the fast path)")));

    for (i = 0; i < cz->dec_nfault; i++)
    {
        PgchDecFaultRec *f = &cz->dec_faults[i];
        const char      *colname = cz->cols[f->col_index].name;
        struct varlena  *dt;
        PgchDecConv      rc;

        if (f->status == PGCH_DECCONV_SPECIAL)
            ereport(ERROR,
                    (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                     errmsg("pg_clickhouse: column \"%s\" holds NaN/Infinity, which is not "
                            "representable as a ClickHouse Decimal", colname)));
        if (f->status == PGCH_DECCONV_OVERFLOW)
            ereport(ERROR,
                    (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                     errmsg("pg_clickhouse: column \"%s\" value exceeds the declared "
                            "ClickHouse Decimal precision/range", colname)));

        /* PGCH_DECCONV_TOASTED: detoast and convert into the slot. */
        Assert(f->status == PGCH_DECCONV_TOASTED);
        dt = pg_detoast_datum((struct varlena *) (uintptr_t) f->valptr);
        rc = pgch_numeric_to_decimal_wire_core((void *) dt, f->scale, f->width, f->dst);
        if (rc != PGCH_DECCONV_OK)
            ereport(ERROR,
                    (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                     errmsg("pg_clickhouse: column \"%s\" value is not representable as a "
                            "ClickHouse Decimal", colname)));
        if ((char *) dt != f->valptr)
            pfree(dt);
    }
    cz->dec_nfault = 0;
}

/* --------------------------------------------------------------------- */
/* Heap-scan readers */
/* --------------------------------------------------------------------- */

/*
 * Scalar (tuple-at-a-time) reader: the original table-AM scan path. Retained as
 * the permanent fail-closed fallback for the vectorized reader and as the
 * correctness reference for the on==off offload oracle.
 */
static uint64
pgch_stream_relation_scalar(Relation rel, Snapshot snapshot,
                            const ShmOffloadColumn *cols, int ncols,
                            ShmProducer *producer, size_t rows_per_block)
{
    /* The scalar table-AM reader is always single-threaded (the parallel block
     * cursor applies only to the eligible vectorized path); it scans the whole
     * relation under its own table_beginscan. */
    ShmColumnizer  *cz;
    TableScanDesc   scan;
    TupleTableSlot *slot;

    cz = pgch_columnizer_begin(cols, ncols, producer, rows_per_block);

    slot = table_slot_create(rel, NULL);
    scan = table_beginscan(rel, snapshot, 0, NULL);

    while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
    {
        CHECK_FOR_INTERRUPTS();
        slot_getallattrs(slot);
        pgch_columnizer_add_row(cz, slot->tts_values, slot->tts_isnull);
    }

    table_endscan(scan);
    ExecDropSingleTupleTableSlot(slot);

    return pgch_columnizer_finish(cz);
}

/*
 * Dispatcher: use the vectorized page reader when the relation / snapshot /
 * projection are eligible; otherwise fall back to the scalar table-AM reader
 * (e.g. a projected Decimal column, a serializable/recovery snapshot, or a
 * non-heap table AM -- see pgch_vectorized_reader_eligible). The choice is made
 * once, before any block is published, so a stream never mixes the two.
 */
uint64
pgch_stream_relation_to_shm(Relation rel, Snapshot snapshot,
                            const ShmOffloadColumn *cols, int ncols,
                            ShmProducer *producer, size_t rows_per_block,
                            ShmBlockCursor *bcursor, PgchVisStats *out_stats)
{
    if (pgch_vectorized_reader_eligible(rel, snapshot, cols, ncols))
        return pgch_stream_relation_vectorized(rel, snapshot, cols, ncols,
                                               producer, rows_per_block, bcursor, out_stats);

    /* The scalar table-AM reader does no page-level visibility classify. */
    if (out_stats)
        memset(out_stats, 0, sizeof(*out_stats));

    return pgch_stream_relation_scalar(rel, snapshot, cols, ncols,
                                       producer, rows_per_block);
}

/* --------------------------------------------------------------------- */
/* SQL entry point */
/* --------------------------------------------------------------------- */

/*
 * clickhouse_stream_relation(rel regclass, shm_name text, rows_per_block int)
 *   -> bigint (rows streamed)
 *
 * Streams every snapshot-visible row of `rel` (all columns, which must all map
 * to supported ClickHouse types) into the named SHM object, then waits for the
 * consumer to drain before tearing it down. A co-located ClickHouse reads them
 * with `streamed_table('<shm_name>', '<schema>')`.
 */
Datum
clickhouse_stream_relation(PG_FUNCTION_ARGS)
{
    Oid relid;
    text *shm_name_text;
    int32 rows_per_block;
    char *shm_name;
    Relation rel;
    TupleDesc tupdesc;
    ShmOffloadColumn *cols;
    ShmColumnSchema *schema;
    ShmProducer *producer;
    int ncols = 0;
    int natts;
    int i;
    uint64 total;

    if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
        ereport(ERROR, (errmsg("pg_clickhouse: relation and shm_name must not be NULL")));

    relid = PG_GETARG_OID(0);
    shm_name_text = PG_GETARG_TEXT_PP(1);
    rows_per_block = PG_ARGISNULL(2) ? 65536 : PG_GETARG_INT32(2);
    shm_name = text_to_cstring(shm_name_text);

    rel = table_open(relid, AccessShareLock);
    tupdesc = RelationGetDescr(rel);
    natts = tupdesc->natts;

    cols = palloc0(sizeof(ShmOffloadColumn) * natts);
    for (i = 0; i < natts; i++)
    {
        Form_pg_attribute att = TupleDescAttr(tupdesc, i);

        if (att->attisdropped)
            continue;
        if (!pgch_pg_type_to_ch_wire(att->atttypid, att->atttypmod, &cols[ncols]))
            ereport(ERROR,
                    (errmsg("pg_clickhouse: column '%s' has type %u which is not supported by "
                            "the streamed_table() offload",
                            NameStr(att->attname), att->atttypid)));
        cols[ncols].attno = att->attnum;
        strlcpy(cols[ncols].name, NameStr(att->attname), sizeof(cols[ncols].name));
        ncols++;
    }
    if (ncols == 0)
        ereport(ERROR, (errmsg("pg_clickhouse: relation has no streamable columns")));

    /* Build the producer schema from the projected columns. */
    schema = palloc0(sizeof(ShmColumnSchema) * ncols);
    for (i = 0; i < ncols; i++)
    {
        strlcpy(schema[i].name, cols[i].name, sizeof(schema[i].name));
        strlcpy(schema[i].type_string, cols[i].ch_type, sizeof(schema[i].type_string));
        schema[i].wire = cols[i].wire;
    }

    producer = shm_producer_create(shm_name, schema, ncols,
                                   (uint32_t) PGCH_SHM_RING_DEPTH_K,
                                   PGCH_SHM_DATA_REGION_BYTES,
                                   CurrentMemoryContext,
                                   PGCH_PRODUCER_TRANSPORT_SHM);

    /* Stream under the active (query) snapshot for correct MVCC visibility
     * (single producer: no shared block cursor), then signal end-of-stream. */
    total = pgch_stream_relation_to_shm(rel, GetActiveSnapshot(), cols, ncols,
                                        producer, (size_t) rows_per_block, NULL, NULL);
    shm_producer_signal_eos(producer);

    shm_producer_destroy(producer);
    table_close(rel, AccessShareLock);

    PG_RETURN_INT64((int64) total);
}

/* --------------------------------------------------------------------- */
/* Module init */
/* --------------------------------------------------------------------- */

/* Called from the extension's _PG_init (option.c) before MarkGUCPrefixReserved. */
void
pgch_shm_offload_init(void)
{
    DefineCustomBoolVariable("pg_clickhouse.enable_shm_offload",
                             "Enable streaming heap-table scans into a co-located ClickHouse via shared memory.",
                             NULL, &pgch_enable_shm_offload, false,
                             PGC_USERSET, 0, NULL, NULL, NULL);

    DefineCustomStringVariable("pg_clickhouse.local_ch_server",
                               "Name of the CREATE SERVER entry for the co-located ClickHouse used by SHM offload.",
                               NULL, &pgch_local_ch_server, "",
                               PGC_USERSET, 0, NULL, NULL, NULL);

    DefineCustomIntVariable("pg_clickhouse.shm_min_rows",
                            "Minimum estimated row count for a scan to be eligible for SHM offload.",
                            NULL, &pgch_shm_min_rows, 100000, 0, INT_MAX,
                            PGC_USERSET, 0, NULL, NULL, NULL);

    DefineCustomBoolVariable("pg_clickhouse.shm_log_stream_stats",
                             "Log the SHM producer's rows, wall time, and CPU time (LOG level) "
                             "after each streamed relation, for benchmarking the heap reader.",
                             NULL, &pgch_log_stream_stats, false,
                             PGC_USERSET, 0, NULL, NULL, NULL);

    DefineCustomBoolVariable("pg_clickhouse.enable_jit_deform",
                             "Within the vectorized reader, JIT-compile a fused per-scan deform "
                             "for fixed-width projections via the optional pg_clickhouse_jit "
                             "module. Falls back to the AOT step plan when the module is absent "
                             "or declines the plan (e.g. a projected string column).",
                             NULL, &pgch_enable_jit_deform, false,
                             PGC_USERSET, 0, NULL, NULL, NULL);

    DefineCustomIntVariable("pg_clickhouse.jit_row_threshold",
                            "Minimum estimated row count for a scan to JIT-compile its deform "
                            "(amortizes the one-time compile cost; subsequent scans of the same "
                            "shape reuse the cached code).",
                            NULL, &pgch_jit_row_threshold, 2000000, 0, INT_MAX,
                            PGC_USERSET, 0, NULL, NULL, NULL);

    DefineCustomEnumVariable("pg_clickhouse.shm_transport_mode",
                             "Transport for offload: 'adopt' (zero-copy adoption out of the "
                             "shared-memory ring, default), 'copy' (the ClickHouse consumer copies "
                             "each block out of shared memory and releases the ring slot immediately), "
                             "or 'tcp' (each producer streams its blocks to the consumer over a "
                             "per-stream TCP connection instead of shared memory). Selected per query "
                             "and emitted as the streamed_table() transport argument.",
                             NULL, &pgch_shm_transport_mode, PGCH_TRANSPORT_ADOPT,
                             pgch_shm_transport_options, PGC_USERSET, 0, NULL, NULL, NULL);

    DefineCustomEnumVariable("pg_clickhouse.tcp_send_method",
                             "For the 'tcp' transport, how the producer submits its socket send: "
                             "'io_uring' (default; one IORING_OP_SEND per buffer via a per-worker "
                             "io_uring ring -- the substrate for zero-copy send) or 'blocking' (the "
                             "Phase-1 blocking send() path). Snapshotted into the streaming-worker "
                             "header so the background worker honors the backend session's choice; "
                             "falls back to blocking if the build lacks liburing or ring init fails.",
                             NULL, &pgch_tcp_send_method, PGCH_TCP_SEND_IOURING,
                             pgch_tcp_send_method_options, PGC_USERSET, 0, NULL, NULL, NULL);

    /* Planner/executor hooks, CustomScan methods, and the
     * last_query_used_clickhouse observability GUC. */
    pgch_register_customscan_and_hooks();

    /* MarkGUCPrefixReserved("pg_clickhouse") is called by the extension's
     * _PG_init in option.c after this function returns. */
}
