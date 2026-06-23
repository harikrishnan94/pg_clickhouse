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
#include "shm_offload.h"
#include "shm_page_reader.h"
#include "shm_producer.h"

#include <stdint.h>
#include <string.h>

/* GUCs */
bool  pgch_enable_shm_offload = false;
char *pgch_local_ch_server = NULL;
int   pgch_shm_ring_depth_k = 4;
int   pgch_shm_data_region_mb = 64;
int   pgch_shm_min_rows = 100000;
bool  pgch_use_vectorized_reader = true;
bool  pgch_use_columnar_deform = true;
bool  pgch_use_vectorized_visibility = true;
bool  pgch_log_stream_stats = false;
bool  pgch_enable_jit_deform = false;
int   pgch_jit_row_threshold = 2000000;
int   pgch_shm_stream_workers = 0;

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

/*
 * Convert a PostgreSQL numeric Datum to a ClickHouse Decimal wire value: the
 * unscaled integer round(value * 10^scale) as a two's-complement signed
 * little-endian integer of `width` bytes (4/8/16 for Decimal32/64/128), written
 * to `out`. Fails closed (raises ERROR) on NaN/Infinity and on any magnitude
 * that does not fit the signed `width` range — which, for a column typed
 * numeric(P, S) whose wire width was chosen from P, means a value outside the
 * declared precision. A value already stored in a numeric(P, S) column is always
 * representable, so this never wrongly rejects valid stored data.
 *
 * The conversion goes through `numeric_out` (exact decimal text), so the result
 * is bit-exact: no float intermediate. The text is parsed digit-by-digit into a
 * 256-bit magnitude (ample headroom for the <=38-digit values we accept), then
 * range-checked and folded into the requested width.
 */
static void
numeric_to_decimal_wire(Datum num_datum, uint32 scale, size_t width,
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

    /*
     * Overflow / precision guard: the magnitude must be < 2^(8*width-1) so it
     * fits the signed `width` range with room for the sign. Require every word
     * at or above the width to be zero AND the top in-width word's sign bit to
     * be clear. (10^38 < 2^127, so a valid numeric(38, S) value always passes.)
     */
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
    cz->block_cxt = AllocSetContextCreate(CurrentMemoryContext,
                                          "pg_clickhouse shm block",
                                          ALLOCSET_DEFAULT_SIZES);
    cz->payloads = palloc0(sizeof(ShmColumnPayload) * ncols);
    cz->bufs = palloc0(sizeof(ColBuf) * ncols);

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
    /* Reset string accumulators (fixed buffers are overwritten in place). */
    for (c = 0; c < cz->ncols; c++)
        if (cz->bufs[c].is_string)
            resetStringInfo(&cz->bufs[c].chars);
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
            text *t = DatumGetTextP(d);     /* detoasts */
            appendBinaryStringInfo(&cz->bufs[c].chars, VARDATA(t), VARSIZE(t) - VARHDRSZ);
            cz->bufs[c].offsets[cz->in_block] = (uint64_t) cz->bufs[c].chars.len;
        }
        else
        {
            write_fixed_value(&cz->bufs[c], &cz->cols[c], cz->in_block, d);
        }
    }

    if (++cz->in_block == cz->rows_per_block)
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
    if (cz->in_block == cz->rows_per_block)
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

    for (r = 0; r < nrows; r++)
    {
        text *t = DatumGetTextP(PointerGetDatum(cur[r]));   /* detoasts */

        appendBinaryStringInfo(&cb->chars, VARDATA(t), VARSIZE(t) - VARHDRSZ);
        cb->offsets[dst_row + r] = (uint64_t) cb->chars.len;
    }
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
 * Dispatcher: use the vectorized page reader when enabled and the relation /
 * snapshot / projection are eligible; otherwise the scalar path. The choice is
 * made once, before any block is published, so a stream never mixes the two.
 */
uint64
pgch_stream_relation_to_shm(Relation rel, Snapshot snapshot,
                            const ShmOffloadColumn *cols, int ncols,
                            ShmProducer *producer, size_t rows_per_block,
                            ShmBlockCursor *bcursor, PgchVisStats *out_stats)
{
    if (pgch_use_vectorized_reader &&
        pgch_vectorized_reader_eligible(rel, snapshot, cols, ncols))
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
                                   (uint32_t) pgch_shm_ring_depth_k,
                                   (size_t) pgch_shm_data_region_mb * 1024 * 1024,
                                   NULL, CurrentMemoryContext);

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

    DefineCustomIntVariable("pg_clickhouse.shm_ring_depth_k",
                            "Number of ring slots in the SHM block stream.",
                            NULL, &pgch_shm_ring_depth_k, 4, 1, 256,
                            PGC_USERSET, 0, NULL, NULL, NULL);

    DefineCustomIntVariable("pg_clickhouse.shm_data_region_mb",
                            "Size of the SHM data region in MiB.",
                            NULL, &pgch_shm_data_region_mb, 64, 1, 65536,
                            PGC_USERSET, 0, NULL, NULL, NULL);

    DefineCustomIntVariable("pg_clickhouse.shm_min_rows",
                            "Minimum estimated row count for a scan to be eligible for SHM offload.",
                            NULL, &pgch_shm_min_rows, 100000, 0, INT_MAX,
                            PGC_USERSET, 0, NULL, NULL, NULL);

    DefineCustomBoolVariable("pg_clickhouse.shm_vectorized_reader",
                             "Use the page-at-a-time vectorized columnar heap reader for SHM "
                             "offload (off forces the tuple-at-a-time table-AM scan).",
                             NULL, &pgch_use_vectorized_reader, true,
                             PGC_USERSET, 0, NULL, NULL, NULL);

    DefineCustomBoolVariable("pg_clickhouse.shm_columnar_deform",
                             "Within the vectorized reader, deform a page's NULL-free tuples "
                             "column-at-a-time (struct-of-arrays) instead of row-at-a-time.",
                             NULL, &pgch_use_columnar_deform, true,
                             PGC_USERSET, 0, NULL, NULL, NULL);

    DefineCustomBoolVariable("pg_clickhouse.shm_vectorized_visibility",
                             "Within the vectorized reader, classify a not-all-visible page's "
                             "tuples with the branch-free struct-of-arrays visibility kernel "
                             "(off uses the scalar reference classifier). The visible set is "
                             "identical either way; undecided tuples always use the MVCC oracle.",
                             NULL, &pgch_use_vectorized_visibility, true,
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

    DefineCustomIntVariable("pg_clickhouse.shm_stream_workers",
                            "Number of cooperating background workers that stream a heap relation "
                            "into the shared-memory ring in parallel (the eligible vectorized "
                            "reader only). 0 = auto (scale with the relation's size, capped by "
                            "max_parallel_workers); 1 = the original single producer. The effective "
                            "value is also forced as ClickHouse max_threads for the offload query.",
                            NULL, &pgch_shm_stream_workers, 0, 0, PGCH_SHM_MAX_STREAM_WORKERS,
                            PGC_USERSET, 0, NULL, NULL, NULL);

    /* Planner/executor hooks, CustomScan methods, and the
     * last_query_used_clickhouse observability GUC. */
    pgch_register_customscan_and_hooks();

    /* MarkGUCPrefixReserved("pg_clickhouse") is called by the extension's
     * _PG_init in option.c after this function returns. */
}
