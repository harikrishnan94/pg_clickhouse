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
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include "shm_offload.h"
#include "shm_producer.h"

#include <stdint.h>
#include <string.h>

/* PostgreSQL DATE epoch is 2000-01-01; ClickHouse Date epoch is 1970-01-01. */
#define PGCH_DATE_EPOCH_DIFF (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) /* 10957 */

/* GUCs */
bool  pgch_enable_shm_offload = false;
char *pgch_local_ch_server = NULL;
int   pgch_shm_ring_depth_k = 4;
int   pgch_shm_data_region_mb = 64;
int   pgch_shm_min_rows = 100000;

PG_FUNCTION_INFO_V1(clickhouse_stream_relation);

/* --------------------------------------------------------------------- */
/* Type mapping */
/* --------------------------------------------------------------------- */

bool
pgch_pg_type_to_ch_wire(Oid pg_type, ShmOffloadColumn *out)
{
    ShmWireType wire;
    const char *ch_type;

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
    strlcpy(out->ch_type, ch_type, sizeof(out->ch_type));
    return true;
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
        default:
            ereport(ERROR, (errmsg("pg_clickhouse: column '%s' has no fixed-width writer for wire tag %d",
                                   col->name, (int) col->wire)));
    }
}

uint64
pgch_stream_relation_to_shm(Relation rel, Snapshot snapshot,
                            const ShmOffloadColumn *cols, int ncols,
                            ShmProducer *producer, size_t rows_per_block)
{
    TableScanDesc scan;
    TupleTableSlot *slot;
    MemoryContext block_cxt;
    MemoryContext old;
    ColBuf *bufs;
    ShmColumnPayload *payloads;
    size_t in_block = 0;
    uint64 total = 0;
    int c;

    if (rows_per_block == 0 || rows_per_block > (1u << 20))
        rows_per_block = 65536;

    block_cxt = AllocSetContextCreate(CurrentMemoryContext,
                                      "pg_clickhouse shm block",
                                      ALLOCSET_DEFAULT_SIZES);

    payloads = palloc0(sizeof(ShmColumnPayload) * ncols);
    bufs = palloc0(sizeof(ColBuf) * ncols);

    /* Allocate per-column staging in the block context (reset per block). */
    old = MemoryContextSwitchTo(block_cxt);
    for (c = 0; c < ncols; c++)
    {
        bufs[c].is_string = (cols[c].wire == SHM_WIRE_STRING);
        if (bufs[c].is_string)
        {
            initStringInfo(&bufs[c].chars);
            bufs[c].offsets = palloc(sizeof(uint64_t) * rows_per_block);
        }
        else
        {
            bufs[c].elem = shm_wire_fixed_width_size(cols[c].wire);
            bufs[c].fixed = palloc(bufs[c].elem * rows_per_block);
        }
    }
    MemoryContextSwitchTo(old);

    slot = table_slot_create(rel, NULL);
    scan = table_beginscan(rel, snapshot, 0, NULL);

    while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
    {
        CHECK_FOR_INTERRUPTS();
        slot_getallattrs(slot);

        for (c = 0; c < ncols; c++)
        {
            int attidx = cols[c].attno - 1;
            Datum d = slot->tts_values[attidx];

            if (slot->tts_isnull[attidx])
                ereport(ERROR,
                        (errmsg("pg_clickhouse: NULL in column '%s' is not supported by the "
                                "phase-1 streamed_table() offload", cols[c].name)));

            if (bufs[c].is_string)
            {
                text *t = DatumGetTextP(d);     /* detoasts */
                appendBinaryStringInfo(&bufs[c].chars, VARDATA(t), VARSIZE(t) - VARHDRSZ);
                bufs[c].offsets[in_block] = (uint64_t) bufs[c].chars.len;
            }
            else
            {
                write_fixed_value(&bufs[c], &cols[c], in_block, d);
            }
        }

        if (++in_block == rows_per_block)
        {
            for (c = 0; c < ncols; c++)
            {
                if (bufs[c].is_string)
                {
                    payloads[c].value_buf = bufs[c].chars.data;
                    payloads[c].value_len = (size_t) bufs[c].chars.len;
                    payloads[c].offsets_buf = bufs[c].offsets;
                    payloads[c].offsets_count = in_block;
                }
                else
                {
                    payloads[c].value_buf = bufs[c].fixed;
                    payloads[c].value_len = bufs[c].elem * in_block;
                    payloads[c].offsets_buf = NULL;
                    payloads[c].offsets_count = 0;
                }
            }
            shm_producer_publish(producer, payloads, ncols, in_block);
            total += in_block;
            in_block = 0;
            /* Reset string accumulators (fixed buffers are overwritten in place). */
            for (c = 0; c < ncols; c++)
                if (bufs[c].is_string)
                    resetStringInfo(&bufs[c].chars);
        }
    }

    /* Flush the trailing partial block. */
    if (in_block > 0)
    {
        for (c = 0; c < ncols; c++)
        {
            if (bufs[c].is_string)
            {
                payloads[c].value_buf = bufs[c].chars.data;
                payloads[c].value_len = (size_t) bufs[c].chars.len;
                payloads[c].offsets_buf = bufs[c].offsets;
                payloads[c].offsets_count = in_block;
            }
            else
            {
                payloads[c].value_buf = bufs[c].fixed;
                payloads[c].value_len = bufs[c].elem * in_block;
                payloads[c].offsets_buf = NULL;
                payloads[c].offsets_count = 0;
            }
        }
        shm_producer_publish(producer, payloads, ncols, in_block);
        total += in_block;
    }

    shm_producer_signal_eos(producer);

    table_endscan(scan);
    ExecDropSingleTupleTableSlot(slot);
    MemoryContextDelete(block_cxt);
    return total;
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
        if (!pgch_pg_type_to_ch_wire(att->atttypid, &cols[ncols]))
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
                                   CurrentMemoryContext);

    /* Stream under the active (query) snapshot for correct MVCC visibility. */
    total = pgch_stream_relation_to_shm(rel, GetActiveSnapshot(), cols, ncols,
                                        producer, (size_t) rows_per_block);

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

    /* MarkGUCPrefixReserved("pg_clickhouse") is called by the extension's
     * _PG_init in option.c after this function returns. */
}
