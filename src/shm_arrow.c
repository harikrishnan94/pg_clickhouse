/*-------------------------------------------------------------------------
 *
 * shm_arrow.c
 *      Apache Arrow IPC serializer for the streamed_table() TCP producer
 *      (Hot-Cold Phase 2, Branch A; decisions D-HC-0201/0202/0203/0207).
 *
 *      Builds one Arrow `ArrowSchema` (struct of N children) from the column
 *      schema, then per block builds a hand-rolled C-Data-Interface `ArrowArray`
 *      whose child buffers point ZERO-COPY at the already-deformed column
 *      buffers, views it (ArrowArrayViewSetArray), and emits standards-valid
 *      encapsulated Arrow IPC messages (Schema once; RecordBatch per block) via
 *      nanoarrow's IPC encoder. The single userspace copy is nanoarrow
 *      concatenating the viewed buffers into the contiguous IPC body (the
 *      mirror of the bespoke serializer's one scratch copy). Branch B fuses it.
 *
 *      PG-FREE on purpose (only nanoarrow + libc) -- see the header.
 *
 * Copyright (c) 2025-2026, ClickHouse, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "shm_arrow.h"

#ifdef PGCH_USE_NANOARROW

#include <stdlib.h>
#include <string.h>

/* nanoarrow is a third-party amalgamation; its headers' static inlines are not
 * clean under the extension's -Wall -Werror. Relax only across the includes. */
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmisleading-indentation"
#include "nanoarrow/nanoarrow.h"
#include "nanoarrow/nanoarrow_ipc.h"
#pragma GCC diagnostic pop

/* Arrow IPC stream end-of-stream marker: continuation 0xFFFFFFFF + length 0. */
const uint8_t SHM_ARROW_EOS_MARKER[8] = {0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00};

struct ShmArrowEncoder
{
    int                  n_fields;
    ShmWireType         *wires;          /* [n_fields] */

    struct ArrowSchema    schema;        /* root struct schema (owns children)    */
    struct ArrowArrayView view;          /* init-from-schema once, re-set per block */
    struct ArrowIpcEncoder encoder;      /* nanoarrow IPC encoder                   */
    struct ArrowBuffer    body;          /* RecordBatch body  (size reset per block)*/
    struct ArrowBuffer    message;       /* encapsulated metadata out (reset/call)  */

    /* Per-block array scaffolding, repopulated each block (no per-block malloc). */
    struct ArrowArray     root_array;
    struct ArrowArray    *child_arrays;  /* [n_fields]                              */
    struct ArrowArray   **root_children; /* [n_fields] -> &child_arrays[i]          */
    const void           *root_buf[1];   /* {NULL} (struct validity)                */
    const void          **child_buf;     /* [n_fields*3] flat buffer-pointer store  */

    /* LargeBinary needs N+1 Arrow offsets [0, end0..endN-1]; the producer carries
     * N END offsets, so we prepend a 0 into a grown-as-needed per-column scratch. */
    int64_t             **offs;          /* [n_fields]; NULL for non-string columns */
    size_t               *offs_cap;      /* [n_fields] capacity in int64 elements   */

    bool                  view_inited;
    bool                  encoder_inited;
    bool                  schema_inited;
};

static void
noop_array_release(struct ArrowArray *array)
{
    array->release = NULL;
}

/* Map a wire tag to its Arrow storage type on `child`. Date/DateTime/DateTime64
 * and Decimal32/64 ship RAW (non-semantic) per D-HC-0203/0207; Decimal128 ships
 * as FixedSizeBinary(16) (16 raw 2's-complement LE bytes == CH Decimal128). The
 * exact CH type is recovered by the consumer from the SQL schema, not here. */
static ArrowErrorCode
set_child_type(struct ArrowSchema *child, ShmWireType wire)
{
    switch (wire)
    {
        case SHM_WIRE_UINT8:    return ArrowSchemaSetType(child, NANOARROW_TYPE_UINT8);
        case SHM_WIRE_UINT16:
        case SHM_WIRE_DATE:     return ArrowSchemaSetType(child, NANOARROW_TYPE_UINT16);
        case SHM_WIRE_UINT32:
        case SHM_WIRE_DATETIME: return ArrowSchemaSetType(child, NANOARROW_TYPE_UINT32);
        case SHM_WIRE_UINT64:   return ArrowSchemaSetType(child, NANOARROW_TYPE_UINT64);
        case SHM_WIRE_INT8:     return ArrowSchemaSetType(child, NANOARROW_TYPE_INT8);
        case SHM_WIRE_INT16:    return ArrowSchemaSetType(child, NANOARROW_TYPE_INT16);
        case SHM_WIRE_INT32:
        case SHM_WIRE_DATE32:
        case SHM_WIRE_DECIMAL32: return ArrowSchemaSetType(child, NANOARROW_TYPE_INT32);
        case SHM_WIRE_INT64:
        case SHM_WIRE_DATETIME64:
        case SHM_WIRE_DECIMAL64: return ArrowSchemaSetType(child, NANOARROW_TYPE_INT64);
        case SHM_WIRE_FLOAT32:  return ArrowSchemaSetType(child, NANOARROW_TYPE_FLOAT);
        case SHM_WIRE_FLOAT64:  return ArrowSchemaSetType(child, NANOARROW_TYPE_DOUBLE);
        case SHM_WIRE_DECIMAL128:
            return ArrowSchemaSetTypeFixedSize(child, NANOARROW_TYPE_FIXED_SIZE_BINARY, 16);
        case SHM_WIRE_STRING:   return ArrowSchemaSetType(child, NANOARROW_TYPE_LARGE_BINARY);
    }
    return EINVAL;
}

static void
set_err(char *errbuf, size_t errbuf_len, const char *msg)
{
    if (errbuf != NULL && errbuf_len > 0)
    {
        strncpy(errbuf, msg, errbuf_len - 1);
        errbuf[errbuf_len - 1] = '\0';
    }
}

ShmArrowEncoder *
shm_arrow_encoder_create(const ShmArrowField *fields, int n_fields,
                         char *errbuf, size_t errbuf_len)
{
    ShmArrowEncoder *enc;
    struct ArrowError nerr;
    int i;

    if (n_fields < 0)
    {
        set_err(errbuf, errbuf_len, "negative column count");
        return NULL;
    }

    enc = (ShmArrowEncoder *) calloc(1, sizeof(*enc));
    if (enc == NULL)
        goto oom;
    enc->n_fields = n_fields;
    enc->wires         = (ShmWireType *)        calloc((size_t) n_fields + 1, sizeof(ShmWireType));
    enc->child_arrays  = (struct ArrowArray *)  calloc((size_t) n_fields + 1, sizeof(struct ArrowArray));
    enc->root_children = (struct ArrowArray **) calloc((size_t) n_fields + 1, sizeof(struct ArrowArray *));
    enc->child_buf     = (const void **)        calloc((size_t) n_fields * 3 + 3, sizeof(const void *));
    enc->offs          = (int64_t **)           calloc((size_t) n_fields + 1, sizeof(int64_t *));
    enc->offs_cap      = (size_t *)             calloc((size_t) n_fields + 1, sizeof(size_t));
    if (!enc->wires || !enc->child_arrays || !enc->root_children || !enc->child_buf ||
        !enc->offs || !enc->offs_cap)
        goto oom;

    /* Init the owning buffers FIRST so a later goto-fail -> destroy can safely
     * ArrowBufferReset them (it calls allocator.free() unconditionally). */
    ArrowBufferInit(&enc->body);
    ArrowBufferInit(&enc->message);

    /* Build the struct schema with one typed, named child per column. */
    if (ArrowSchemaInitFromType(&enc->schema, NANOARROW_TYPE_STRUCT) != NANOARROW_OK)
    {
        set_err(errbuf, errbuf_len, "ArrowSchemaInitFromType(STRUCT) failed");
        goto fail;
    }
    enc->schema_inited = true;
    if (ArrowSchemaAllocateChildren(&enc->schema, n_fields) != NANOARROW_OK)
    {
        set_err(errbuf, errbuf_len, "ArrowSchemaAllocateChildren failed");
        goto fail;
    }
    for (i = 0; i < n_fields; i++)
    {
        enc->wires[i] = fields[i].wire;
        /* AllocateChildren leaves each child released (release==NULL); init it
         * before setting its type or schema validation rejects it. */
        ArrowSchemaInit(enc->schema.children[i]);
        if (set_child_type(enc->schema.children[i], fields[i].wire) != NANOARROW_OK)
        {
            set_err(errbuf, errbuf_len, "unsupported wire type for Arrow mapping");
            goto fail;
        }
        if (ArrowSchemaSetName(enc->schema.children[i],
                               fields[i].name != NULL ? fields[i].name : "") != NANOARROW_OK)
        {
            set_err(errbuf, errbuf_len, "ArrowSchemaSetName failed");
            goto fail;
        }
    }

    if (ArrowArrayViewInitFromSchema(&enc->view, &enc->schema, &nerr) != NANOARROW_OK)
    {
        set_err(errbuf, errbuf_len, nerr.message);
        goto fail;
    }
    enc->view_inited = true;

    if (ArrowIpcEncoderInit(&enc->encoder) != NANOARROW_OK)
    {
        set_err(errbuf, errbuf_len, "ArrowIpcEncoderInit failed");
        goto fail;
    }
    enc->encoder_inited = true;

    ArrowBufferInit(&enc->body);
    ArrowBufferInit(&enc->message);
    return enc;

oom:
    set_err(errbuf, errbuf_len, "out of memory");
fail:
    shm_arrow_encoder_destroy(enc);
    return NULL;
}

void
shm_arrow_encoder_destroy(ShmArrowEncoder *enc)
{
    int i;

    if (enc == NULL)
        return;
    if (enc->encoder_inited)
        ArrowIpcEncoderReset(&enc->encoder);
    if (enc->view_inited)
        ArrowArrayViewReset(&enc->view);
    if (enc->schema_inited && enc->schema.release != NULL)
        enc->schema.release(&enc->schema);
    ArrowBufferReset(&enc->body);
    ArrowBufferReset(&enc->message);
    if (enc->offs != NULL)
        for (i = 0; i < enc->n_fields; i++)
            free(enc->offs[i]);
    free(enc->offs);
    free(enc->offs_cap);
    free(enc->child_buf);
    free(enc->root_children);
    free(enc->child_arrays);
    free(enc->wires);
    free(enc);
}

bool
shm_arrow_encode_schema(ShmArrowEncoder *enc, const uint8_t **out, size_t *out_len,
                        char *errbuf, size_t errbuf_len)
{
    struct ArrowError nerr;

    if (ArrowIpcEncoderEncodeSchema(&enc->encoder, &enc->schema, &nerr) != NANOARROW_OK)
    {
        set_err(errbuf, errbuf_len, nerr.message);
        return false;
    }
    enc->message.size_bytes = 0;   /* reuse the allocation; Finalize appends */
    if (ArrowIpcEncoderFinalizeBuffer(&enc->encoder, /*encapsulate=*/1, &enc->message) != NANOARROW_OK)
    {
        set_err(errbuf, errbuf_len, "ArrowIpcEncoderFinalizeBuffer(schema) failed");
        return false;
    }
    *out = enc->message.data;
    *out_len = (size_t) enc->message.size_bytes;
    return true;
}

static bool
ensure_offsets(ShmArrowEncoder *enc, int col, size_t n_int64, char *errbuf, size_t errbuf_len)
{
    if (enc->offs_cap[col] < n_int64)
    {
        int64_t *p = (int64_t *) realloc(enc->offs[col], n_int64 * sizeof(int64_t));
        if (p == NULL)
        {
            set_err(errbuf, errbuf_len, "out of memory (offsets scratch)");
            return false;
        }
        enc->offs[col] = p;
        enc->offs_cap[col] = n_int64;
    }
    return true;
}

bool
shm_arrow_encode_record_batch(ShmArrowEncoder *enc,
                              const ShmArrowColBuffers *cols, int n_cols, size_t row_count,
                              const uint8_t **meta, size_t *meta_len,
                              const uint8_t **body, size_t *body_len,
                              char *errbuf, size_t errbuf_len)
{
    struct ArrowError nerr;
    int i;

    if (n_cols != enc->n_fields)
    {
        set_err(errbuf, errbuf_len, "record batch column count != schema");
        return false;
    }

    for (i = 0; i < n_cols; i++)
    {
        struct ArrowArray *child = &enc->child_arrays[i];
        const void **bufs = &enc->child_buf[(size_t) i * 3];

        child->length = (int64_t) row_count;
        child->null_count = 0;
        child->offset = 0;
        child->n_children = 0;
        child->children = NULL;
        child->dictionary = NULL;
        child->private_data = NULL;
        child->release = noop_array_release;

        bufs[0] = NULL;   /* validity: non-nullable -> all valid */
        if (enc->wires[i] == SHM_WIRE_STRING)
        {
            int64_t *o;
            size_t k;

            if (!ensure_offsets(enc, i, row_count + 1, errbuf, errbuf_len))
                return false;
            o = enc->offs[i];
            o[0] = 0;
            for (k = 0; k < row_count; k++)
                o[k + 1] = (int64_t) cols[i].offsets_buf[k];
            bufs[1] = o;                  /* offsets (N+1 int64)        */
            bufs[2] = cols[i].value_buf;  /* chars (may be NULL if 0)   */
            child->n_buffers = 3;
        }
        else
        {
            bufs[1] = cols[i].value_buf;  /* contiguous LE data         */
            child->n_buffers = 2;
        }
        child->buffers = bufs;
        enc->root_children[i] = child;
    }

    enc->root_array.length = (int64_t) row_count;
    enc->root_array.null_count = 0;
    enc->root_array.offset = 0;
    enc->root_array.n_buffers = 1;
    enc->root_buf[0] = NULL;
    enc->root_array.buffers = enc->root_buf;
    enc->root_array.n_children = n_cols;
    enc->root_array.children = enc->root_children;
    enc->root_array.dictionary = NULL;
    enc->root_array.private_data = NULL;
    enc->root_array.release = noop_array_release;

    if (ArrowArrayViewSetArray(&enc->view, &enc->root_array, &nerr) != NANOARROW_OK)
    {
        set_err(errbuf, errbuf_len, nerr.message);
        return false;
    }

    enc->body.size_bytes = 0;   /* reuse allocation; encoder appends the body */
    if (ArrowIpcEncoderEncodeSimpleRecordBatch(&enc->encoder, &enc->view, &enc->body, &nerr)
        != NANOARROW_OK)
    {
        set_err(errbuf, errbuf_len, nerr.message);
        return false;
    }
    enc->message.size_bytes = 0;
    if (ArrowIpcEncoderFinalizeBuffer(&enc->encoder, /*encapsulate=*/1, &enc->message)
        != NANOARROW_OK)
    {
        set_err(errbuf, errbuf_len, "ArrowIpcEncoderFinalizeBuffer(record batch) failed");
        return false;
    }

    *meta = enc->message.data;
    *meta_len = (size_t) enc->message.size_bytes;
    *body = enc->body.data;
    *body_len = (size_t) enc->body.size_bytes;
    return true;
}

#endif /* PGCH_USE_NANOARROW */
