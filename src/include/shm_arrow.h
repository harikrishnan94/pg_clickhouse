/*-------------------------------------------------------------------------
 *
 * shm_arrow.h
 *      Apache Arrow IPC serializer for the streamed_table() TCP producer
 *      (Hot-Cold Phase 2, Branch A; decisions D-HC-0201/0202/0203/0207).
 *
 *      Emits a STANDARD Arrow IPC stream -- a Schema message, then one
 *      RecordBatch message per block -- from the already-deformed column
 *      buffers, viewed zero-copy (the only userspace copy is nanoarrow's
 *      concatenation of the buffers into the IPC body, mirroring the bespoke
 *      serializer's single scratch copy). The stock ClickHouse Arrow reader
 *      can decode the result; the bespoke TcpFrame.h wire becomes retire-able
 *      on this transport.
 *
 *      INTENTIONALLY PG-FREE (only nanoarrow + libc) so it is standalone
 *      buildable/testable -- see dev/hotcold/phase2/tests/arrow_roundtrip_test.c.
 *      The caller (shm_producer.c) adapts ShmColumnSchema/ShmColumnPayload to
 *      these structs and turns a false return into an ereport(ERROR).
 *
 * Copyright (c) 2025-2026, ClickHouse, Inc.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_CLICKHOUSE_SHM_ARROW_H
#define PG_CLICKHOUSE_SHM_ARROW_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "shm_wire.h"   /* ShmWireType + shm_wire_fixed_width_size (PG-free) */

#ifdef __cplusplus
extern "C" {
#endif

/* One column's schema: the wire type tag + the column name (NUL-terminated). */
typedef struct ShmArrowField
{
    ShmWireType wire;
    const char *name;
} ShmArrowField;

/*
 * One column's buffers for one block (field-compatible with ShmColumnPayload).
 *  - Fixed-width: value_buf points at row_count elements, value_len is the byte
 *    length; offsets_buf/offsets_count are unused.
 *  - String: value_buf is the concatenated chars buffer and value_len its byte
 *    length; offsets_buf holds row_count monotonically non-decreasing UInt64 END
 *    offsets (offsets_buf[row_count-1] == value_len).
 */
typedef struct ShmArrowColBuffers
{
    const void     *value_buf;
    size_t          value_len;
    const uint64_t *offsets_buf;
    size_t          offsets_count;
} ShmArrowColBuffers;

typedef struct ShmArrowEncoder ShmArrowEncoder;

/*
 * Create an encoder bound to the column schema (builds the Arrow struct schema
 * + the reusable array-view scaffolding once). Returns NULL on failure; if
 * errbuf != NULL it receives a message (errbuf_len bytes). The fields' name
 * pointers must outlive the encoder (the producer's schema copy does).
 */
extern ShmArrowEncoder *shm_arrow_encoder_create(const ShmArrowField *fields, int n_fields,
                                                 char *errbuf, size_t errbuf_len);

extern void shm_arrow_encoder_destroy(ShmArrowEncoder *enc);

/*
 * Encode the Arrow IPC Schema message (encapsulated: continuation + metadata
 * size + flatbuffer + pad). On success returns true and sets out and out_len to
 * an encoder-owned buffer valid until the next encode call or destroy.
 */
extern bool shm_arrow_encode_schema(ShmArrowEncoder *enc,
                                    const uint8_t **out, size_t *out_len,
                                    char *errbuf, size_t errbuf_len);

/*
 * Encode one RecordBatch from the per-column buffers (viewed zero-copy). On
 * success returns true and sets meta/meta_len to the encapsulated metadata and
 * body/body_len to the concatenated, 8-byte-padded body -- both encoder-owned
 * and valid until the next encode call or destroy. The wire message is meta
 * followed immediately by body.
 */
extern bool shm_arrow_encode_record_batch(ShmArrowEncoder *enc,
                                          const ShmArrowColBuffers *cols, int n_cols,
                                          size_t row_count,
                                          const uint8_t **meta, size_t *meta_len,
                                          const uint8_t **body, size_t *body_len,
                                          char *errbuf, size_t errbuf_len);

/* The Arrow IPC stream end-of-stream marker (continuation 0xFFFFFFFF + length 0). */
extern const uint8_t SHM_ARROW_EOS_MARKER[8];

#ifdef __cplusplus
}
#endif

#endif /* PG_CLICKHOUSE_SHM_ARROW_H */
