/*
 * Standalone round-trip test for the producer-side Arrow IPC serializer
 * (src/shm_arrow.c, Hot-Cold Phase 2 Branch A). PG-free: builds known column
 * buffers, encodes a Schema + one RecordBatch via shm_arrow_*, then decodes the
 * resulting IPC stream with nanoarrow's stock stream reader and asserts the
 * values survive the round trip. The independent ClickHouse Arrow-C++ decode
 * (ArrowColumnToCHColumn oracle) is the consumer-side cross-check; this proves
 * the producer emits well-formed, value-correct Arrow IPC in isolation.
 *
 * Build + run (from repo root):
 *   gcc -O2 -DPGCH_USE_NANOARROW -I src/include -I src/nanoarrow/include \
 *     dev/hotcold/phase2/tests/arrow_roundtrip_test.c src/shm_arrow.c src/shm_wire.c \
 *     src/nanoarrow/src/nanoarrow.c src/nanoarrow/src/nanoarrow_ipc.c \
 *     src/nanoarrow/src/flatcc.c -o /tmp/arrow_roundtrip_test && /tmp/arrow_roundtrip_test
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#include "nanoarrow/nanoarrow.h"
#include "nanoarrow/nanoarrow_ipc.h"
#include "shm_arrow.h"

static int failures = 0;
#define CHECK(cond, ...)                                                        \
    do {                                                                        \
        if (!(cond)) {                                                          \
            fprintf(stderr, "FAIL %s:%d: ", __FILE__, __LINE__);                \
            fprintf(stderr, __VA_ARGS__);                                       \
            fprintf(stderr, "\n");                                              \
            failures++;                                                         \
        }                                                                       \
    } while (0)

int
main(void)
{
    char err[256];

    /* ---- Known 3-row block across the type families Branch A must carry. ---- */
    enum { N = 3 };

    const uint64_t id_vals[N] = {10, 20, 30};                 /* UInt64        */
    const int32_t  n_vals[N]  = {-5, 0, 7};                   /* Int32         */
    const double   f_vals[N]  = {1.5, 2.5, 3.5};              /* Float64       */
    const uint16_t dt_vals[N] = {100, 200, 300};              /* Date  (raw u16) */
    const uint32_t ts_vals[N] = {1000, 2000, 3000};           /* DateTime (raw u32) */

    /* String: "a","bb","ccc" -> chars + monotone END offsets. */
    const char     s_chars[]   = {'a', 'b', 'b', 'c', 'c', 'c'};
    const uint64_t s_offs[N]    = {1, 3, 6};                  /* END offsets    */

    /* Decimal128: 3 x 16-byte little-endian two's-complement (1, -2, 1000000). */
    unsigned char dec_buf[N * 16];
    memset(dec_buf, 0, sizeof(dec_buf));
    dec_buf[0] = 1;                                           /* row0 = 1       */
    memset(dec_buf + 16, 0xFF, 16);  dec_buf[16] = 0xFE;      /* row1 = -2      */
    { uint64_t v = 1000000; memcpy(dec_buf + 32, &v, 8); }    /* row2 = 1000000 */

    const ShmArrowField fields[] = {
        {SHM_WIRE_UINT64,     "id"},
        {SHM_WIRE_STRING,     "s"},
        {SHM_WIRE_INT32,      "n"},
        {SHM_WIRE_FLOAT64,    "f"},
        {SHM_WIRE_DATE,       "d"},
        {SHM_WIRE_DATETIME,   "t"},
        {SHM_WIRE_DECIMAL128, "dec"},
    };
    const int n_fields = (int) (sizeof(fields) / sizeof(fields[0]));

    const ShmArrowColBuffers cols[] = {
        {id_vals,  sizeof(id_vals), NULL,   0},
        {s_chars,  sizeof(s_chars), s_offs, N},
        {n_vals,   sizeof(n_vals),  NULL,   0},
        {f_vals,   sizeof(f_vals),  NULL,   0},
        {dt_vals,  sizeof(dt_vals), NULL,   0},
        {ts_vals,  sizeof(ts_vals), NULL,   0},
        {dec_buf,  sizeof(dec_buf), NULL,   0},
    };

    /* ---- Encode: Schema message + one RecordBatch message. ---- */
    ShmArrowEncoder *enc = shm_arrow_encoder_create(fields, n_fields, err, sizeof(err));
    CHECK(enc != NULL, "encoder_create: %s", err);
    if (!enc)
        return 1;

    const uint8_t *schema_msg = NULL, *meta = NULL, *body = NULL;
    size_t schema_len = 0, meta_len = 0, body_len = 0;

    /* ---- Assemble the IPC stream: schema | rb-metadata | rb-body | EOS. ----
     * The encoder reuses one output buffer, so the schema bytes must be copied
     * into the stream BEFORE encoding the record batch overwrites them (this is
     * the documented contract; the real producer sends the schema before the
     * first batch is encoded). */
    struct ArrowBuffer stream;
    ArrowBufferInit(&stream);

    CHECK(shm_arrow_encode_schema(enc, &schema_msg, &schema_len, err, sizeof(err)),
          "encode_schema: %s", err);
    CHECK(ArrowBufferAppend(&stream, schema_msg, (int64_t) schema_len) == NANOARROW_OK, "append schema");

    CHECK(shm_arrow_encode_record_batch(enc, cols, n_fields, N,
                                        &meta, &meta_len, &body, &body_len, err, sizeof(err)),
          "encode_record_batch: %s", err);
    CHECK(ArrowBufferAppend(&stream, meta, (int64_t) meta_len) == NANOARROW_OK, "append meta");
    CHECK(ArrowBufferAppend(&stream, body, (int64_t) body_len) == NANOARROW_OK, "append body");
    CHECK(ArrowBufferAppend(&stream, SHM_ARROW_EOS_MARKER, 8) == NANOARROW_OK, "append eos");

    /* ---- Decode via nanoarrow's stock stream reader (independent of the encoder path). ---- */
    struct ArrowError nerr;
    struct ArrowIpcInputStream input;
    struct ArrowArrayStream reader;
    CHECK(ArrowIpcInputStreamInitBuffer(&input, &stream) == NANOARROW_OK, "input init");
    CHECK(ArrowIpcArrayStreamReaderInit(&reader, &input, NULL) == NANOARROW_OK, "reader init");

    struct ArrowSchema out_schema;
    memset(&out_schema, 0, sizeof(out_schema));
    CHECK(reader.get_schema(&reader, &out_schema) == 0, "get_schema");
    CHECK(out_schema.n_children == n_fields, "n_children=%lld want %d",
          (long long) out_schema.n_children, n_fields);

    struct ArrowArray out_array;
    memset(&out_array, 0, sizeof(out_array));
    CHECK(reader.get_next(&reader, &out_array) == 0, "get_next");
    CHECK(out_array.release != NULL, "first batch released early (empty stream)");
    CHECK(out_array.length == N, "length=%lld want %d", (long long) out_array.length, N);

    if (out_array.release != NULL && out_schema.n_children == n_fields)
    {
        struct ArrowArrayView view;
        CHECK(ArrowArrayViewInitFromSchema(&view, &out_schema, &nerr) == NANOARROW_OK,
              "view init: %s", nerr.message);
        CHECK(ArrowArrayViewSetArray(&view, &out_array, &nerr) == NANOARROW_OK,
              "view set: %s", nerr.message);

        for (int i = 0; i < N; i++)
        {
            CHECK((uint64_t) ArrowArrayViewGetUIntUnsafe(view.children[0], i) == id_vals[i],
                  "id[%d]=%llu want %llu", i,
                  (unsigned long long) ArrowArrayViewGetUIntUnsafe(view.children[0], i),
                  (unsigned long long) id_vals[i]);

            struct ArrowStringView sv = ArrowArrayViewGetStringUnsafe(view.children[1], i);
            const char *expect[N] = {"a", "bb", "ccc"};
            CHECK(sv.size_bytes == (int64_t) strlen(expect[i]) &&
                      memcmp(sv.data, expect[i], (size_t) sv.size_bytes) == 0,
                  "s[%d]='%.*s' want '%s'", i, (int) sv.size_bytes, sv.data, expect[i]);

            CHECK(ArrowArrayViewGetIntUnsafe(view.children[2], i) == n_vals[i],
                  "n[%d] mismatch", i);
            CHECK(ArrowArrayViewGetDoubleUnsafe(view.children[3], i) == f_vals[i],
                  "f[%d] mismatch", i);
            CHECK((uint16_t) ArrowArrayViewGetUIntUnsafe(view.children[4], i) == dt_vals[i],
                  "d[%d] mismatch", i);
            CHECK((uint32_t) ArrowArrayViewGetUIntUnsafe(view.children[5], i) == ts_vals[i],
                  "t[%d] mismatch", i);

            struct ArrowBufferView dv = ArrowArrayViewGetBytesUnsafe(view.children[6], i);
            CHECK(dv.size_bytes == 16 && memcmp(dv.data.data, dec_buf + i * 16, 16) == 0,
                  "dec[%d] 16-byte mismatch", i);
        }
        ArrowArrayViewReset(&view);
    }

    if (out_array.release)
        out_array.release(&out_array);
    if (out_schema.release)
        out_schema.release(&out_schema);
    reader.release(&reader);
    shm_arrow_encoder_destroy(enc);

    if (failures == 0)
        printf("arrow_roundtrip_test: ALL %d-row round trip across 7 columns PASSED\n", N);
    else
        printf("arrow_roundtrip_test: %d FAILURES\n", failures);
    return failures == 0 ? 0 : 1;
}
