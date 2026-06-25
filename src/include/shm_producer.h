/*-------------------------------------------------------------------------
 *
 * shm_producer.h
 *      Producer side of the ClickHouse SHM block-stream ABI (version 1).
 *
 *      Creates a POSIX shared-memory object, lays out the handshake / slot
 *      table / schema table / data region exactly as the ClickHouse consumer
 *      expects (see ClickHouse src/Storages/SharedMemorySource/Wire/Layout.h
 *      and docs/en/development/shm-block-stream-abi-v1.md), publishes blocks of
 *      columnar data with the release/acquire publication protocol, and serves
 *      the readiness eventfd to the consumer over a Unix-domain control socket
 *      via SCM_RIGHTS.
 *
 *      The ClickHouse-side `streamed_table('<name>', '<schema>')` table function
 *      adopts the published buffers zero-copy.
 *
 * Copyright (c) 2025-2026, ClickHouse, Inc.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_CLICKHOUSE_SHM_PRODUCER_H
#define PG_CLICKHOUSE_SHM_PRODUCER_H

#include "postgres.h"
#include "utils/palloc.h"

#include <stddef.h>
#include <stdint.h>

/*
 * Wire column type tags. Values MUST match ClickHouse's
 * SharedMemoryWire::WireColumnType (Layout.h). Only the subset the producer
 * can emit is listed here.
 */
typedef enum ShmWireType {
    SHM_WIRE_UINT64 = 1,
    SHM_WIRE_STRING = 2,
    SHM_WIRE_INT8 = 3,
    SHM_WIRE_INT16 = 4,
    SHM_WIRE_INT32 = 5,
    SHM_WIRE_INT64 = 6,
    SHM_WIRE_UINT8 = 7,
    SHM_WIRE_UINT16 = 8,
    SHM_WIRE_UINT32 = 9,
    SHM_WIRE_FLOAT32 = 10,
    SHM_WIRE_FLOAT64 = 11,
    SHM_WIRE_DATE = 12,     /* UInt16 storage */
    SHM_WIRE_DATETIME = 13, /* UInt32 storage */
    SHM_WIRE_DATE32 = 14,   /* Int32 storage  */
    /*
     * Fixed-width decimals. The on-wire value is the unscaled two's-complement
     * little-endian integer; the SCALE is carried only in the type string
     * ("Decimal(P, S)") that the ClickHouse consumer parses, never in the value
     * buffer. SHM_WIRE_DECIMAL128's value buffer must be 16-byte aligned.
     */
    SHM_WIRE_DECIMAL32 = 15,  /* Int32 storage  */
    SHM_WIRE_DECIMAL64 = 16,  /* Int64 storage  */
    SHM_WIRE_DECIMAL128 = 17, /* Int128 storage, 16-byte aligned */
    SHM_WIRE_DATETIME64 = 18, /* Int64 storage (Decimal64 ticks) */
} ShmWireType;

/* Returns the fixed-width element size in bytes, or 0 for String. */
extern size_t shm_wire_fixed_width_size(ShmWireType t);

/*
 * One column's schema entry. `name` and `type_string` are the column name and
 * the ClickHouse type string (e.g. "UInt64", "String", "Date"), each
 * NUL-terminated and at most 63 bytes. `wire` is the corresponding wire tag.
 */
typedef struct ShmColumnSchema {
    char name[64];
    char type_string[64];
    ShmWireType wire;
} ShmColumnSchema;

/*
 * One column's payload for one published block.
 *   - Fixed-width: `value_buf` points at row_count elements, each
 *     shm_wire_fixed_width_size(wire) bytes; `value_len` is the byte length;
 *     offsets_* are unused.
 *   - String: `value_buf` is the concatenated chars buffer and `value_len` is
 *     its byte length; `offsets_buf` holds row_count monotonically
 *     non-decreasing UInt64 end-offsets (offsets[row_count-1] == value_len).
 */
typedef struct ShmColumnPayload {
    const void *value_buf;
    size_t value_len;
    const uint64_t *offsets_buf;
    size_t offsets_count;
} ShmColumnPayload;

typedef struct ShmProducer ShmProducer;

/*
 * Create the SHM object and bring up the handshake + control socket. `name`
 * is the SHM object name passed to shm_open (a leading '/' is added if
 * missing). The producer is registered for cleanup on `owner_cxt` reset/delete
 * so an aborted query never leaks the /dev/shm object or the socket.
 *
 * Raises a PostgreSQL ERROR (ereport) on any setup failure.
 */
extern ShmProducer *shm_producer_create(const char *name,
                                        const ShmColumnSchema *schema, int n_columns,
                                        uint32_t ring_depth_k, size_t data_region_size,
                                        MemoryContext owner_cxt);

/*
 * Publish one block of `row_count` rows (one ShmColumnPayload per schema
 * column, in schema order). Blocks (cooperatively, honouring query cancel)
 * until a ring slot is reusable. Raises ERROR on per-slot overflow or schema
 * mismatch.
 */
extern void shm_producer_publish(ShmProducer *p,
                                 const ShmColumnPayload *payloads, int n_payloads,
                                 size_t row_count);

/* Publish the end-of-stream marker block (row_count 0, eos_marker = 1). */
extern void shm_producer_signal_eos(ShmProducer *p);

/*
 * Slot sizing helpers for byte-bounded blocks. shm_producer_slot_capacity returns
 * the usable per-slot byte budget; shm_producer_block_footprint returns the bytes a
 * block of `row_count` rows would occupy given each string column's staged chars
 * length in string_lens[] (0 for fixed columns). The columnizer publishes a short
 * block before the next sub-batch would push the footprint past the capacity, so a
 * wide projection no longer trips publish_block's per-slot overflow ERROR.
 */
extern size_t shm_producer_slot_capacity(const ShmProducer *p);
extern size_t shm_producer_block_footprint(const ShmProducer *p,
                                           const size_t *string_lens, size_t row_count);

/*
 * Block (cooperatively) until every published slot's retain_refcount is back
 * to zero, then unmap, close fds, and unlink the SHM object and socket. Safe to
 * call more than once. Also invoked automatically by the owner-context cleanup
 * callback registered in shm_producer_create.
 */
extern void shm_producer_destroy(ShmProducer *p);

/* The SHM object name actually used (with a leading '/'). */
extern const char *shm_producer_shm_name(const ShmProducer *p);

/*
 * Set the originating backend PID for liveness checks. When set, the producer's
 * ring-full wait (and drain) abort if that backend dies, so a worker cannot hang
 * forever after the backend (and thus the ClickHouse consumer) has gone away.
 */
extern void shm_producer_set_origin_pid(ShmProducer *p, int pid);

/*
 * Attach a per-worker phase stopwatch (benchmark instrumentation). When set,
 * publish_block charges the ring-full wait to PUBLISH_STALL and the slot memcpy
 * to PUBLISH, save/restoring the caller's current phase. NULL disables it (the
 * default). The pointer must outlive every publish; the reader clears it (NULL)
 * before its stack frame goes away so the post-scan EOS publish is unaffected.
 */
struct PgchPhaseTimers;
extern void shm_producer_set_phase_timers(ShmProducer *p, struct PgchPhaseTimers *t);

#endif /* PG_CLICKHOUSE_SHM_PRODUCER_H */
