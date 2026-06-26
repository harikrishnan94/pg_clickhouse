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

#include "shm_wire.h"    /* ShmWireType + shm_wire_fixed_width_size (PG-free) */

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
 * Transport for a producer (Hot-Cold D-HC-0102/0103). SHM = the POSIX shared-memory ring +
 * control-socket eventfd handshake (the default). TCP = a per-stream TCP listener the co-located
 * ClickHouse consumer connects to; blocks are serialized frame-relative (Wire/TcpFrame.h) and sent,
 * with no SHM object / control socket / pump thread.
 */
typedef enum ShmProducerTransport
{
    PGCH_PRODUCER_TRANSPORT_SHM = 0,
    PGCH_PRODUCER_TRANSPORT_TCP = 1,    /* bespoke TcpFrame.h block bytes (Phase 1)              */
    PGCH_PRODUCER_TRANSPORT_ARROW = 2,  /* Apache Arrow IPC stream over the same TCP socket (A)  */
} ShmProducerTransport;

/*
 * Create the producer for `transport`. For SHM: create the SHM object + handshake + control socket;
 * `name` is the shm_open name (leading '/' added if missing). For TCP: bind a 127.0.0.1 listener on
 * an ephemeral port (read it back with shm_producer_tcp_port) and allocate the per-block serialize
 * scratch (sized from data_region_size); `name` is retained for diagnostics. The producer is
 * registered for cleanup on `owner_cxt` reset/delete so an aborted query never leaks the object,
 * socket, listener, or connection.
 *
 * Raises a PostgreSQL ERROR (ereport) on any setup failure.
 */
extern ShmProducer *shm_producer_create(const char *name,
                                        const ShmColumnSchema *schema, int n_columns,
                                        uint32_t ring_depth_k, size_t data_region_size,
                                        MemoryContext owner_cxt,
                                        ShmProducerTransport transport);

/*
 * For a TCP-transport producer, the ephemeral TCP port its listener bound (host 127.0.0.1). The
 * worker reports this to the backend so the emitted streamed_table('<name>','<schema>',
 * 'tcp:127.0.0.1:<port>') call points the consumer at this stream. 0 for an SHM producer.
 */
extern uint16_t shm_producer_tcp_port(const ShmProducer *p);

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

/*
 * Select the TCP producer's socket-send submission method (Hot-Cold Phase 2, Branch 0).
 * `method` is a PgchTcpSendMethod (0 = blocking send(), 1 = io_uring IORING_OP_SEND). No-op
 * for an SHM producer. io_uring is lazily initialised on the first send; if the build lacks
 * liburing or ring init fails, the producer transparently falls back to the blocking path.
 * Must be called before the first publish.
 */
extern void shm_producer_set_tcp_send_method(ShmProducer *p, int method);

/*
 * Branch-0 observability: how many logical TCP sends the producer issued via io_uring vs the
 * blocking send() path, and the total bytes sent. Lets a test/benchmark prove io_uring is
 * actually on the path (io_uring sends > 0, blocking sends == 0) rather than silently falling
 * back. Any out-param may be NULL. All zero for an SHM producer.
 */
extern void shm_producer_tcp_send_stats(const ShmProducer *p,
                                        uint64_t *iouring_sends,
                                        uint64_t *blocking_sends,
                                        uint64_t *send_bytes);

/*
 * Branch B (B-it4) observability: MSG_ZEROCOPY send accounting -- how many send(MSG_ZEROCOPY) calls
 * were issued, how many SO_EE_ORIGIN_ZEROCOPY completions were drained, and how many of those carried
 * SO_EE_CODE_ZEROCOPY_COPIED. On a loopback/NIC-less host zc_copied == zc_notifs proves the deferred
 * copy (the measured null). Any out-param may be NULL. All zero unless tcp_send_method=msg_zerocopy.
 */
extern void shm_producer_tcp_zc_stats(const ShmProducer *p,
                                      uint64_t *zc_sends,
                                      uint64_t *zc_notifs,
                                      uint64_t *zc_copied);

#endif /* PG_CLICKHOUSE_SHM_PRODUCER_H */
