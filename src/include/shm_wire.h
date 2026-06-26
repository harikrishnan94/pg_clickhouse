/*-------------------------------------------------------------------------
 *
 * shm_wire.h
 *      PG-free wire-type vocabulary for the ClickHouse SHM/TCP block stream.
 *
 *      The `ShmWireType` tags and their fixed-width sizes are shared by the
 *      bespoke serializer (shm_producer.c) and the Apache Arrow serializer
 *      (shm_arrow.c, Hot-Cold Phase 2 Branch A). This header intentionally has
 *      NO PostgreSQL dependency (only <stddef.h>/<stdint.h>) so the Arrow
 *      module stays standalone-buildable/testable.
 *
 * Copyright (c) 2025-2026, ClickHouse, Inc.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_CLICKHOUSE_SHM_WIRE_H
#define PG_CLICKHOUSE_SHM_WIRE_H

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

/*
 * Returns the fixed-width element size in bytes, or 0 for String. Defined as a
 * single extern function in shm_wire.c (NOT static inline): under -flto the
 * inline form was non-deterministically out-of-lined into an undefined symbol.
 */
extern size_t shm_wire_fixed_width_size(ShmWireType t);

#endif /* PG_CLICKHOUSE_SHM_WIRE_H */
