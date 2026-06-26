/*-------------------------------------------------------------------------
 *
 * shm_wire.c
 *      PG-free definition of the shared wire-type helpers (shm_wire.h).
 *
 *      Kept in its own translation unit (rather than a header static inline) so
 *      there is exactly one external definition: under -flto a static inline was
 *      non-deterministically out-of-lined into an undefined symbol at .so load.
 *      Compiled into the extension (the base src C-file glob) and into the
 *      standalone Arrow round-trip test (dev/hotcold/phase2/tests/).
 *
 * Copyright (c) 2025-2026, ClickHouse, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "shm_wire.h"

size_t
shm_wire_fixed_width_size(ShmWireType t)
{
    switch (t)
    {
        case SHM_WIRE_INT8:  case SHM_WIRE_UINT8:  return 1;
        case SHM_WIRE_INT16: case SHM_WIRE_UINT16: case SHM_WIRE_DATE: return 2;
        case SHM_WIRE_INT32: case SHM_WIRE_UINT32: case SHM_WIRE_FLOAT32:
        case SHM_WIRE_DATETIME: case SHM_WIRE_DATE32: case SHM_WIRE_DECIMAL32: return 4;
        case SHM_WIRE_INT64: case SHM_WIRE_UINT64: case SHM_WIRE_FLOAT64:
        case SHM_WIRE_DECIMAL64: case SHM_WIRE_DATETIME64: return 8;
        case SHM_WIRE_DECIMAL128: return 16;
        case SHM_WIRE_STRING: return 0;
    }
    return 0;
}
