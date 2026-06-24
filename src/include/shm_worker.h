/*-------------------------------------------------------------------------
 *
 * shm_worker.h
 *      Dynamic background worker that streams a heap relation's
 *      snapshot-visible rows into a co-located ClickHouse over a bounded SHM
 *      ring, concurrently with the originating backend dispatching the
 *      streamed_table() query and reading its results.
 *
 *      A single backend cannot both block-publish into a full ring and block on
 *      the ClickHouse result, so the heap scan + columnize + publish loop runs
 *      in this worker while the backend drains the result. Peak shared memory is
 *      bounded by the ring (K slots x per-slot size) regardless of table size.
 *
 * Copyright (c) 2025-2026, ClickHouse, Inc.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_CLICKHOUSE_SHM_WORKER_H
#define PG_CLICKHOUSE_SHM_WORKER_H

#include "postgres.h"

#include "fmgr.h"
#include "nodes/pg_list.h"
#include "storage/dsm.h"
#include "utils/snapmgr.h"

/* Opaque backend-side handle for a launched streaming worker. */
typedef struct ShmWorkerHandle ShmWorkerHandle;

/*
 * Launch `nworkers` (>= 1) cooperating background workers that stream
 * `heap_relid` (projecting the 1-based `attnos`, an integer List) under
 * `snapshot`, each into its OWN bounded shared ring derived from `shm_name`
 * (fixed geometry: PGCH_SHM_RING_DEPTH_K slots over PGCH_SHM_DATA_REGION_BYTES,
 * PGCH_SHM_ROWS_PER_BLOCK rows per block). The W workers cooperatively cover the
 * relation's blocks under the one shared snapshot. `nworkers == 1` reproduces
 * the single-producer path.
 *
 * Returns a handle owning all W workers + the shared DSM. Raises ERROR (after
 * reaping any already-started workers) if a worker cannot be registered or
 * started. The workers have not necessarily created/attached the SHM object yet
 * -- call pgch_shm_worker_wait_ready before dispatching the ClickHouse query.
 */
extern ShmWorkerHandle *pgch_shm_worker_launch(const char *shm_name,
                                               Oid heap_relid, List *attnos,
                                               Snapshot snapshot,
                                               int nworkers);

/*
 * Block until the worker has created the SHM object + control socket (so the
 * ClickHouse consumer can attach), the worker has finished, or the worker has
 * died/reported an error. Raises the worker's error (or a worker-death error)
 * rather than returning in the failure cases.
 */
extern void pgch_shm_worker_wait_ready(ShmWorkerHandle *h);

/*
 * If the worker has reported an error (or died unexpectedly), raise it. Cheap
 * to call between result rows so a worker failure surfaces as the originating
 * query's error rather than an opaque ClickHouse producer-death error.
 */
extern void pgch_shm_worker_check_error(ShmWorkerHandle *h);

/*
 * Terminate the worker if still running, wait for it to exit, and detach the
 * DSM segment. Idempotent. Used on both normal completion and the error /
 * cancellation teardown path; guarantees no leaked worker, fds, /dev/shm object,
 * or control socket.
 */
extern void pgch_shm_worker_shutdown(ShmWorkerHandle *h);

/* Background worker entry point (resolved via bgw_library_name/bgw_function_name). */
PGDLLEXPORT void pgch_shm_worker_main(Datum main_arg);

#endif /* PG_CLICKHOUSE_SHM_WORKER_H */
