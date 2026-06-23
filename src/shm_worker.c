/*-------------------------------------------------------------------------
 *
 * shm_worker.c
 *      Dynamic background worker that streams a heap relation's
 *      snapshot-visible rows into a co-located ClickHouse over a bounded SHM
 *      ring, concurrently with the originating backend dispatching the
 *      streamed_table() query and draining the result.
 *
 *      The originating backend serializes its query snapshot and the projected
 *      attnos into a DSM segment, registers this worker, and waits for it to
 *      create the SHM producer (so the ClickHouse consumer can attach). The
 *      worker then runs the heap scan + columnize + ring publish loop, applying
 *      the producer's own ring backpressure; peak shared memory is bounded by
 *      the ring regardless of table size. Worker errors are reported back to the
 *      backend through a DSM status word + message so they surface as the
 *      originating query's error.
 *
 * Copyright (c) 2025-2026, ClickHouse, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/table.h"
#include "access/xact.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "pgstat.h"
#include "port/atomics.h"
#include "postmaster/bgworker.h"
#include "storage/dsm.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "utils/elog.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/resowner.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

#include "shm_offload.h"
#include "shm_producer.h"
#include "shm_worker.h"

#include <signal.h>
#include <sys/resource.h>

/* Worker lifecycle state, in the DSM status word (atomic). */
typedef enum ShmWorkerState
{
    PGCH_WS_INIT = 0,   /* registered, not yet ready */
    PGCH_WS_READY = 1,  /* SHM producer + control socket created; safe to dispatch */
    PGCH_WS_DONE = 2,   /* stream finished and producer torn down cleanly */
    PGCH_WS_ERROR = 3,  /* worker reported an error (see errmsg) */
} ShmWorkerState;

/*
 * DSM layout: this fixed header, followed by AttrNumber[ncols] at attnos_offset,
 * followed by the serialized snapshot at snapshot_offset.
 */
typedef struct ShmWorkerHeader
{
    /* inputs (backend -> worker; immutable once the worker starts) */
    Oid         database_id;
    Oid         user_id;
    Oid         heap_relid;
    int         ncols;
    int         ring_depth_k;
    Size        data_region_size;
    int         rows_per_block;
    bool        use_vectorized;      /* honor the backend session's shm_vectorized_reader GUC */
    bool        log_stream_stats;    /* honor the backend session's shm_log_stream_stats GUC */
    char        shm_name[256];
    PGPROC     *backend_proc;        /* for snapshot xmin tracking + latch wakeups */
    Size        attnos_offset;
    Size        snapshot_offset;
    Size        snapshot_len;

    /* status (worker -> backend) */
    pg_atomic_uint32 state;
    char        errmsg[1024];
} ShmWorkerHeader;

struct ShmWorkerHandle
{
    dsm_segment            *seg;
    ShmWorkerHeader        *hdr;
    BackgroundWorkerHandle *bgw;
    bool                    shut_down;
};

/* --------------------------------------------------------------------- */
/* Backend side */
/* --------------------------------------------------------------------- */

ShmWorkerHandle *
pgch_shm_worker_launch(const char *shm_name, Oid heap_relid, List *attnos,
                       Snapshot snapshot, int ring_depth_k,
                       Size data_region_size, int rows_per_block)
{
    ShmWorkerHandle *h;
    dsm_segment    *seg;
    ShmWorkerHeader *hdr;
    AttrNumber     *attno_arr;
    Size            hdr_sz;
    Size            attnos_sz;
    Size            snap_sz;
    int             ncols = list_length(attnos);
    int             i;
    ListCell       *lc;
    BackgroundWorker bgw;
    BackgroundWorkerHandle *bgwhandle = NULL;
    pid_t           pid;

    if (ncols <= 0)
        ereport(ERROR, (errmsg("pg_clickhouse: shm streaming worker needs at least one column")));
    if (strlen(shm_name) >= sizeof(hdr->shm_name))
        ereport(ERROR, (errmsg("pg_clickhouse: shm object name too long for the streaming worker")));

    hdr_sz = MAXALIGN(sizeof(ShmWorkerHeader));
    attnos_sz = MAXALIGN(sizeof(AttrNumber) * ncols);
    snap_sz = EstimateSnapshotSpace(snapshot);

    seg = dsm_create(hdr_sz + attnos_sz + snap_sz, 0);
    /* Pin the mapping so the backend's resource owner does not detach it from
     * under us on a subtransaction boundary; we detach explicitly in shutdown. */
    dsm_pin_mapping(seg);
    hdr = (ShmWorkerHeader *) dsm_segment_address(seg);

    memset(hdr, 0, sizeof(*hdr));
    hdr->database_id = MyDatabaseId;
    hdr->user_id = GetUserId();
    hdr->heap_relid = heap_relid;
    hdr->ncols = ncols;
    hdr->ring_depth_k = ring_depth_k;
    hdr->data_region_size = data_region_size;
    hdr->rows_per_block = rows_per_block;
    /* Snapshot the session GUCs here (backend side) so the worker, a fresh
     * bgworker that would otherwise see only the defaults, honors them. */
    hdr->use_vectorized = pgch_use_vectorized_reader;
    hdr->log_stream_stats = pgch_log_stream_stats;
    strlcpy(hdr->shm_name, shm_name, sizeof(hdr->shm_name));
    hdr->backend_proc = MyProc;
    hdr->attnos_offset = hdr_sz;
    hdr->snapshot_offset = hdr_sz + attnos_sz;
    hdr->snapshot_len = snap_sz;
    pg_atomic_init_u32(&hdr->state, PGCH_WS_INIT);

    attno_arr = (AttrNumber *) ((char *) hdr + hdr->attnos_offset);
    i = 0;
    foreach (lc, attnos)
        attno_arr[i++] = (AttrNumber) lfirst_int(lc);

    SerializeSnapshot(snapshot, (char *) hdr + hdr->snapshot_offset);

    memset(&bgw, 0, sizeof(bgw));
    bgw.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
    bgw.bgw_restart_time = BGW_NEVER_RESTART;
    strncpy(bgw.bgw_library_name, "pg_clickhouse", BGW_MAXLEN);
    strncpy(bgw.bgw_function_name, "pgch_shm_worker_main", BGW_MAXLEN);
    snprintf(bgw.bgw_name, BGW_MAXLEN, "pg_clickhouse shm stream %u", heap_relid);
    snprintf(bgw.bgw_type, BGW_MAXLEN, "pg_clickhouse shm stream");
    bgw.bgw_main_arg = UInt32GetDatum(dsm_segment_handle(seg));
    bgw.bgw_notify_pid = MyProcPid;

    if (!RegisterDynamicBackgroundWorker(&bgw, &bgwhandle))
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                 errmsg("pg_clickhouse: could not register SHM streaming background worker"),
                 errhint("Consider raising max_worker_processes.")));

    if (WaitForBackgroundWorkerStartup(bgwhandle, &pid) != BGWH_STARTED)
    {
        pfree(bgwhandle);
        dsm_detach(seg);
        ereport(ERROR, (errmsg("pg_clickhouse: SHM streaming background worker failed to start")));
    }

    h = (ShmWorkerHandle *) palloc0(sizeof(ShmWorkerHandle));
    h->seg = seg;
    h->hdr = hdr;
    h->bgw = bgwhandle;
    h->shut_down = false;
    return h;
}

/* Raise the worker's error (or a worker-death error) if it has failed. */
void
pgch_shm_worker_check_error(ShmWorkerHandle *h)
{
    uint32 st;
    pid_t  pid;

    if (h == NULL)
        return;

    st = pg_atomic_read_u32(&h->hdr->state);
    if (st == PGCH_WS_ERROR)
        ereport(ERROR,
                (errmsg("pg_clickhouse: shm streaming worker failed: %s", h->hdr->errmsg)));

    if (st != PGCH_WS_DONE && GetBackgroundWorkerPid(h->bgw, &pid) == BGWH_STOPPED)
    {
        /* Re-read in case the worker set the state just before exiting. */
        st = pg_atomic_read_u32(&h->hdr->state);
        if (st == PGCH_WS_ERROR)
            ereport(ERROR,
                    (errmsg("pg_clickhouse: shm streaming worker failed: %s", h->hdr->errmsg)));
        if (st != PGCH_WS_DONE)
            ereport(ERROR,
                    (errmsg("pg_clickhouse: shm streaming worker stopped unexpectedly")));
    }
}

void
pgch_shm_worker_wait_ready(ShmWorkerHandle *h)
{
    for (;;)
    {
        uint32 st = pg_atomic_read_u32(&h->hdr->state);

        if (st == PGCH_WS_READY || st == PGCH_WS_DONE)
            return;

        /* Raises on PGCH_WS_ERROR or unexpected worker death. */
        pgch_shm_worker_check_error(h);

        CHECK_FOR_INTERRUPTS();
        (void) WaitLatch(MyLatch,
                         WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                         50L, PG_WAIT_EXTENSION);
        ResetLatch(MyLatch);
    }
}

void
pgch_shm_worker_shutdown(ShmWorkerHandle *h)
{
    if (h == NULL || h->shut_down)
        return;
    h->shut_down = true;

    if (h->bgw != NULL)
    {
        /* SIGTERM if still running (the worker's CHECK_FOR_INTERRUPTS in the
         * publish / drain loops turns this into a clean FATAL that aborts its
         * transaction and unlinks the SHM object + control socket); a no-op if
         * it already exited. Then wait for it to be gone so no worker, fd, or
         * /dev/shm object outlives the query. */
        TerminateBackgroundWorker(h->bgw);
        WaitForBackgroundWorkerShutdown(h->bgw);
        pfree(h->bgw);
        h->bgw = NULL;
    }
    if (h->seg != NULL)
    {
        dsm_detach(h->seg);
        h->seg = NULL;
    }
}

/* --------------------------------------------------------------------- */
/* Worker side */
/* --------------------------------------------------------------------- */

void
pgch_shm_worker_main(Datum main_arg)
{
    dsm_segment     *seg;
    ShmWorkerHeader *hdr;

    pqsignal(SIGTERM, die);
    BackgroundWorkerUnblockSignals();

    seg = dsm_attach(DatumGetUInt32(main_arg));
    if (seg == NULL)
    {
        /* No way to report; the backend's wait will see the worker stop. */
        ereport(LOG, (errmsg("pg_clickhouse: shm streaming worker could not attach to its DSM segment")));
        return;
    }
    hdr = (ShmWorkerHeader *) dsm_segment_address(seg);

    BackgroundWorkerInitializeConnectionByOid(hdr->database_id, hdr->user_id, 0);

    StartTransactionCommand();

    PG_TRY();
    {
        Snapshot          snap;
        Relation          rel;
        TupleDesc         td;
        ShmOffloadColumn *cols;
        ShmColumnSchema  *schema;
        ShmProducer      *producer;
        AttrNumber       *attnos = (AttrNumber *) ((char *) hdr + hdr->attnos_offset);
        int               ncols = hdr->ncols;
        int               i;

        /* Run the scan under the originating query's snapshot, with the worker's
         * xmin tied to the backend's proc (mirrors a parallel worker) so the rows
         * the query sees are protected and concurrent writers cannot change the
         * offloaded result. */
        snap = RestoreSnapshot((char *) hdr + hdr->snapshot_offset);
        RestoreTransactionSnapshot(snap, hdr->backend_proc);
        PushActiveSnapshot(snap);

        rel = table_open(hdr->heap_relid, AccessShareLock);
        td = RelationGetDescr(rel);

        cols = (ShmOffloadColumn *) palloc0(sizeof(ShmOffloadColumn) * ncols);
        schema = (ShmColumnSchema *) palloc0(sizeof(ShmColumnSchema) * ncols);
        for (i = 0; i < ncols; i++)
        {
            Form_pg_attribute att = TupleDescAttr(td, attnos[i] - 1);

            if (!pgch_pg_type_to_ch_wire(att->atttypid, att->atttypmod, &cols[i]))
                ereport(ERROR,
                        (errmsg("pg_clickhouse: column \"%s\" became unsupported for SHM offload",
                                NameStr(att->attname))));
            cols[i].attno = attnos[i];
            strlcpy(cols[i].name, NameStr(att->attname), sizeof(cols[i].name));
            strlcpy(schema[i].name, NameStr(att->attname), sizeof(schema[i].name));
            strlcpy(schema[i].type_string, cols[i].ch_type, sizeof(schema[i].type_string));
            schema[i].wire = cols[i].wire;
        }

        /* Create the SHM object + control socket. Owned by the transaction
         * context so an abort (error / SIGTERM-FATAL) unlinks it via the
         * producer's reset callback. */
        producer = shm_producer_create(hdr->shm_name, schema, ncols,
                                       (uint32_t) hdr->ring_depth_k,
                                       hdr->data_region_size, CurTransactionContext);

        /* Tell the backend it may now dispatch the ClickHouse query (the consumer
         * can attach to the control socket). */
        pg_atomic_write_u32(&hdr->state, PGCH_WS_READY);
        SetLatch(&hdr->backend_proc->procLatch);

        /* Apply the backend session's vectorized-reader choice in this worker. */
        pgch_use_vectorized_reader = hdr->use_vectorized;

        /* Stream the relation into the ring (ring backpressure applies; the
         * ClickHouse consumer drains concurrently), then signal end-of-stream.
         * Optionally measure the producer in isolation: CPU time excludes the
         * ring-backpressure wait (pg_usleep), so it is the pure scan + visibility
         * + deform + columnize cost independent of consumer speed. */
        if (hdr->log_stream_stats)
        {
            struct rusage r0, r1;
            TimestampTz    w0, w1;
            uint64         rows;
            double         cpu_ms, wall_ms;

            getrusage(RUSAGE_SELF, &r0);
            w0 = GetCurrentTimestamp();
            rows = pgch_stream_relation_to_shm(rel, GetActiveSnapshot(), cols, ncols,
                                               producer, (size_t) hdr->rows_per_block);
            w1 = GetCurrentTimestamp();
            getrusage(RUSAGE_SELF, &r1);

            cpu_ms = (r1.ru_utime.tv_sec - r0.ru_utime.tv_sec) * 1000.0
                   + (r1.ru_utime.tv_usec - r0.ru_utime.tv_usec) / 1000.0
                   + (r1.ru_stime.tv_sec - r0.ru_stime.tv_sec) * 1000.0
                   + (r1.ru_stime.tv_usec - r0.ru_stime.tv_usec) / 1000.0;
            wall_ms = (double) (w1 - w0) / 1000.0;

            elog(LOG,
                 "pg_clickhouse shm stream: reader=%s rows=" UINT64_FORMAT
                 " wall=%.1fms cpu=%.1fms producer_throughput=%.2f Mrows/s(cpu)",
                 hdr->use_vectorized ? "vectorized" : "scalar", rows, wall_ms, cpu_ms,
                 cpu_ms > 0 ? (double) rows / cpu_ms / 1000.0 : 0.0);
        }
        else
            (void) pgch_stream_relation_to_shm(rel, GetActiveSnapshot(), cols, ncols,
                                               producer, (size_t) hdr->rows_per_block);

        /* Producer-outlives-consumer: wait for every retained block to release,
         * then unlink the SHM object + socket. */
        shm_producer_destroy(producer);

        table_close(rel, AccessShareLock);
        PopActiveSnapshot();

        pg_atomic_write_u32(&hdr->state, PGCH_WS_DONE);
        SetLatch(&hdr->backend_proc->procLatch);
    }
    PG_CATCH();
    {
        MemoryContext ecxt = MemoryContextSwitchTo(TopMemoryContext);
        ErrorData    *edata = CopyErrorData();

        strlcpy(hdr->errmsg, edata->message, sizeof(hdr->errmsg));
        FreeErrorData(edata);
        MemoryContextSwitchTo(ecxt);

        /* Publish the error BEFORE tearing down the producer, so the backend (which
         * may be woken either by this latch or by ClickHouse seeing the producer go
         * away) reads the real cause rather than a generic producer-death error. */
        pg_atomic_write_u32(&hdr->state, PGCH_WS_ERROR);
        SetLatch(&hdr->backend_proc->procLatch);

        EmitErrorReport();
        FlushErrorState();
        /* Abort resets CurTransactionContext -> producer reset callback unlinks the
         * SHM object + control socket, so ClickHouse stops and nothing leaks. */
        AbortCurrentTransaction();
        dsm_detach(seg);
        proc_exit(1);
    }
    PG_END_TRY();

    CommitTransactionCommand();
    dsm_detach(seg);
    proc_exit(0);
}
