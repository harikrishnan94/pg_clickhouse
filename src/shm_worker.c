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
 * Per-worker status word + error buffer (worker -> backend). With W cooperating
 * workers there is one of these per worker, in the shared DSM, so the backend can
 * tell which worker is ready / done / failed.
 */
typedef struct ShmWorkerSlot
{
    pg_atomic_uint32 state;          /* ShmWorkerState */
    char             errmsg[1024];
} ShmWorkerSlot;

/*
 * Cross-process coordination shared by all W workers (in the DSM). The only
 * shared mutable state is the heap-block work allocator: each worker writes its
 * OWN private ring, so there is no shared publish cursor or end-of-stream
 * coordination -- every worker independently signals EOS on its own ring and the
 * ClickHouse consumer reads the W rings as W parallel sources.
 */
typedef struct ShmWorkerCoord
{
    ShmBlockCursor   block;          /* shared heap-block allocator (work-stealing) */
} ShmWorkerCoord;

/*
 * DSM layout: this fixed header, then AttrNumber[ncols] at attnos_offset, then
 * the serialized snapshot at snapshot_offset, then one ShmWorkerCoord at
 * coord_offset, then ShmWorkerSlot[nworkers] at wslot_offset. All W workers
 * attach the SAME segment; the originating backend's PGPROC pins the snapshot.
 * The header's shm_name is the BASE ring name; worker w owns ring <base>_<w>.
 */
typedef struct ShmWorkerHeader
{
    /* inputs (backend -> worker; immutable once the worker starts) */
    Oid         database_id;
    Oid         user_id;
    Oid         heap_relid;
    int         ncols;
    int         nworkers;            /* number of cooperating streaming workers (W) */
    bool        log_stream_stats;    /* honor the backend session's shm_log_stream_stats GUC */
    bool        enable_jit_deform;   /* honor the backend session's enable_jit_deform GUC */
    int         jit_row_threshold;   /* honor the backend session's jit_row_threshold GUC */
    char        shm_name[256];
    PGPROC     *backend_proc;        /* for snapshot xmin tracking + latch wakeups */
    int         backend_pid;         /* originating backend PID, for liveness checks */
    Size        attnos_offset;
    Size        snapshot_offset;
    Size        snapshot_len;
    Size        coord_offset;        /* ShmWorkerCoord */
    Size        wslot_offset;        /* ShmWorkerSlot[nworkers] */
} ShmWorkerHeader;

static inline ShmWorkerCoord *
coord_of(ShmWorkerHeader *hdr)
{
    return (ShmWorkerCoord *) ((char *) hdr + hdr->coord_offset);
}

static inline ShmWorkerSlot *
wslot_of(ShmWorkerHeader *hdr, int i)
{
    return &((ShmWorkerSlot *) ((char *) hdr + hdr->wslot_offset))[i];
}

struct ShmWorkerHandle
{
    dsm_segment            *seg;
    ShmWorkerHeader        *hdr;
    int                     nworkers;
    BackgroundWorkerHandle **bgw;    /* [nworkers] */
    bool                    shut_down;
};

/* --------------------------------------------------------------------- */
/* Backend side */
/* --------------------------------------------------------------------- */

ShmWorkerHandle *
pgch_shm_worker_register(const char *shm_name, Oid heap_relid, List *attnos,
                         Snapshot snapshot, int nworkers)
{
    ShmWorkerHandle *h;
    dsm_segment    *seg;
    ShmWorkerHeader *hdr;
    ShmWorkerCoord *coord;
    AttrNumber     *attno_arr;
    Size            hdr_sz;
    Size            attnos_sz;
    Size            snap_sz;
    Size            coord_sz;
    int             ncols = list_length(attnos);
    int             i;
    int             w;
    int             launched = 0;
    ListCell       *lc;

    if (ncols <= 0)
        ereport(ERROR, (errmsg("pg_clickhouse: shm streaming worker needs at least one column")));
    if (strlen(shm_name) >= sizeof(hdr->shm_name))
        ereport(ERROR, (errmsg("pg_clickhouse: shm object name too long for the streaming worker")));
    if (nworkers < 1)
        nworkers = 1;

    hdr_sz = MAXALIGN(sizeof(ShmWorkerHeader));
    attnos_sz = MAXALIGN(sizeof(AttrNumber) * ncols);
    snap_sz = MAXALIGN(EstimateSnapshotSpace(snapshot));
    coord_sz = MAXALIGN(sizeof(ShmWorkerCoord));

    seg = dsm_create(hdr_sz + attnos_sz + snap_sz + coord_sz
                     + (Size) nworkers * MAXALIGN(sizeof(ShmWorkerSlot)), 0);
    /* Pin the mapping so the backend's resource owner does not detach it from
     * under us on a subtransaction boundary; we detach explicitly in shutdown. */
    dsm_pin_mapping(seg);
    hdr = (ShmWorkerHeader *) dsm_segment_address(seg);

    memset(hdr, 0, sizeof(*hdr));
    hdr->database_id = MyDatabaseId;
    hdr->user_id = GetUserId();
    hdr->heap_relid = heap_relid;
    hdr->ncols = ncols;
    hdr->nworkers = nworkers;
    /* Snapshot the session GUCs here (backend side) so the worker, a fresh
     * bgworker that would otherwise see only the defaults, honors them. */
    hdr->log_stream_stats = pgch_log_stream_stats;
    hdr->enable_jit_deform = pgch_enable_jit_deform;
    hdr->jit_row_threshold = pgch_jit_row_threshold;
    strlcpy(hdr->shm_name, shm_name, sizeof(hdr->shm_name));
    hdr->backend_proc = MyProc;
    hdr->backend_pid = MyProcPid;
    hdr->attnos_offset = hdr_sz;
    hdr->snapshot_offset = hdr_sz + attnos_sz;
    hdr->snapshot_len = snap_sz;
    hdr->coord_offset = hdr_sz + attnos_sz + snap_sz;
    hdr->wslot_offset = hdr->coord_offset + coord_sz;

    attno_arr = (AttrNumber *) ((char *) hdr + hdr->attnos_offset);
    i = 0;
    foreach (lc, attnos)
        attno_arr[i++] = (AttrNumber) lfirst_int(lc);

    SerializeSnapshot(snapshot, (char *) hdr + hdr->snapshot_offset);

    /* Initialize the shared coordination before any worker can start. */
    coord = coord_of(hdr);
    pg_atomic_init_u32(&coord->block.next_block, 0);
    for (w = 0; w < nworkers; w++)
        pg_atomic_init_u32(&wslot_of(hdr, w)->state, PGCH_WS_INIT);

    h = (ShmWorkerHandle *) palloc0(sizeof(ShmWorkerHandle));
    h->seg = seg;
    h->hdr = hdr;
    h->nworkers = nworkers;
    h->bgw = (BackgroundWorkerHandle **) palloc0(sizeof(BackgroundWorkerHandle *) * nworkers);
    h->shut_down = false;

    /*
     * Spawn in two passes so the postmaster forks all W workers concurrently.
     * Pass 1 registers every worker -- RegisterDynamicBackgroundWorker only
     * enqueues a slot and signals the postmaster, it does not block -- and pass
     * 2 waits for them to come up. The previous register-then-wait-per-worker
     * loop serialized startup (~W x single-worker fork+InitPostgres latency on
     * the critical path, before any streaming begins); registering all of them
     * first lets the postmaster fork them in parallel, so the wait pass blocks
     * for roughly the slowest single worker (~1x) rather than the sum.
     */
    for (w = 0; w < nworkers; w++)
    {
        BackgroundWorker bgw;
        BackgroundWorkerHandle *bgwhandle = NULL;

        memset(&bgw, 0, sizeof(bgw));
        bgw.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
        bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
        bgw.bgw_restart_time = BGW_NEVER_RESTART;
        strncpy(bgw.bgw_library_name, "pg_clickhouse", BGW_MAXLEN);
        strncpy(bgw.bgw_function_name, "pgch_shm_worker_main", BGW_MAXLEN);
        snprintf(bgw.bgw_name, BGW_MAXLEN, "pg_clickhouse shm stream %u/%d", heap_relid, w);
        snprintf(bgw.bgw_type, BGW_MAXLEN, "pg_clickhouse shm stream");
        bgw.bgw_main_arg = UInt32GetDatum(dsm_segment_handle(seg));
        bgw.bgw_notify_pid = MyProcPid;
        /* Per-worker index (owner == 0) carried in bgw_extra. */
        memcpy(bgw.bgw_extra, &w, sizeof(w));

        if (!RegisterDynamicBackgroundWorker(&bgw, &bgwhandle))
        {
            /* Could not register the full set; reap whatever started and fail
             * (fail-closed: do not silently run with fewer producers than the
             * active_workers count, which would hang the consumer at EOS). */
            h->shut_down = false;
            pgch_shm_worker_shutdown(h);
            ereport(ERROR,
                    (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                     errmsg("pg_clickhouse: could not register SHM streaming background worker %d/%d",
                            w, nworkers),
                     errhint("Consider raising max_worker_processes.")));
        }
        h->bgw[w] = bgwhandle;
        launched++;
    }

    (void) launched;
    return h;
}

/* Phase 2 of the launch: block until every registered worker has started. On
 * failure, reap this handle's workers and raise (the caller reaps other sources
 * via its abort path). Separated from registration so a multi-source scan can
 * register all sources first and let the postmaster fork them concurrently. */
void
pgch_shm_worker_wait_started(ShmWorkerHandle *h)
{
    int w;

    for (w = 0; w < h->nworkers; w++)
    {
        pid_t pid;

        if (WaitForBackgroundWorkerStartup(h->bgw[w], &pid) != BGWH_STARTED)
        {
            pgch_shm_worker_shutdown(h);
            ereport(ERROR, (errmsg("pg_clickhouse: SHM streaming background worker %d/%d failed to start",
                                   w, h->nworkers)));
        }
    }
}

/* Convenience: register the workers and wait for them to start (single source). */
ShmWorkerHandle *
pgch_shm_worker_launch(const char *shm_name, Oid heap_relid, List *attnos,
                       Snapshot snapshot, int nworkers)
{
    ShmWorkerHandle *h = pgch_shm_worker_register(shm_name, heap_relid, attnos,
                                                  snapshot, nworkers);

    pgch_shm_worker_wait_started(h);
    return h;
}

/* Raise the error of any failed worker (or a worker-death error). Scans all W. */
void
pgch_shm_worker_check_error(ShmWorkerHandle *h)
{
    int w;

    if (h == NULL)
        return;

    for (w = 0; w < h->nworkers; w++)
    {
        uint32 st = pg_atomic_read_u32(&wslot_of(h->hdr, w)->state);
        pid_t  pid;

        if (st == PGCH_WS_ERROR)
            ereport(ERROR,
                    (errmsg("pg_clickhouse: shm streaming worker failed: %s",
                            wslot_of(h->hdr, w)->errmsg)));

        if (st != PGCH_WS_DONE && h->bgw[w] != NULL
            && GetBackgroundWorkerPid(h->bgw[w], &pid) == BGWH_STOPPED)
        {
            /* Re-read in case the worker set the state just before exiting. */
            st = pg_atomic_read_u32(&wslot_of(h->hdr, w)->state);
            if (st == PGCH_WS_ERROR)
                ereport(ERROR,
                        (errmsg("pg_clickhouse: shm streaming worker failed: %s",
                                wslot_of(h->hdr, w)->errmsg)));
            if (st != PGCH_WS_DONE)
                ereport(ERROR,
                        (errmsg("pg_clickhouse: shm streaming worker %d/%d stopped unexpectedly",
                                w, h->nworkers)));
        }
    }
}

/* Block until EVERY worker has created/attached its producer (so the consumer
 * can attach and all W producers are counted toward end-of-stream). */
void
pgch_shm_worker_wait_ready(ShmWorkerHandle *h)
{
    for (;;)
    {
        bool all_ready = true;
        int  w;

        for (w = 0; w < h->nworkers; w++)
        {
            uint32 st = pg_atomic_read_u32(&wslot_of(h->hdr, w)->state);

            if (st != PGCH_WS_READY && st != PGCH_WS_DONE)
            {
                all_ready = false;
                break;
            }
        }
        if (all_ready)
            return;

        /* Raises on any worker's PGCH_WS_ERROR or unexpected death. */
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
    int w;

    if (h == NULL || h->shut_down)
        return;
    h->shut_down = true;

    /* SIGTERM every still-running worker (its CHECK_FOR_INTERRUPTS in the publish
     * / drain / attach loops turns this into a clean FATAL that aborts its
     * transaction; the owner's abort unlinks the SHM object + control socket).
     * Then wait for each to be gone so no worker, fd, or /dev/shm object outlives
     * the query. Issue all SIGTERMs first, then reap, so teardown is concurrent. */
    if (h->bgw != NULL)
    {
        for (w = 0; w < h->nworkers; w++)
            if (h->bgw[w] != NULL)
                TerminateBackgroundWorker(h->bgw[w]);
        for (w = 0; w < h->nworkers; w++)
            if (h->bgw[w] != NULL)
            {
                WaitForBackgroundWorkerShutdown(h->bgw[w]);
                pfree(h->bgw[w]);
                h->bgw[w] = NULL;
            }
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
    ShmWorkerCoord  *coord;
    ShmWorkerSlot   *me;
    int              my_index = 0;

    pqsignal(SIGTERM, die);
    BackgroundWorkerUnblockSignals();

    /* This worker's index within the cooperating set, passed in bgw_extra by the
     * launcher. Worker w owns ring "<base>_<w>". */
    memcpy(&my_index, MyBgworkerEntry->bgw_extra, sizeof(my_index));

    seg = dsm_attach(DatumGetUInt32(main_arg));
    if (seg == NULL)
    {
        /* No way to report; the backend's wait will see the worker stop. */
        ereport(LOG, (errmsg("pg_clickhouse: shm streaming worker could not attach to its DSM segment")));
        return;
    }
    hdr = (ShmWorkerHeader *) dsm_segment_address(seg);
    coord = coord_of(hdr);
    me = wslot_of(hdr, my_index);

    BackgroundWorkerInitializeConnectionByOid(hdr->database_id, hdr->user_id, 0);

    StartTransactionCommand();

    PG_TRY();
    {
        Snapshot          snap;
        Relation          rel;
        ShmOffloadColumn *cols;
        ShmColumnSchema  *schema;
        ShmProducer      *producer;
        ShmBlockCursor   *bcursor;
        char             *my_shm_name;
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

        {
            List *attno_list = NIL;

            for (i = 0; i < ncols; i++)
                attno_list = lappend_int(attno_list, attnos[i]);
            ncols = pgch_build_offload_columns(rel, attno_list, &cols);
        }
        /* The producer schema (names + CH type strings) follows from the columns. */
        schema = (ShmColumnSchema *) palloc0(sizeof(ShmColumnSchema) * ncols);
        for (i = 0; i < ncols; i++)
        {
            strlcpy(schema[i].name, cols[i].name, sizeof(schema[i].name));
            strlcpy(schema[i].type_string, cols[i].ch_type, sizeof(schema[i].type_string));
            schema[i].wire = cols[i].wire;
        }

        /*
         * Each worker creates and owns its OWN ring, named "<base>_<index>".
         * The W rings are independent: the ClickHouse query reads them as W
         * parallel streamed_table() sources (UNION ALL), so the consumer
         * parallelizes W-way. The producer is owned by the transaction context so
         * an abort (error / SIGTERM-FATAL) unlinks this worker's ring + socket via
         * the reset callback.
         */
        my_shm_name = psprintf("%s_%d", hdr->shm_name, my_index);
        producer = shm_producer_create(my_shm_name, schema, ncols,
                                       (uint32_t) PGCH_SHM_RING_DEPTH_K,
                                       PGCH_SHM_DATA_REGION_BYTES,
                                       CurTransactionContext);

        /* Abandon the stream (rather than hang) if the originating backend dies
         * while we are blocked on a full ring -- a dead backend means the
         * ClickHouse consumer was cancelled and this ring will never drain. */
        shm_producer_set_origin_pid(producer, hdr->backend_pid);

        /* Mark this worker ready. Once every worker is ready the backend dispatches
         * the ClickHouse query (each ring's control socket is up for its source). */
        pg_atomic_write_u32(&me->state, PGCH_WS_READY);
        SetLatch(&hdr->backend_proc->procLatch);

        /* Apply the backend session's GUC choices in this worker. */
        pgch_log_stream_stats = hdr->log_stream_stats;
        pgch_enable_jit_deform = hdr->enable_jit_deform;
        pgch_jit_row_threshold = hdr->jit_row_threshold;

        /* Cooperating workers (W>1) pull disjoint heap-block ranges from the
         * shared cursor; a lone worker scans the whole relation (NULL). */
        bcursor = (hdr->nworkers > 1) ? &coord->block : NULL;

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
            PgchVisStats   vis;

            memset(&vis, 0, sizeof(vis));
            getrusage(RUSAGE_SELF, &r0);
            w0 = GetCurrentTimestamp();
            rows = pgch_stream_relation_to_shm(rel, GetActiveSnapshot(), cols, ncols,
                                               producer, (size_t) PGCH_SHM_ROWS_PER_BLOCK, bcursor, &vis);
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
                 vis.used_vectorized ? "vectorized" : "scalar", rows, wall_ms, cpu_ms,
                 cpu_ms > 0 ? (double) rows / cpu_ms / 1000.0 : 0.0);

            /* Visibility-path breakdown: lets a test prove which path ran (a
             * result match alone never proves the classifier/oracle executed).
             * Only meaningful for the vectorized page reader. */
            if (vis.used_vectorized)
                elog(LOG,
                     "pg_clickhouse shm visibility: pages=" UINT64_FORMAT
                     " all_visible=" UINT64_FORMAT " classified=" UINT64_FORMAT
                     " gathered=" UINT64_FORMAT " visible_fast=" UINT64_FORMAT
                     " invisible_fast=" UINT64_FORMAT " undecided=" UINT64_FORMAT
                     " slow_visible=" UINT64_FORMAT,
                     vis.pages_total, vis.pages_all_visible, vis.pages_classified,
                     vis.tuples_gathered, vis.n_visible_fast, vis.n_invisible_fast,
                     vis.n_undecided, vis.n_slow_visible);
        }
        else
            (void) pgch_stream_relation_to_shm(rel, GetActiveSnapshot(), cols, ncols,
                                               producer, (size_t) PGCH_SHM_ROWS_PER_BLOCK, bcursor, NULL);

        /*
         * This worker streamed its share into its OWN ring; signal end-of-stream
         * on it (each ring is independent -- the consumer reads each source to its
         * own EOS), then drain consumer retains and unlink. No cross-worker
         * coordination is needed.
         */
        shm_producer_signal_eos(producer);
        shm_producer_destroy(producer);     /* drains consumer retains, unlinks */

        table_close(rel, AccessShareLock);
        PopActiveSnapshot();

        pg_atomic_write_u32(&me->state, PGCH_WS_DONE);
        SetLatch(&hdr->backend_proc->procLatch);
    }
    PG_CATCH();
    {
        MemoryContext ecxt = MemoryContextSwitchTo(TopMemoryContext);
        ErrorData    *edata = CopyErrorData();

        strlcpy(me->errmsg, edata->message, sizeof(me->errmsg));
        FreeErrorData(edata);
        MemoryContextSwitchTo(ecxt);

        /* Publish this worker's error BEFORE tearing down the producer, so the
         * backend (woken by this latch or by ClickHouse seeing this ring's source
         * go away) reads the real cause rather than a generic producer-death
         * error. This worker's ring is independent; its abort unlinks only its own
         * ring + socket, and that source's failure fails the whole UNION query. */
        pg_atomic_write_u32(&me->state, PGCH_WS_ERROR);
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
