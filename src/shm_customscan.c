/*-------------------------------------------------------------------------
 *
 * shm_customscan.c
 *      Automatic, GUC-gated offload of regular heap-table scans to a co-located
 *      ClickHouse via the SHM streamed_table() path.
 *
 *      A set_rel_pathlist_hook adds a CustomScan path for an eligible heap
 *      base relation when pg_clickhouse.enable_shm_offload is on. The CustomScan
 *      executor streams the scan's snapshot-visible rows into shared memory
 *      (reusing the shm_offload columnizer + shm_producer), dispatches
 *      `SELECT <cols> FROM streamed_table('<shm>','<schema>') WHERE <pushed>`
 *      over the existing ClickHouse connection, and returns the result rows.
 *      With the GUC off, no path is added and plans are identical to stock
 *      pg_clickhouse, so offload-on vs offload-off result comparison is trivial.
 *
 *      ExecutorStart/End hooks maintain the read-only
 *      pg_clickhouse.last_query_used_clickhouse flag (read via SHOW).
 *
 *      Pre-buffering note: a single backend cannot both block publishing into
 *      the ring and block on the ClickHouse result, so the whole (bounded)
 *      input is streamed into the SHM region first, then the query is
 *      dispatched and drained. A size guard fails fast if the relation does not
 *      fit in pg_clickhouse.shm_data_region_mb; unbounded streaming via a
 *      background worker is a follow-up.
 *
 * Copyright (c) 2025-2026, ClickHouse, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/sysattr.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/pg_class.h"
#include "executor/executor.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/bitmapset.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "nodes/pathnodes.h"
#include "nodes/plannodes.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planner.h"
#include "optimizer/restrictinfo.h"
#include "parser/parsetree.h"
#include "storage/bufmgr.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include "engine.h"
#include "fdw.h"
#include "shm_offload.h"
#include "shm_producer.h"

/* fdw_private / custom_private indexes for the CustomScan. */
enum ShmScanPrivate {
    ShmScanPrivateSql = 0,        /* String: the ClickHouse SELECT */
    ShmScanPrivateRetrievedAttrs, /* List<int>: result attno mapping */
    ShmScanPrivateFetchSize,      /* Integer: streaming fetch size */
    ShmScanPrivateShmName,        /* String: SHM object name */
    ShmScanPrivateSchema,         /* String: streamed_table schema columns */
    ShmScanPrivateAttnos,         /* List<int>: projected heap attnos */
    ShmScanPrivateHeapRelid       /* Integer (Oid): heap relation to scan */
};

/* Observability: did the current / previous query offload to ClickHouse? */
bool pgch_this_query_used_ch = false;
static bool pgch_last_query_used_ch = false;
static bool pgch_last_query_used_ch_gucvar = false; /* GUC backing var (unused for display) */

static set_rel_pathlist_hook_type prev_set_rel_pathlist_hook = NULL;
static create_upper_paths_hook_type prev_create_upper_paths_hook = NULL;
static ExecutorStart_hook_type prev_ExecutorStart_hook = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd_hook = NULL;

/* Executor state for a SHM-offload CustomScan. */
typedef struct ShmScanState {
    CustomScanState css;
    char           *sql;
    List           *retrieved_attrs;
    int             fetch_size;
    char           *shm_name;
    char           *schema_string;
    List           *attnos;
    Oid             heap_relid;
    Relation        heap_rel;
    bool            opened_rel;     /* true if we table_open'd heap_rel ourselves */
    ShmOffloadColumn *cols;
    int             ncols;
    ShmProducer    *producer;
    ch_connection   conn;
    ch_cursor      *cursor;
    bool            is_streaming;
    bool            dispatched;
    TupleDesc       tupdesc;
    AttInMetadata  *attinmeta;
    MemoryContext   batch_cxt;
    MemoryContext   temp_cxt;
} ShmScanState;

static void shm_create_upper_paths(PlannerInfo *root, UpperRelationKind stage,
                                   RelOptInfo *input_rel, RelOptInfo *output_rel,
                                   void *extra);
static Plan *shm_plan_custom_path(PlannerInfo *root, RelOptInfo *rel,
                                  CustomPath *best_path, List *tlist,
                                  List *clauses, List *custom_plans);
static Node *shm_create_custom_scan_state(CustomScan *cscan);
static void shm_begin_custom_scan(CustomScanState *node, EState *estate, int eflags);
static TupleTableSlot *shm_exec_custom_scan(CustomScanState *node);
static void shm_end_custom_scan(CustomScanState *node);
static void shm_rescan_custom_scan(CustomScanState *node);

static CustomPathMethods shm_path_methods = {
    .CustomName = "ClickHouseShmScan",
    .PlanCustomPath = shm_plan_custom_path,
};

static CustomScanMethods shm_scan_methods = {
    .CustomName = "ClickHouseShmScan",
    .CreateCustomScanState = shm_create_custom_scan_state,
};

static CustomExecMethods shm_exec_methods = {
    .CustomName = "ClickHouseShmScan",
    .BeginCustomScan = shm_begin_custom_scan,
    .ExecCustomScan = shm_exec_custom_scan,
    .EndCustomScan = shm_end_custom_scan,
    .ReScanCustomScan = shm_rescan_custom_scan,
};

/* SHOW hook for pg_clickhouse.last_query_used_clickhouse. */
static const char *
shm_last_query_show_hook(void)
{
    return pgch_last_query_used_ch ? "on" : "off";
}

/* --------------------------------------------------------------------- */
/* Planner: set_rel_pathlist_hook */
/* --------------------------------------------------------------------- */

/*
 * Decide whether `rel` (a heap base relation) is eligible for SHM offload and,
 * if so, build its CHFdwRelationInfo (conditions classified, projected schema)
 * and add a CustomScan path. Returns silently (no path) when not eligible, so
 * the normal plan is used.
 */
static void
shm_consider_offload(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
    CHFdwRelationInfo *fpinfo;
    ForeignServer *server;
    Bitmapset *attrs_used = NULL;
    List *attnos = NIL;
    StringInfoData schema;
    Relation heap_rel;
    TupleDesc tupdesc;
    CustomPath *cpath;
    int x;
    ListCell *lc;
    static uint32 counter = 0;

    if (!pgch_enable_shm_offload || pgch_local_ch_server == NULL || pgch_local_ch_server[0] == '\0')
        return;

    /* Plain, non-system heap base relation only. */
    if (rte->rtekind != RTE_RELATION || rte->relkind != RELKIND_RELATION)
        return;
    if (rel->reloptkind != RELOPT_BASEREL)
        return;
    if (rel->rows < (double) pgch_shm_min_rows)
        return;

    /* The local ClickHouse server must exist (a CREATE SERVER). */
    server = GetForeignServerByName(pgch_local_ch_server, true);
    if (server == NULL)
        return;

    /* Columns referenced by the query for this rel (target + restrict clauses). */
    pull_varattnos((Node *) rel->reltarget->exprs, rel->relid, &attrs_used);
    foreach (lc, rel->baserestrictinfo)
    {
        RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);
        pull_varattnos((Node *) rinfo->clause, rel->relid, &attrs_used);
    }

    heap_rel = table_open(rte->relid, NoLock); /* planner already holds a lock */
    tupdesc = RelationGetDescr(heap_rel);

    initStringInfo(&schema);
    x = -1;
    while ((x = bms_next_member(attrs_used, x)) >= 0)
    {
        AttrNumber attno = x + FirstLowInvalidHeapAttributeNumber;
        Form_pg_attribute att;
        ShmOffloadColumn col;

        if (attno <= 0)                 /* skip whole-row / system columns */
        {
            table_close(heap_rel, NoLock);
            return;                     /* unsupported reference -> decline */
        }
        att = TupleDescAttr(tupdesc, attno - 1);
        if (att->attisdropped || !att->attnotnull)
        {
            table_close(heap_rel, NoLock);
            return;                     /* NULLs unsupported in phase 1 -> decline */
        }
        if (!pgch_pg_type_to_ch_wire(att->atttypid, &col))
        {
            table_close(heap_rel, NoLock);
            return;                     /* unsupported type -> decline */
        }
        if (schema.len > 0)
            appendStringInfoString(&schema, ", ");
        appendStringInfo(&schema, "%s %s", NameStr(att->attname), col.ch_type);
        attnos = lappend_int(attnos, attno);
    }
    table_close(heap_rel, NoLock);

    if (attnos == NIL)                  /* nothing to project -> decline */
        return;

    fpinfo = (CHFdwRelationInfo *) palloc0(sizeof(CHFdwRelationInfo));
    fpinfo->pushdown_safe = true;
    fpinfo->is_heap_offload = true;
    fpinfo->heap_relid = rte->relid;
    fpinfo->server = server;
    fpinfo->table = NULL;
    fpinfo->user = NULL;
    fpinfo->fetch_size = 65536;
    fpinfo->shm_attnos = attnos;
    fpinfo->shm_schema_string = schema.data;
    fpinfo->shm_name = psprintf("/pgch_%d_%u_%u", (int) MyProcPid, rel->relid, ++counter);
    fpinfo->relation_name = makeStringInfo();
    appendStringInfoString(fpinfo->relation_name, get_rel_name(rte->relid));

    chfdw_classify_conditions(root, rel, rel->baserestrictinfo,
                              &fpinfo->remote_conds, &fpinfo->local_conds);
    fpinfo->attrs_used = attrs_used;
    rel->fdw_private = fpinfo;

    /* Add a CustomScan path with a cost that wins when the feature is enabled. */
    cpath = makeNode(CustomPath);
    cpath->path.pathtype = T_CustomScan;
    cpath->path.parent = rel;
    cpath->path.pathtarget = rel->reltarget;
    cpath->path.param_info = NULL;
    cpath->path.rows = rel->rows;
    cpath->path.startup_cost = 1.0;
    cpath->path.total_cost = 1.0 + rel->rows * 0.001;
    cpath->path.pathkeys = NIL;
    cpath->flags = 0;
    cpath->custom_paths = NIL;
    cpath->custom_private = NIL;
    cpath->methods = &shm_path_methods;
    add_path(rel, (Path *) cpath);
}

static void
shm_set_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte)
{
    if (prev_set_rel_pathlist_hook)
        prev_set_rel_pathlist_hook(root, rel, rti, rte);

    PG_TRY();
    {
        shm_consider_offload(root, rel, rte);
    }
    PG_CATCH();
    {
        /* Never let an offload-planning hiccup break normal planning. */
        FlushErrorState();
        rel->fdw_private = NULL;
    }
    PG_END_TRY();
}

/* --------------------------------------------------------------------- */
/* Planner: create_upper_paths_hook (aggregate / GROUP BY pushdown) */
/* --------------------------------------------------------------------- */

/*
 * For a GROUP/aggregate upper rel whose input is an offload-eligible heap rel,
 * add a CustomScan path that pushes the whole scan+filter+aggregate fragment to
 * ClickHouse (deparsed against streamed_table()). Reuses the FDW's
 * foreign_grouping_ok to validate shippability and build the grouped tlist.
 */
static void
shm_create_upper_paths(PlannerInfo *root, UpperRelationKind stage,
                       RelOptInfo *input_rel, RelOptInfo *output_rel, void *extra)
{
    CHFdwRelationInfo *ifpinfo;
    CHFdwRelationInfo *fpinfo;
    Query *parse = root->parse;
    Node *havingQual;
    CustomPath *cpath;
    bool ok = false;

    if (prev_create_upper_paths_hook)
        prev_create_upper_paths_hook(root, stage, input_rel, output_rel, extra);

    if (!pgch_enable_shm_offload)
        return;
    if (stage != UPPERREL_GROUP_AGG || output_rel->fdw_private != NULL)
        return;
    if (input_rel->fdw_private == NULL)
        return;
    ifpinfo = (CHFdwRelationInfo *) input_rel->fdw_private;
    if (!ifpinfo->is_heap_offload || !ifpinfo->pushdown_safe)
        return;
    if (!parse->groupClause && !parse->groupingSets && !parse->hasAggs && !root->hasHavingQual)
        return;
    if (parse->groupingSets)
        return;                         /* GROUPING SETS not supported */

    havingQual = ((GroupPathExtraData *) extra)->havingQual;

    /* Grouped fpinfo: carry the SHM source info from the base rel. The seam
     * still fires on outerrel (the base rel) during deparse, so is_heap_offload
     * stays false here. */
    fpinfo = (CHFdwRelationInfo *) palloc0(sizeof(CHFdwRelationInfo));
    fpinfo->stage = stage;
    fpinfo->pushdown_safe = false;
    fpinfo->outerrel = input_rel;
    fpinfo->server = ifpinfo->server;
    fpinfo->table = NULL;
    fpinfo->user = NULL;
    fpinfo->fetch_size = ifpinfo->fetch_size;
    fpinfo->is_heap_offload = false;
    fpinfo->heap_relid = ifpinfo->heap_relid;
    fpinfo->shm_name = ifpinfo->shm_name;
    fpinfo->shm_schema_string = ifpinfo->shm_schema_string;
    fpinfo->shm_attnos = ifpinfo->shm_attnos;
    fpinfo->relation_name = ifpinfo->relation_name;
    output_rel->fdw_private = fpinfo;

    PG_TRY();
    {
        ok = foreign_grouping_ok(root, output_rel, havingQual);
    }
    PG_CATCH();
    {
        FlushErrorState();
        ok = false;
    }
    PG_END_TRY();

    if (!ok)
    {
        output_rel->fdw_private = NULL;
        return;
    }

    cpath = makeNode(CustomPath);
    cpath->path.pathtype = T_CustomScan;
    cpath->path.parent = output_rel;
    cpath->path.pathtarget = output_rel->reltarget;
    cpath->path.param_info = NULL;
    cpath->path.rows = output_rel->rows > 0 ? output_rel->rows : 1;
    cpath->path.startup_cost = 1.0;
    cpath->path.total_cost = 2.0;            /* win when the feature is enabled */
    cpath->path.pathkeys = NIL;
    cpath->flags = 0;
    cpath->custom_paths = NIL;
    cpath->custom_private = NIL;
    cpath->methods = &shm_path_methods;
    add_path(output_rel, (Path *) cpath);
}

/* --------------------------------------------------------------------- */
/* Planner: build the CustomScan plan node */
/* --------------------------------------------------------------------- */

static Plan *
shm_plan_custom_path(PlannerInfo *root, RelOptInfo *rel, CustomPath *best_path,
                     List *tlist, List *clauses, List *custom_plans)
{
    CHFdwRelationInfo *fpinfo = (CHFdwRelationInfo *) rel->fdw_private;
    CustomScan *cscan = makeNode(CustomScan);
    List *remote_exprs = NIL;
    List *local_exprs = NIL;
    List *fdw_scan_tlist = NIL;
    List *retrieved_attrs = NIL;
    List *params_list = NIL;
    StringInfoData sql;
    Index scan_relid;

    if (IS_UPPER_REL(rel))
    {
        /* Aggregate/GROUP BY pushdown: scanrelid 0; the columns to fetch are the
         * grouped target list, WHERE comes from the base rel's pushed conditions
         * (handled inside the deparser), and HAVING from this rel's remote_conds. */
        scan_relid = 0;
        fdw_scan_tlist = chfdw_build_tlist_to_deparse(rel);
        remote_exprs = extract_actual_clauses(fpinfo->remote_conds, false);
        local_exprs = extract_actual_clauses(fpinfo->local_conds, false);
    }
    else
    {
        ListCell *lc;

        /* Base scan: split scan_clauses into remote (pushed) and local (kept). */
        scan_relid = rel->relid;
        foreach (lc, clauses)
        {
            RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

            if (rinfo->pseudoconstant)
                continue;
            if (list_member_ptr(fpinfo->remote_conds, rinfo))
                remote_exprs = lappend(remote_exprs, rinfo->clause);
            else if (list_member_ptr(fpinfo->local_conds, rinfo))
                local_exprs = lappend(local_exprs, rinfo->clause);
            else if (chfdw_is_foreign_expr(root, rel, rinfo->clause))
                remote_exprs = lappend(remote_exprs, rinfo->clause);
            else
                local_exprs = lappend(local_exprs, rinfo->clause);
        }
    }

    initStringInfo(&sql);
    chfdw_deparse_select_stmt_for_rel(&sql, root, rel, fdw_scan_tlist, remote_exprs, NIL,
                                      false, false, false,
                                      &retrieved_attrs, &params_list);

    cscan->scan.plan.targetlist = tlist;
    cscan->scan.plan.qual = local_exprs;
    cscan->scan.scanrelid = scan_relid;
    cscan->custom_scan_tlist = fdw_scan_tlist;   /* NIL for base, grouped tlist for upper */
    cscan->custom_plans = custom_plans;
    cscan->custom_exprs = NIL;                    /* no external params in phase 1 */
    cscan->custom_private = list_make5(
        makeString(sql.data),
        retrieved_attrs,
        makeInteger(fpinfo->fetch_size),
        makeString(fpinfo->shm_name),
        makeString(fpinfo->shm_schema_string));
    cscan->custom_private = lappend(cscan->custom_private, fpinfo->shm_attnos);
    cscan->custom_private = lappend(cscan->custom_private, makeInteger((int) fpinfo->heap_relid));
    cscan->methods = &shm_scan_methods;
    return (Plan *) cscan;
}

/* --------------------------------------------------------------------- */
/* Executor */
/* --------------------------------------------------------------------- */

static Node *
shm_create_custom_scan_state(CustomScan *cscan)
{
    ShmScanState *sss = (ShmScanState *) palloc0(sizeof(ShmScanState));

    NodeSetTag(sss, T_CustomScanState);
    sss->css.methods = &shm_exec_methods;
    /* We hand the executor heap tuples (built by shm_fetch_into_slot), so the
     * scan tuple slot must be a heap-tuple slot (same as the FDW ForeignScan). */
    sss->css.slotOps = &TTSOpsHeapTuple;
    sss->sql = strVal(list_nth(cscan->custom_private, ShmScanPrivateSql));
    sss->retrieved_attrs = (List *) list_nth(cscan->custom_private, ShmScanPrivateRetrievedAttrs);
    sss->fetch_size = intVal(list_nth(cscan->custom_private, ShmScanPrivateFetchSize));
    sss->shm_name = strVal(list_nth(cscan->custom_private, ShmScanPrivateShmName));
    sss->schema_string = strVal(list_nth(cscan->custom_private, ShmScanPrivateSchema));
    sss->attnos = (List *) list_nth(cscan->custom_private, ShmScanPrivateAttnos);
    sss->heap_relid = (Oid) intVal(list_nth(cscan->custom_private, ShmScanPrivateHeapRelid));
    return (Node *) sss;
}

static void
shm_begin_custom_scan(CustomScanState *node, EState *estate, int eflags)
{
    ShmScanState *sss = (ShmScanState *) node;
    Relation rel;
    ForeignServer *server;
    UserMapping *user;
    ShmColumnSchema *producer_schema;
    TupleDesc heap_tupdesc;
    ListCell *lc;
    int i;

    if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
        return;

    /* Observability: this query is offloading to ClickHouse. */
    pgch_this_query_used_ch = true;

    /* Base scan (scanrelid>0) reuses the executor's relation; the grouped scan
     * (scanrelid==0) has no ss_currentRelation, so open the heap rel ourselves. */
    if (node->ss.ss_currentRelation != NULL)
    {
        rel = node->ss.ss_currentRelation;
        sss->opened_rel = false;
    }
    else
    {
        rel = table_open(sss->heap_relid, AccessShareLock);
        sss->opened_rel = true;
    }
    sss->heap_rel = rel;
    heap_tupdesc = RelationGetDescr(rel);

    /* Build the projected ShmOffloadColumn[] from the stored attnos. */
    sss->ncols = list_length(sss->attnos);
    sss->cols = palloc0(sizeof(ShmOffloadColumn) * sss->ncols);
    producer_schema = palloc0(sizeof(ShmColumnSchema) * sss->ncols);
    i = 0;
    foreach (lc, sss->attnos)
    {
        AttrNumber attno = (AttrNumber) lfirst_int(lc);
        Form_pg_attribute att = TupleDescAttr(heap_tupdesc, attno - 1);

        if (!pgch_pg_type_to_ch_wire(att->atttypid, &sss->cols[i]))
            ereport(ERROR, (errmsg("pg_clickhouse: column '%s' became unsupported for SHM offload",
                                   NameStr(att->attname))));
        sss->cols[i].attno = attno;
        strlcpy(sss->cols[i].name, NameStr(att->attname), sizeof(sss->cols[i].name));
        strlcpy(producer_schema[i].name, NameStr(att->attname), sizeof(producer_schema[i].name));
        strlcpy(producer_schema[i].type_string, sss->cols[i].ch_type, sizeof(producer_schema[i].type_string));
        producer_schema[i].wire = sss->cols[i].wire;
        i++;
    }

    /* Size guard: the whole relation must fit in the SHM data region (pre-buffer). */
    {
        size_t region = (size_t) pgch_shm_data_region_mb * 1024 * 1024;
        size_t est = (size_t) RelationGetNumberOfBlocks(rel) * BLCKSZ;

        if (est > region)
            ereport(ERROR,
                    (errmsg("pg_clickhouse: relation \"%s\" (~%zu bytes) exceeds "
                            "pg_clickhouse.shm_data_region_mb (%d MiB) for SHM offload",
                            RelationGetRelationName(rel), est, pgch_shm_data_region_mb),
                     errhint("Raise pg_clickhouse.shm_data_region_mb, or disable "
                             "pg_clickhouse.enable_shm_offload for this query.")));
    }

    server = GetForeignServerByName(pgch_local_ch_server, false);
    user = GetUserMapping(GetUserId(), server->serverid);
    sss->conn = chfdw_get_connection(user);

    /* Result rows are fetched into the scan tuple slot: heap tupdesc for a base
     * scan, the grouped (custom_scan_tlist) tupdesc for an aggregate scan. */
    sss->tupdesc = node->ss.ss_ScanTupleSlot->tts_tupleDescriptor;
    sss->attinmeta = TupleDescGetAttInMetadata(sss->tupdesc);
    sss->batch_cxt = AllocSetContextCreate(estate->es_query_cxt,
                                           "pg_clickhouse shm scan", ALLOCSET_DEFAULT_SIZES);
    sss->temp_cxt = AllocSetContextCreate(estate->es_query_cxt,
                                          "pg_clickhouse shm scan tmp", ALLOCSET_SMALL_SIZES);

    sss->producer = shm_producer_create(sss->shm_name, producer_schema, sss->ncols,
                                        (uint32_t) pgch_shm_ring_depth_k,
                                        (size_t) pgch_shm_data_region_mb * 1024 * 1024,
                                        estate->es_query_cxt);
    sss->dispatched = false;
}

/* Pull one result row from the ClickHouse cursor into `slot`, or NULL at EOF. */
static bool
shm_fetch_into_slot(ShmScanState *sss, TupleTableSlot *slot)
{
    MemoryContext old = MemoryContextSwitchTo(sss->temp_cxt);
    Datum *values = palloc0(sss->tupdesc->natts * sizeof(Datum));
    bool *nulls = palloc(sss->tupdesc->natts * sizeof(bool));
    cursor_fetch_row_method fetch_fn;
    ChFdwScanRowContext ctx;
    HeapTuple tuple;

    memset(nulls, true, sss->tupdesc->natts * sizeof(bool));

    ctx.tupdesc = sss->tupdesc;
    ctx.retrieved_attrs = sss->retrieved_attrs;
    ctx.attinmeta = sss->attinmeta;
    ctx.cursor = sss->cursor;
    ctx.values = values;
    ctx.nulls = nulls;

    fetch_fn = sss->conn.methods->fetch_row;
    if (sss->is_streaming && sss->conn.methods->streaming_fetch_row != NULL)
        fetch_fn = sss->conn.methods->streaming_fetch_row;

    if (fetch_fn(&ctx) == NULL)
    {
        MemoryContextSwitchTo(old);
        MemoryContextReset(sss->temp_cxt);
        return false;
    }

    MemoryContextSwitchTo(old);
    tuple = heap_form_tuple(sss->tupdesc, values, nulls);
    HeapTupleHeaderSetXmax(tuple->t_data, InvalidTransactionId);
    HeapTupleHeaderSetXmin(tuple->t_data, InvalidTransactionId);
    HeapTupleHeaderSetCmin(tuple->t_data, InvalidTransactionId);
    ExecStoreHeapTuple(tuple, slot, false);
    MemoryContextReset(sss->temp_cxt);
    return true;
}

/* ExecScan access method: produce the next raw scan tuple. On the first call it
 * streams the whole relation into SHM and dispatches the ClickHouse query. */
static TupleTableSlot *
shm_scan_access_mtd(ScanState *ss)
{
    ShmScanState *sss = (ShmScanState *) ss;   /* ss is at offset 0 of CustomScanState */
    TupleTableSlot *slot = ss->ss_ScanTupleSlot;

    if (!sss->dispatched)
    {
        EState *estate = ss->ps.state;
        MemoryContext old;

        /* 1. Stream the whole relation into SHM (pre-buffer) + EOS, under the
         *    executor snapshot for correct MVCC visibility. */
        pgch_stream_relation_to_shm(sss->heap_rel, estate->es_snapshot,
                                    sss->cols, sss->ncols, sss->producer, 65536);

        /* 2. Dispatch the ClickHouse query; CH attaches to the (now fully
         *    populated) SHM stream, consumes it, and returns the result. */
        old = MemoryContextSwitchTo(sss->batch_cxt);
        {
            ch_query query = new_query(sss->sql, 0, NULL, sss->tupdesc, sss->retrieved_attrs);

            sss->is_streaming = sss->fetch_size > 0 && sss->conn.methods->streaming_query != NULL;
            if (sss->is_streaming)
                sss->cursor = sss->conn.methods->streaming_query(sss->conn.conn, &query, sss->fetch_size);
            else
                sss->cursor = sss->conn.methods->simple_query(sss->conn.conn, &query);
        }
        MemoryContextSwitchTo(old);
        sss->dispatched = true;
    }

    if (!shm_fetch_into_slot(sss, slot))
        return ExecClearTuple(slot);
    return slot;
}

static bool
shm_scan_recheck_mtd(ScanState *ss, TupleTableSlot *slot)
{
    (void) ss;
    (void) slot;
    return true;
}

static TupleTableSlot *
shm_exec_custom_scan(CustomScanState *node)
{
    CHECK_FOR_INTERRUPTS();
    /* ExecScan applies the local quals (scan.plan.qual) and projection. */
    return ExecScan(&node->ss, shm_scan_access_mtd, shm_scan_recheck_mtd);
}

static void
shm_end_custom_scan(CustomScanState *node)
{
    ShmScanState *sss = (ShmScanState *) node;

    if (sss->cursor)
    {
        MemoryContextDelete(sss->cursor->memcxt);
        sss->cursor = NULL;
    }
    if (sss->producer)
    {
        shm_producer_destroy(sss->producer);
        sss->producer = NULL;
    }
    if (sss->opened_rel && sss->heap_rel)
    {
        table_close(sss->heap_rel, AccessShareLock);
        sss->heap_rel = NULL;
        sss->opened_rel = false;
    }
}

static void
shm_rescan_custom_scan(CustomScanState *node)
{
    ShmScanState *sss = (ShmScanState *) node;

    /* Phase 1: a rescan re-streams from scratch. Tear down and re-arm. */
    if (sss->cursor)
    {
        MemoryContextDelete(sss->cursor->memcxt);
        sss->cursor = NULL;
    }
    if (sss->producer)
    {
        shm_producer_destroy(sss->producer);
        sss->producer = NULL;
    }
    ereport(ERROR, (errmsg("pg_clickhouse: rescan of a SHM-offload scan is not supported in phase 1")));
}

/* --------------------------------------------------------------------- */
/* Executor hooks for the observability flag */
/* --------------------------------------------------------------------- */

static void
shm_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
    pgch_this_query_used_ch = false;
    if (prev_ExecutorStart_hook)
        prev_ExecutorStart_hook(queryDesc, eflags);
    else
        standard_ExecutorStart(queryDesc, eflags);
}

static void
shm_ExecutorEnd(QueryDesc *queryDesc)
{
    pgch_last_query_used_ch = pgch_this_query_used_ch;
    if (prev_ExecutorEnd_hook)
        prev_ExecutorEnd_hook(queryDesc);
    else
        standard_ExecutorEnd(queryDesc);
}

/* --------------------------------------------------------------------- */
/* Registration */
/* --------------------------------------------------------------------- */

void
pgch_register_customscan_and_hooks(void)
{
    DefineCustomBoolVariable("pg_clickhouse.last_query_used_clickhouse",
                             "Whether the previous query in this session offloaded to ClickHouse (read via SHOW).",
                             NULL, &pgch_last_query_used_ch_gucvar, false,
                             PGC_INTERNAL, 0, NULL, NULL, shm_last_query_show_hook);

    RegisterCustomScanMethods(&shm_scan_methods);

    prev_set_rel_pathlist_hook = set_rel_pathlist_hook;
    set_rel_pathlist_hook = shm_set_rel_pathlist;

    prev_create_upper_paths_hook = create_upper_paths_hook;
    create_upper_paths_hook = shm_create_upper_paths;

    prev_ExecutorStart_hook = ExecutorStart_hook;
    ExecutorStart_hook = shm_ExecutorStart;

    prev_ExecutorEnd_hook = ExecutorEnd_hook;
    ExecutorEnd_hook = shm_ExecutorEnd;
}
