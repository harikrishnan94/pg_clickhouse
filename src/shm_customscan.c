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
 *      Streaming note: a single backend cannot both block publishing into the
 *      ring and block on the ClickHouse result, so the heap scan + columnize +
 *      publish loop runs in cooperating background workers (shm_worker.c) while
 *      this backend dispatches the query and drains the result. Peak shared
 *      memory is bounded by the per-worker ring (K slots) regardless of table
 *      size, so an arbitrarily large relation streams through without
 *      pre-buffering.
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
#include "catalog/pg_type_d.h"
#include "executor/executor.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/bitmapset.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pathnodes.h"
#include "nodes/plannodes.h"
#include "optimizer/cost.h"
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
#include "kv_list.h"
#include "shm_offload.h"
#include "shm_page_reader.h"
#include "shm_producer.h"
#include "shm_worker.h"

/* custom_private indexes for the CustomScan. A scan may stream one (base scan /
 * aggregate over a base rel) or several (join) SHM sources, so the per-source
 * stream descriptors are carried as a List of fixed-shape sublists. */
enum ShmScanPrivate {
    ShmScanPrivateSql = 0,        /* String: the ClickHouse SELECT */
    ShmScanPrivateRetrievedAttrs, /* List<int>: result attno mapping */
    ShmScanPrivateFetchSize,      /* Integer: streaming fetch size */
    ShmScanPrivateSources         /* List<List>: one per SHM source (see ShmScanSourcePrivate) */
};

/* Indexes within one source's sublist in ShmScanPrivateSources. */
enum ShmScanSourcePrivate {
    ShmSourcePrivateShmName = 0,  /* String: SHM object base name */
    ShmSourcePrivateHeapRelid,    /* Integer (Oid): heap relation to scan */
    ShmSourcePrivateSchema,       /* String: streamed_table schema columns */
    ShmSourcePrivateAttnos        /* List<int>: projected heap attnos */
};

/* Observability: did the current / previous query offload to ClickHouse? */
bool pgch_this_query_used_ch = false;
static bool pgch_last_query_used_ch = false;
static bool pgch_last_query_used_ch_gucvar = false; /* GUC backing var (unused for display) */

static set_rel_pathlist_hook_type prev_set_rel_pathlist_hook = NULL;
static set_join_pathlist_hook_type prev_set_join_pathlist_hook = NULL;
static create_upper_paths_hook_type prev_create_upper_paths_hook = NULL;
static ExecutorStart_hook_type prev_ExecutorStart_hook = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd_hook = NULL;

/* One SHM source streamed for a scan: a single heap relation streamed into its
 * own ring(s) by a group of cooperating background workers. A base scan has one
 * of these; a join has one per base relation. */
typedef struct ShmScanSource {
    char            *shm_name;       /* SHM object base name */
    Oid              heap_relid;     /* heap relation to scan */
    char            *schema_string;  /* streamed_table schema columns */
    List            *attnos;         /* projected heap attnos, handed to the workers */
    int              nproducers;     /* producer workers launched for this source */
    ShmWorkerHandle *worker;         /* worker group streaming this heap into SHM */
} ShmScanSource;

/* Executor state for a SHM-offload CustomScan. */
typedef struct ShmScanState {
    CustomScanState css;
    char           *sql;
    List           *retrieved_attrs;
    int             fetch_size;
    ShmScanSource  *sources;        /* [nsources] per-relation SHM streams */
    int             nsources;
    ch_connection   conn;
    ch_cursor      *cursor;
    bool            is_streaming;
    bool            dispatched;
    TupleDesc       tupdesc;
    AttInMetadata  *attinmeta;
    MemoryContext   batch_cxt;
    MemoryContext   temp_cxt;
    MemoryContextCallback worker_cb;   /* reaps the workers on query end OR abort */
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
        if (!pgch_pg_type_to_ch_wire(att->atttypid, att->atttypmod, &col))
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
/* Planner: set_join_pathlist_hook (JOIN pushdown) */
/* --------------------------------------------------------------------- */

/*
 * Is `rel` a relation whose rows are (or can be) sourced from a co-located
 * ClickHouse over shared memory? True for a heap base relation that the base-rel
 * hook marked eligible (is_heap_offload), and recursively for a join relation
 * all of whose leaves are such heap relations (set by this join hook). The
 * recursion through join leaves is what distinguishes OUR heap-offload joins
 * from the FDW's foreign-table joins (whose leaves are foreign tables, never
 * is_heap_offload), so a mixed heap/foreign join fails closed here.
 */
static bool
shm_rel_is_offload_source(RelOptInfo *rel)
{
    CHFdwRelationInfo *fp = (CHFdwRelationInfo *) rel->fdw_private;

    if (fp == NULL || !fp->pushdown_safe)
        return false;
    if (fp->is_heap_offload)
        return true;
    if (IS_JOIN_REL(rel))
        return fp->outerrel != NULL && fp->innerrel != NULL
            && shm_rel_is_offload_source(fp->outerrel)
            && shm_rel_is_offload_source(fp->innerrel);
    return false;
}

/*
 * Decide whether `joinrel` (a join of two SHM-offload sources) can be pushed
 * down and, if so, build its CHFdwRelationInfo (via the FDW's foreign_join_ok)
 * and add a CustomScan path. Each base relation in the join is streamed into its
 * own SHM ring; ClickHouse runs a single
 * SELECT ... FROM streamed_table(A) ALL <type> JOIN streamed_table(B) ON ...
 * (the deparser already emits streamed_table() for is_heap_offload leaves).
 */
static void
shm_consider_join_offload(PlannerInfo *root, RelOptInfo *joinrel,
                          RelOptInfo *outerrel, RelOptInfo *innerrel,
                          JoinType jointype, JoinPathExtraData *extra)
{
    CHFdwRelationInfo *fpinfo;
    CHFdwRelationInfo *fpinfo_o;
    CustomPath *cpath;
    bool ok = false;

    if (!pgch_enable_shm_offload || pgch_local_ch_server == NULL || pgch_local_ch_server[0] == '\0')
        return;

    /* This join combination has already been considered. */
    if (joinrel->fdw_private != NULL)
        return;

    /* Both inputs must be SHM-offload sources (heap rels or lower SHM joins). */
    if (!shm_rel_is_offload_source(outerrel) || !shm_rel_is_offload_source(innerrel))
        return;

    fpinfo_o = (CHFdwRelationInfo *) outerrel->fdw_private;

    /*
     * Allocate the join fpinfo and let the FDW's foreign_join_ok classify the
     * join/where clauses, merge options and fill the join relation info. It
     * supports INNER/LEFT/RIGHT/FULL/SEMI. Mark the join as considered first so
     * a planning hiccup cannot cause infinite reconsideration; reset to NULL on
     * decline so a different (pushable) join order may still be tried.
     */
    fpinfo = (CHFdwRelationInfo *) palloc0(sizeof(CHFdwRelationInfo));
    fpinfo->pushdown_safe = false;
    fpinfo->server = fpinfo_o->server;
    fpinfo->table = NULL;
    fpinfo->user = NULL;
    fpinfo->attrs_used = NULL;
    joinrel->fdw_private = fpinfo;

    PG_TRY();
    {
        ok = foreign_join_ok(root, joinrel, jointype, outerrel, innerrel, extra);
    }
    PG_CATCH();
    {
        FlushErrorState();
        ok = false;
    }
    PG_END_TRY();

    if (!ok)
    {
        joinrel->fdw_private = NULL;
        return;
    }

    /* Add a CustomScan path with a cost that wins when the feature is enabled. */
    cpath = makeNode(CustomPath);
    cpath->path.pathtype = T_CustomScan;
    cpath->path.parent = joinrel;
    cpath->path.pathtarget = joinrel->reltarget;
    cpath->path.param_info = NULL;
    cpath->path.rows = joinrel->rows > 0 ? joinrel->rows : 1;
    cpath->path.startup_cost = 1.0;
    cpath->path.total_cost = 1.0 + (joinrel->rows > 0 ? joinrel->rows : 1) * 0.001;
    cpath->path.pathkeys = NIL;
    cpath->flags = 0;
    cpath->custom_paths = NIL;
    cpath->custom_private = NIL;
    cpath->methods = &shm_path_methods;
    add_path(joinrel, (Path *) cpath);
}

static void
shm_set_join_pathlist(PlannerInfo *root, RelOptInfo *joinrel,
                      RelOptInfo *outerrel, RelOptInfo *innerrel,
                      JoinType jointype, JoinPathExtraData *extra)
{
    if (prev_set_join_pathlist_hook)
        prev_set_join_pathlist_hook(root, joinrel, outerrel, innerrel, jointype, extra);

    PG_TRY();
    {
        shm_consider_join_offload(root, joinrel, outerrel, innerrel, jointype, extra);
    }
    PG_CATCH();
    {
        /* Never let an offload-planning hiccup break normal planning. */
        FlushErrorState();
        joinrel->fdw_private = NULL;
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
    /* The input must be a pushdown-safe SHM source: a heap-offload base relation
     * or a pushed-down SHM join. A grouped fragment over a join streams every
     * leaf relation (shm_collect_sources recurses through outerrel) and pushes
     * the aggregate down over the join. */
    if (!ifpinfo->pushdown_safe || !(ifpinfo->is_heap_offload || IS_JOIN_REL(input_rel)))
        return;
    if (!parse->groupClause && !parse->groupingSets && !parse->hasAggs && !root->hasHavingQual)
        return;
    if (parse->groupingSets)
        return;                         /* GROUPING SETS not supported */

    havingQual = ((GroupPathExtraData *) extra)->havingQual;

    /* Grouped fpinfo: carry the SHM source info from the input scan/join. The
     * deparse seam still fires on outerrel (the base rel or join) during deparse,
     * so is_heap_offload stays false here; shm_collect_sources walks outerrel to
     * find every leaf relation to stream. */
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

    /*
     * Decimal aggregates: keep the math in PostgreSQL for bit-identical results.
     * A pushed aggregate that RETURNS a numeric value is not reproducible across
     * the two engines -- ClickHouse formats a Decimal without PostgreSQL's
     * display-scale trailing zeros (e.g. "0.5" vs "0.50") and avg() over a
     * Decimal yields Float64. So decline the grouped push-down whenever any
     * output column is numeric: the base scan still offloads (ClickHouse adopts
     * the Decimal columns zero-copy and read_rows still covers the whole table)
     * and PostgreSQL computes the exact decimal aggregate over the streamed rows.
     * Non-numeric outputs (count, integer/float aggregates, and HAVING-only
     * decimal comparisons that return a bool) are unaffected and still push down.
     */
    {
        ListCell *lc;

        foreach (lc, output_rel->reltarget->exprs)
        {
            if (exprType((Node *) lfirst(lc)) == NUMERICOID)
            {
                output_rel->fdw_private = NULL;
                return;
            }
        }
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

/*
 * Build the custom_scan_tlist for a base SHM-offload scan.
 *
 * A base scan streams only the projected columns, not the whole heap row, so
 * the scan tuple must be described by an explicit target list rather than the
 * heap relation's descriptor. Relying on the relation descriptor (scanrelid > 0,
 * custom_scan_tlist == NIL) leaves un-projected columns as phantom NULLs in the
 * formed heap tuple; because such a column may be declared NOT NULL, JIT-compiled
 * tuple deforming (slot_compile_deform) treats it as guaranteed-present and skips
 * its null-bitmap check, then reads every following column at the wrong offset --
 * silently corrupting results (the interpreted deform path reads the bitmap and is
 * unaffected, so the bug only appears once JIT kicks in above its cost threshold).
 *
 * ExecTypeFromTL() builds the scan tuple descriptor from this list with attnotnull
 * cleared, so deforming is always correct. The Vars carry varno = scanrelid so
 * setrefs.c maps the plan's targetlist/qual onto the scan tuple via INDEX_VAR.
 *
 * On entry *retrieved_attrs lists the heap attnos in the order the deparsed SELECT
 * fetches them; we emit one Var per fetched column in that order and rewrite
 * *retrieved_attrs to the matching 1-based positions in the new descriptor.
 */
static List *
shm_build_base_scan_tlist(Oid heap_relid, Index scanrelid, List **retrieved_attrs)
{
    List *tlist = NIL;
    List *seq = NIL;
    Relation heap_rel = table_open(heap_relid, NoLock); /* planner already holds a lock */
    TupleDesc tupdesc = RelationGetDescr(heap_rel);
    ListCell *lc;
    int pos = 0;

    foreach (lc, *retrieved_attrs)
    {
        AttrNumber attno = (AttrNumber) lfirst_int(lc);
        Form_pg_attribute att = TupleDescAttr(tupdesc, attno - 1);
        Var *var = makeVar(scanrelid, attno, att->atttypid, att->atttypmod,
                           att->attcollation, 0);

        pos++;
        tlist = lappend(tlist, makeTargetEntry((Expr *) var, pos, NULL, false));
        seq = lappend_int(seq, pos);
    }
    table_close(heap_rel, NoLock);

    *retrieved_attrs = seq;
    return tlist;
}

/*
 * Collect the SHM stream descriptors for every heap relation feeding `rel`,
 * appending one fixed-shape sublist ([shm_name, heap_relid, schema, attnos], see
 * ShmScanSourcePrivate) per source to *sources. A base heap rel contributes one
 * source; a join rel recurses into its outer then inner sides (matching the
 * deparser's FROM-clause order), so a join yields one source per leaf relation.
 */
static void
shm_collect_sources(RelOptInfo *rel, List **sources)
{
    CHFdwRelationInfo *fp = (CHFdwRelationInfo *) rel->fdw_private;

    if (fp == NULL)
        return;

    if (IS_JOIN_REL(rel))
    {
        shm_collect_sources(fp->outerrel, sources);
        shm_collect_sources(fp->innerrel, sources);
    }
    else if (fp->is_heap_offload)
    {
        *sources = lappend(*sources,
                           list_make4(makeString(fp->shm_name),
                                      makeInteger((int) fp->heap_relid),
                                      makeString(fp->shm_schema_string),
                                      fp->shm_attnos));
    }
}

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
    List *sources = NIL;
    StringInfoData sql;
    Index scan_relid;

    if (IS_UPPER_REL(rel))
    {
        /* Aggregate/GROUP BY pushdown: scanrelid 0; the columns to fetch are the
         * grouped target list, WHERE comes from the underlying scan's pushed
         * conditions (handled inside the deparser), and HAVING from this rel's
         * remote_conds. The underlying scan may be a base rel or a join. */
        scan_relid = 0;
        fdw_scan_tlist = chfdw_build_tlist_to_deparse(rel);
        remote_exprs = extract_actual_clauses(fpinfo->remote_conds, false);
        local_exprs = extract_actual_clauses(fpinfo->local_conds, false);
    }
    else if (IS_JOIN_REL(rel))
    {
        /* Join pushdown: scanrelid 0; the columns to fetch are the join's
         * explicit target list, the join ON conditions come from the deparser
         * (fpinfo->joinclauses), WHERE from this rel's remote_conds, and any
         * non-shippable clause stays as a local qual. */
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

    /*
     * Base scan: describe the projected scan tuple with an explicit
     * custom_scan_tlist (see shm_build_base_scan_tlist) so the scan tuple
     * descriptor has no NOT NULL flags on un-streamed columns, which would
     * otherwise make JIT tuple deforming misread the row. The upper/aggregate
     * and join paths already supply their explicit tlist above.
     */
    if (IS_SIMPLE_REL(rel))
        fdw_scan_tlist = shm_build_base_scan_tlist(fpinfo->heap_relid, scan_relid,
                                                   &retrieved_attrs);

    /*
     * Collect the per-relation SHM streams this scan must launch: the single
     * heap relation for a base scan, the underlying scan for an aggregate, or
     * every leaf relation for a join.
     */
    shm_collect_sources(IS_UPPER_REL(rel) ? fpinfo->outerrel : rel, &sources);

    cscan->scan.plan.targetlist = tlist;
    cscan->scan.plan.qual = local_exprs;
    cscan->scan.scanrelid = scan_relid;
    cscan->custom_scan_tlist = fdw_scan_tlist;   /* projected base tlist, or explicit join/upper tlist */
    cscan->custom_plans = custom_plans;
    cscan->custom_exprs = NIL;                    /* no external params in phase 1 */
    cscan->custom_private = list_make4(
        makeString(sql.data),
        retrieved_attrs,
        makeInteger(fpinfo->fetch_size),
        sources);
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
    List *sources = (List *) list_nth(cscan->custom_private, ShmScanPrivateSources);
    ListCell *lc;
    int i;

    NodeSetTag(sss, T_CustomScanState);
    sss->css.methods = &shm_exec_methods;
    /* We hand the executor heap tuples (built by shm_fetch_into_slot), so the
     * scan tuple slot must be a heap-tuple slot (same as the FDW ForeignScan). */
    sss->css.slotOps = &TTSOpsHeapTuple;
    sss->sql = strVal(list_nth(cscan->custom_private, ShmScanPrivateSql));
    sss->retrieved_attrs = (List *) list_nth(cscan->custom_private, ShmScanPrivateRetrievedAttrs);
    sss->fetch_size = intVal(list_nth(cscan->custom_private, ShmScanPrivateFetchSize));

    sss->nsources = list_length(sources);
    sss->sources = (ShmScanSource *) palloc0(sizeof(ShmScanSource) * sss->nsources);
    i = 0;
    foreach (lc, sources)
    {
        List *src = (List *) lfirst(lc);

        sss->sources[i].shm_name = strVal(list_nth(src, ShmSourcePrivateShmName));
        sss->sources[i].heap_relid = (Oid) intVal(list_nth(src, ShmSourcePrivateHeapRelid));
        sss->sources[i].schema_string = strVal(list_nth(src, ShmSourcePrivateSchema));
        sss->sources[i].attnos = (List *) list_nth(src, ShmSourcePrivateAttnos);
        sss->sources[i].nproducers = 0;
        sss->sources[i].worker = NULL;
        i++;
    }
    return (Node *) sss;
}

/*
 * Reap the streaming workers when the scan's batch context is destroyed. This
 * fires on BOTH a normal end (EndCustomScan already reaped, so this is a no-op)
 * AND on transaction abort / query cancel, where EndCustomScan is NOT called --
 * so a cancelled mid-scan offload never leaves a worker, fd, /dev/shm object, or
 * control socket behind. The worker handle is allocated in this same context, so
 * it is still valid when the reset callback runs (callbacks fire before the
 * context's memory is freed).
 */
static void
shm_worker_cleanup_cb(void *arg)
{
    ShmScanState *sss = (ShmScanState *) arg;
    int i;

    for (i = 0; i < sss->nsources; i++)
    {
        if (sss->sources[i].worker)
        {
            pgch_shm_worker_shutdown(sss->sources[i].worker);
            sss->sources[i].worker = NULL;
        }
    }
}

static void
shm_begin_custom_scan(CustomScanState *node, EState *estate, int eflags)
{
    ShmScanState *sss = (ShmScanState *) node;
    ForeignServer *server;
    UserMapping *user;

    if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
        return;

    /* Observability: this query is offloading to ClickHouse. */
    pgch_this_query_used_ch = true;

    server = GetForeignServerByName(pgch_local_ch_server, false);
    user = GetUserMapping(GetUserId(), server->serverid);
    sss->conn = chfdw_get_connection(user);

    /* Result rows are fetched into the scan tuple slot: the projected base-scan
     * tupdesc, or the explicit (custom_scan_tlist) tupdesc for a join/aggregate. */
    sss->tupdesc = node->ss.ss_ScanTupleSlot->tts_tupleDescriptor;
    sss->attinmeta = TupleDescGetAttInMetadata(sss->tupdesc);
    sss->batch_cxt = AllocSetContextCreate(estate->es_query_cxt,
                                           "pg_clickhouse shm scan", ALLOCSET_DEFAULT_SIZES);
    sss->temp_cxt = AllocSetContextCreate(estate->es_query_cxt,
                                          "pg_clickhouse shm scan tmp", ALLOCSET_SMALL_SIZES);

    /* Guarantee the workers are reaped even on query cancel / transaction abort,
     * where EndCustomScan is not called: a reset callback on batch_cxt fires when
     * the executor tears the context down for any reason. */
    sss->worker_cb.func = shm_worker_cleanup_cb;
    sss->worker_cb.arg = sss;
    MemoryContextRegisterResetCallback(sss->batch_cxt, &sss->worker_cb);

    /* The heap scan + columnize + ring publish runs in background workers (one
     * group per SHM source, launched on the first ExecCustomScan call once the
     * query snapshot is available), so the backend can drain the ClickHouse
     * result concurrently and peak shared memory stays bounded by the rings
     * regardless of table size. Per-source worker handles are NULL until then. */
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

/*
 * Decide how many cooperating streaming workers to launch for this offload from
 * PostgreSQL's own parallel-query budget -- there is no dedicated GUC. The count
 * is the relation's `parallel_workers` reloption when set, otherwise the same
 * geometric growth over `min_parallel_table_scan_size` that core's
 * compute_parallel_worker() applies to a sequential scan (one extra worker per
 * ~3x). It is then clamped to:
 *   - max_parallel_workers_per_gather (the per-query degree of parallelism; 0
 *     disables query parallelism and so forces a single producer),
 *   - max_parallel_workers (the cluster-wide pool),
 *   - PGCH_SHM_MAX_STREAM_WORKERS (the producer's control-socket parking ceiling),
 *   - the number of heap blocks to scan,
 *   - and a floor of 1.
 * The chosen count is also forced as ClickHouse max_threads
 * (shm_settings_force_max_threads), so producers and consumer threads stay matched.
 *
 * Fail-closed to a single producer when the scan is not eligible for the parallel
 * vectorized reader (the scalar fallback is single-producer).
 *
 * `cap_remaining` is the worker budget still available across the remaining SHM
 * sources of this scan (a join streams several): the chosen count is clamped to
 * it so a multi-source join cannot oversubscribe the shared worker pool. A
 * source always gets at least one producer (each source needs its own ring), so
 * a depleted budget still yields a single-producer stream rather than zero.
 */
static int
shm_choose_stream_workers(Oid heap_relid, List *attnos, Snapshot snapshot,
                          int cap_remaining)
{
    Relation          rel;
    ShmOffloadColumn *cols;
    int               ncols;
    BlockNumber       nblocks;
    int               reloption;
    int               w;
    int               cap;

    rel = table_open(heap_relid, AccessShareLock);
    ncols = pgch_build_offload_columns(rel, attnos, &cols);

    /* Not eligible for the vectorized reader -> scalar path -> single producer. */
    if (!pgch_vectorized_reader_eligible(rel, snapshot, cols, ncols))
    {
        table_close(rel, AccessShareLock);
        return 1;
    }

    /* Per-query degree of parallelism. 0 (parallel query disabled) -> single producer. */
    cap = max_parallel_workers_per_gather;
    if (cap < 1)
    {
        table_close(rel, AccessShareLock);
        return 1;
    }

    nblocks = RelationGetNumberOfBlocks(rel);

    /*
     * Worker count, mirroring compute_parallel_worker(): the per-table
     * `parallel_workers` reloption wins if set (>= 0), else one extra worker per
     * ~3x over min_parallel_table_scan_size.
     */
    reloption = RelationGetParallelWorkers(rel, -1);
    if (reloption >= 0)
        w = reloption;
    else
    {
        int threshold = Max(min_parallel_table_scan_size, 1);

        w = 1;
        while (nblocks >= (BlockNumber) (threshold * 3))
        {
            w++;
            if (threshold > INT_MAX / 3)
                break;
            threshold *= 3;
        }
    }

    /* Clamp to the per-query DOP, the cluster pool, the SHM parking ceiling, the
     * blocks available, and a floor of 1. */
    if (w > cap)
        w = cap;
    if (max_parallel_workers > 0 && w > max_parallel_workers)
        w = max_parallel_workers;
    if (w > PGCH_SHM_MAX_STREAM_WORKERS)
        w = PGCH_SHM_MAX_STREAM_WORKERS;
    /* Clamp to the remaining shared budget, but never below one (each source
     * needs its own ring), so a depleted budget yields a single producer. */
    if (cap_remaining < 1)
        cap_remaining = 1;
    if (w > cap_remaining)
        w = cap_remaining;
    if (nblocks > 0 && (BlockNumber) w > nblocks)
        w = (int) nblocks;
    if (w < 1)
        w = 1;

    table_close(rel, AccessShareLock);
    return w;
}

/*
 * Build the ClickHouse session settings for an SHM-offload query with
 * max_threads forced to the producer worker count `nworkers`: copy every
 * user-supplied setting except any existing max_threads, then append
 * max_threads = nworkers. Documented precedence: shm_stream_workers
 * unconditionally overrides a user-supplied session_settings max_threads, so the
 * consumer's thread count always matches the producer parallelism.
 */
static const kv_list *
shm_settings_force_max_threads(int nworkers)
{
    const kv_list *base = chfdw_get_session_settings();
    List          *items = NIL;
    kv_iter        it;

    for (it = new_kv_iter(base); !kv_iter_done(&it); kv_iter_next(&it))
    {
        if (pg_strcasecmp(it.name, "max_threads") == 0)
            continue;       /* drop: shm_stream_workers wins */
        items = lappend(items, makeDefElem(pstrdup(it.name),
                                           (Node *) makeString(pstrdup(it.value)), -1));
    }
    items = lappend(items, makeDefElem(pstrdup("max_threads"),
                                       (Node *) makeString(psprintf("%d", nworkers)), -1));

    return new_kv_list_from_pg_list(items, kv_pair_palloc);
}

/*
 * Rewrite the offload SQL to read from W independent rings instead of one.
 * The deparser emitted a single `streamed_table('<base>', '<schema>')`; replace
 * that exact reference with a UNION ALL of W per-worker rings
 * `(SELECT * FROM streamed_table('<base>_0','<schema>') UNION ALL ...)`.
 * Each streamed_table() call is its own ClickHouse source, so the consumer reads
 * the W rings as W PARALLEL streams (one PollableShmSource each) -- which is what
 * lifts the single-source bottleneck. Worker w creates the matching ring
 * "<base>_<w>". W=1 yields a single-source subquery (semantically identical).
 */
static char *
shm_build_union_sql(const char *sql, const char *base, const char *schema, int nworkers)
{
    char         *needle = psprintf("streamed_table(%s, %s)",
                                    ch_quote_literal(base), ch_quote_literal(schema));
    const char   *pos = strstr(sql, needle);
    StringInfoData out;
    int           w;

    if (pos == NULL)
        ereport(ERROR,
                (errmsg("pg_clickhouse: could not locate streamed_table() in the SHM-offload SQL")));

    initStringInfo(&out);
    appendBinaryStringInfo(&out, sql, pos - sql);          /* everything before the source */
    appendStringInfoChar(&out, '(');
    for (w = 0; w < nworkers; w++)
    {
        char *name = psprintf("%s_%d", base, w);

        if (w > 0)
            appendStringInfoString(&out, " UNION ALL ");
        appendStringInfo(&out, "SELECT * FROM streamed_table(%s, %s)",
                         ch_quote_literal(name), ch_quote_literal(schema));
        pfree(name);
    }
    appendStringInfoChar(&out, ')');
    appendStringInfoString(&out, pos + strlen(needle));    /* everything after the source */

    pfree(needle);
    return out.data;
}

/* ExecScan access method: produce the next raw scan tuple. On the first call it
 * launches the streaming background workers and dispatches the ClickHouse query;
 * the workers fill their rings while ClickHouse drains them concurrently. */
static TupleTableSlot *
shm_scan_access_mtd(ScanState *ss)
{
    ShmScanState *sss = (ShmScanState *) ss;   /* ss is at offset 0 of CustomScanState */
    TupleTableSlot *slot = ss->ss_ScanTupleSlot;
    bool got;

    if (!sss->dispatched)
    {
        EState *estate = ss->ps.state;
        MemoryContext old;
        int total_producers = 0;
        int budget;
        int i;

        /* Shared worker budget across this scan's SHM sources (a join streams one
         * per leaf): cap the running total to the producer parking ceiling and the
         * cluster-wide parallel pool so a multi-source join can't oversubscribe. */
        budget = PGCH_SHM_MAX_STREAM_WORKERS;
        if (max_parallel_workers > 0 && budget > max_parallel_workers)
            budget = max_parallel_workers;

        /* 1. For each SHM source, decide producer parallelism (fail-closed to 1
         *    if the relation is not eligible for the parallel vectorized reader),
         *    then launch that many cooperating workers to stream the relation into
         *    its own bounded ring(s) under the query snapshot. Allocate the handle
         *    in batch_cxt so the reset-callback reap (which fires on cancel/abort)
         *    sees a still-valid handle. */
        for (i = 0; i < sss->nsources; i++)
        {
            ShmScanSource *src = &sss->sources[i];
            int            nworkers;

            nworkers = shm_choose_stream_workers(src->heap_relid, src->attnos,
                                                 estate->es_snapshot, budget);
            src->nproducers = nworkers;
            total_producers += nworkers;
            budget -= nworkers;

            old = MemoryContextSwitchTo(sss->batch_cxt);
            src->worker = pgch_shm_worker_launch(src->shm_name, src->heap_relid,
                                                 src->attnos, estate->es_snapshot,
                                                 nworkers);
            MemoryContextSwitchTo(old);
        }

        /* Wait for every source's workers to create/attach their SHM producers so
         * the ClickHouse consumer can attach to all streamed_table() sources. */
        for (i = 0; i < sss->nsources; i++)
            pgch_shm_worker_wait_ready(sss->sources[i].worker);

        /* 2. Dispatch the ClickHouse query; it attaches to the SHM streams and
         *    drains them concurrently with the workers filling the rings. Each
         *    source's single streamed_table('<shm_name>', ...) reference is
         *    rewritten into a UNION ALL over its per-worker rings; the per-source
         *    shm_name makes each rewrite target unique so they compose. Force the
         *    consumer's max_threads to the total producer count across sources. */
        old = MemoryContextSwitchTo(sss->batch_cxt);
        {
            char    *union_sql = sss->sql;

            for (i = 0; i < sss->nsources; i++)
                union_sql = shm_build_union_sql(union_sql, sss->sources[i].shm_name,
                                                sss->sources[i].schema_string,
                                                sss->sources[i].nproducers);

            {
            ch_query query = new_query(union_sql, 0, NULL, sss->tupdesc, sss->retrieved_attrs);

            query.settings = shm_settings_force_max_threads(total_producers);

            sss->is_streaming = sss->fetch_size > 0 && sss->conn.methods->streaming_query != NULL;
            if (sss->is_streaming)
                sss->cursor = sss->conn.methods->streaming_query(sss->conn.conn, &query, sss->fetch_size);
            else
                sss->cursor = sss->conn.methods->simple_query(sss->conn.conn, &query);
            }
        }
        MemoryContextSwitchTo(old);
        sss->dispatched = true;
    }

    /* If ClickHouse errors because a worker died mid-stream, surface the worker's
     * real error (e.g. an out-of-domain numeric) rather than an opaque
     * producer-death error. Check every source's worker group. */
    PG_TRY();
    {
        got = shm_fetch_into_slot(sss, slot);
    }
    PG_CATCH();
    {
        int i;

        for (i = 0; i < sss->nsources; i++)
            pgch_shm_worker_check_error(sss->sources[i].worker);
        PG_RE_THROW();
    }
    PG_END_TRY();

    if (!got)
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
    int i;

    if (sss->cursor)
    {
        MemoryContextDelete(sss->cursor->memcxt);
        sss->cursor = NULL;
    }
    /* Stop each source's streaming workers (SIGTERM + wait) and detach the DSM
     * segment. On a normal finish the workers have already exited and this just
     * reaps them; on cancel/error it tears them down so no worker, fd, /dev/shm
     * object, or control socket outlives the query. */
    for (i = 0; i < sss->nsources; i++)
    {
        if (sss->sources[i].worker)
        {
            pgch_shm_worker_shutdown(sss->sources[i].worker);
            sss->sources[i].worker = NULL;
        }
    }
}

static void
shm_rescan_custom_scan(CustomScanState *node)
{
    ShmScanState *sss = (ShmScanState *) node;
    int i;

    /* Phase 1: a rescan re-streams from scratch. Tear down and re-arm. */
    if (sss->cursor)
    {
        MemoryContextDelete(sss->cursor->memcxt);
        sss->cursor = NULL;
    }
    for (i = 0; i < sss->nsources; i++)
    {
        if (sss->sources[i].worker)
        {
            pgch_shm_worker_shutdown(sss->sources[i].worker);
            sss->sources[i].worker = NULL;
        }
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

    prev_set_join_pathlist_hook = set_join_pathlist_hook;
    set_join_pathlist_hook = shm_set_join_pathlist;

    prev_create_upper_paths_hook = create_upper_paths_hook;
    create_upper_paths_hook = shm_create_upper_paths;

    prev_ExecutorStart_hook = ExecutorStart_hook;
    ExecutorStart_hook = shm_ExecutorStart;

    prev_ExecutorEnd_hook = ExecutorEnd_hook;
    ExecutorEnd_hook = shm_ExecutorEnd;
}
