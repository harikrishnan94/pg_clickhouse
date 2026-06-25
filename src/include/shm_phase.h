/*-------------------------------------------------------------------------
 *
 * shm_phase.h
 *      Per-worker producer-phase stopwatch for the SHM-offload reader.
 *
 *      Partitions a single stream worker's wall AND CPU time across the four
 *      producer phases READ / DEFORM / PUBLISH / PUBLISH_STALL, so the producer
 *      cost can be decomposed (benchmark instrumentation only).
 *
 *      Model: a single-threaded "stopwatch". The worker is single-threaded over
 *      its own ring, so exactly one phase is current at any instant. Each
 *      boundary call charges the elapsed wall+CPU since the previous boundary to
 *      the phase that was current, then switches. Because every instant between
 *      pgch_phase_begin() and the final pgch_phase_switch() is attributed to
 *      exactly one phase, sum(phase wall) == total wall and sum(phase CPU) ==
 *      total CPU of the timed region BY CONSTRUCTION -- which is the internal
 *      self-consistency the convergence gate G1 checks against getrusage.
 *
 *      Phase boundaries:
 *        - READ:   set in the page reader around heap read + visibility classify.
 *        - DEFORM: set after the page lock is dropped, around columnar deform.
 *        - PUBLISH/PUBLISH_STALL: set inside publish_block (the ring-full
 *          pg_usleep wait is STALL -- idle time waiting on the consumer, NOT
 *          producer work; the memcpy of the built columns into the slot is
 *          PUBLISH). publish_block save/restores the caller's phase so a publish
 *          nested inside DEFORM (via the columnizer) returns to DEFORM.
 *
 *      CPU clock = CLOCK_THREAD_CPUTIME_ID (this worker only; advances ~0 while
 *      the worker sleeps in the STALL wait). Wall clock = CLOCK_MONOTONIC.
 *
 *      Gated on `enabled` (the shm_log_stream_stats GUC): when off, every call is
 *      an early-return, so the headline timed runs pay nothing.
 *
 * Copyright (c) 2025-2026, ClickHouse, Inc.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_CLICKHOUSE_SHM_PHASE_H
#define PG_CLICKHOUSE_SHM_PHASE_H

#include "postgres.h"

#include <string.h>
#include <time.h>

typedef enum PgchPhase
{
    PGCH_PH_READ = 0,       /* heap page read + tuple visibility classify */
    PGCH_PH_DEFORM = 1,     /* tuple -> columnar deform / columnize */
    PGCH_PH_PUBLISH = 2,    /* publish_block memcpy into the SHM ring slot */
    PGCH_PH_STALL = 3,      /* ring-full backpressure wait (idle, not work) */
    PGCH_PH_N = 4,
} PgchPhase;

typedef struct PgchPhaseTimers
{
    uint64 cpu_ns[PGCH_PH_N];   /* CLOCK_THREAD_CPUTIME_ID ns per phase */
    uint64 wall_ns[PGCH_PH_N];  /* CLOCK_MONOTONIC ns per phase */
    int    cur;                 /* phase currently being charged */
    uint64 last_cpu;            /* clock readings at the last boundary */
    uint64 last_wall;
    bool   enabled;             /* mirror of pgch_log_stream_stats */
} PgchPhaseTimers;

static inline uint64
pgch_phase_now_ns(clockid_t clk)
{
    struct timespec ts;

    clock_gettime(clk, &ts);
    return (uint64) ts.tv_sec * UINT64CONST(1000000000) + (uint64) ts.tv_nsec;
}

/* Zero the accumulators and start the stopwatch in `start`. No-op if disabled. */
static inline void
pgch_phase_begin(PgchPhaseTimers *t, int start)
{
    if (t == NULL || !t->enabled)
        return;
    memset(t->cpu_ns, 0, sizeof(t->cpu_ns));
    memset(t->wall_ns, 0, sizeof(t->wall_ns));
    t->cur = start;
    t->last_cpu = pgch_phase_now_ns(CLOCK_THREAD_CPUTIME_ID);
    t->last_wall = pgch_phase_now_ns(CLOCK_MONOTONIC);
}

/* Charge elapsed wall+CPU since the last boundary to t->cur, then switch to `next`.
 * Calling with next == t->cur simply flushes the trailing segment. No-op if disabled. */
static inline void
pgch_phase_switch(PgchPhaseTimers *t, int next)
{
    uint64 c, w;

    if (t == NULL || !t->enabled)
        return;
    c = pgch_phase_now_ns(CLOCK_THREAD_CPUTIME_ID);
    w = pgch_phase_now_ns(CLOCK_MONOTONIC);
    t->cpu_ns[t->cur] += c - t->last_cpu;
    t->wall_ns[t->cur] += w - t->last_wall;
    t->last_cpu = c;
    t->last_wall = w;
    t->cur = next;
}

#endif /* PG_CLICKHOUSE_SHM_PHASE_H */
