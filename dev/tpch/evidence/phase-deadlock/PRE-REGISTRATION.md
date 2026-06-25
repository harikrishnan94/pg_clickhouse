# Phase A (Reproduce/diagnose) — PRE-REGISTRATION

Date: 2026-06-25. Author: unattended agent. Written BEFORE measuring.

## Scope + acceptance criteria (Phase A)
Reproduce the Q8/Q9/Q11/Q14 hash-join-BUILD deadlock with a minimal 2-source repro
(`streamed_table(lineitem) ALL INNER JOIN streamed_table(part)`), and root-cause the
mechanism with >=2 independent, converging evidence classes:
- Liveness: CH `system.processes.read_rows` frozen while `elapsed` climbs; producer
  publication progress / slot states.
- Thread state at freeze: CH thread stacks (gdb) + PG stream-worker states, showing
  WHICH operator/source each thread is parked in (the wait cycle).
- Pipeline structure: EXPLAIN PIPELINE for a deadlocking vs a working join.
Phase A is GREEN only when the mechanism is proven and explains why this 2-way shape
deadlocks while 3-6-way joins (Q3/Q5/Q7/Q10) do not, and an independent adversarial
review passes.

## Environment
- PG: `sudo -u postgres psql -d tpch_sf10`, schema pg, SF10.
- CH: RUN_ID=tpchcb, HTTP 127.0.0.1:21000 (manifest; spec's :21002 is STALE),
  server child pid 2673848 (resolve live via `ss -ltnp`).
- SHM ring geometry: PGCH_SHM_RING_DEPTH_K = 4 slots/ring; producer publish_block
  blocks on `slot->state == EMPTY`. Consumer (PollableShmSource) sets EMPTY only on
  LAST RetainToken drop. Consumer stall = shm_source_stall_timeout_ms = 30000ms.

## Hypothesised mechanisms (competing) + predicted observables

### H1 — Ring-capacity retention by the hash-join build (zero-copy hold)
Mechanism: ClickHouse HashJoin BUILD stores build blocks in its hash table. With
zero-copy adoption, each stored block keeps a RetainToken alias alive, so the
consumed slot never returns to EMPTY. With only K=4 ring slots, once the build has
adopted/held the ring's worth of blocks, the producer blocks in publish_block
waiting for EMPTY → no further publication → read_rows freezes → stall (Code 781).
Predicted observables IF H1:
- At freeze: ALL 4 build-side (`part`) ring slots are state==PUBLISHED with
  retain_refcount>0 (held by the hash table).
- The build-side PG stream worker is spinning in `publish_block` `while(state!=EMPTY)`.
- read_rows on the part source ≈ 4 blocks (one ring) then frozen.
- CH executor threads NOT all parked — at least the build path made progress to 4
  blocks; the hash join is mid-build, holding adopted columns.
- Predict: a working 3-6-way join's build side fits within <=4 ring blocks (small
  dimension tables) so it never exhausts the ring.

### H2 — Pipeline-executor thread starvation / build-probe scheduling barrier
Mechanism: The 2-way pipeline parks all executor threads on Async sources (e.g. the
lineitem PROBE source filling its ring and going Async), leaving no thread to drain
the build, OR a structural ordering blocks the build after 1 block.
Predicted observables IF H2:
- At freeze: read_rows frozen at ~1 block (not 4) on the build side.
- ALL CH pipeline-executor threads parked (futex/epoll wait); none running the build.
- Both producers (lineitem + part) stalled in publish_block.

## Discriminator
The number of build-side blocks consumed at freeze (read_rows ≈ 1 block vs ≈ 4
blocks = full ring), the build-side ring slot states + retain_refcounts, and whether
any CH thread is actively running the build vs all parked. The prior session note
says "stuck at exactly ONE ring block" and a parallel_workers=1 run "stuck at 262144
on the part build side" (one block) — this leans H2/refinement, but H1 (held ring)
is the simplest and must be checked directly via slot states. I will measure slot
states + thread stacks to decide; a prediction/result mismatch is a finding to chase.

## Method
1. Confirm clean baseline (0 rings, 0 workers).
2. Run Q14 offload ON in background (PG statement_timeout=120s backstop; default 30s
   consumer stall). Poll CH system.processes read_rows every 2s → freeze log.
3. While frozen (<30s), dump CH thread stacks (gdb -p 2673848), PG pg_stat_activity
   for stream workers, and read the part-source ring slot states from /dev/shm.
4. Capture EXPLAIN PIPELINE for Q14 (deadlock) and Q5 (working) under offload.
5. Verify clean teardown after the stall error.
