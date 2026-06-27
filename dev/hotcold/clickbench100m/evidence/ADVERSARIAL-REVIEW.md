# Hot/Cold ClickBench-100M — Adversarial Review (synthesis)

Per task §12. Reviewers A–E ran as 5 independent isolated subagents (read-only; no producers / no script
re-runs / no mutations), each given only this task's evidence standard + the Unit-0 acceptance criteria + the
committed artifact. This synthesis merges duplicates and resolves cross-axis conflicts ON EVIDENCE (the side
meeting §9 wins; correctness is not traded for speed). LEADs are not promoted to findings.

## UNIT 0 — review round 1 (after the first green oracle: 112 exact + 14 proven-benign top-N)

### ACCEPTED (severity-ordered)

**[BLOCKING → FIXED] A1 — cold/pure timestamp arm disagreed with the hot arm by the +5:30 server-TZ offset.**
- Axis: Correctness. Sources (≥3, converging): (i) code — hot arm streams the PG naive wall-clock as
  `DateTime64(6,'UTC')` while cold/pure used `toDateTime64(EventTime,6,'UTC')` which RELABELS the CH instant
  (server TZ = Asia/Kolkata); (ii) raw row — WatchID 9223361798749042077: hits_100m display `2013-08-01 01:01:07`,
  PG hot heap `2013-08-01 01:01:07`, but pure-CH `hits_dt64` `2013-07-31 19:31:07`; (iii) 100M counterfactual —
  Agent A measured a `toMinute=0` bucket of 1,667,466 (pure) vs 1,672,481 (merge) ≈ 5,015 rows shifted. Masked in
  all 5 eligible timestamp queries (q19,q24,q25,q27,q43) only by incidental row placement (ASC ordering / EventDate
  filters / cold-dominant groups) — so the green was partly luck for the timestamp class.
- RESOLUTION (L0040): `dt64_cast()` now uses `toDateTime64(toString(EventTime),6,'UTC')` — the server-TZ wall-clock
  relabeled UTC — identical to the streamed hot value AND to native-PG ClickBench semantics. Re-verified by 3
  independent sources: the row now reads `01:01:07`; the **10M fidelity bridge** native-PG `extract(minute)` ==
  pure-CH(fixed) `toMinute` top-5 IDENTICAL (settles A8); a **hot-reaching** minute histogram (buckets to
  `2013-08-01 01:29:00`) merge == pure-CH byte-identical. Full oracle re-run confirms (see round-1-fix verdict).

**[should-fix → FIXED] B1 — socket-poll name bug (06_bench.sh, 07_verify_topn.sh).** They polled
`/tmp/clickhouse_shm_pgch_${f}.sock` (missing `hot_`); the ring is `pgch_hot_${f}` → socket `..._hot_${f}.sock`.
Benign in 07 (9 s slack let the producer get ready anyway → 18/18 still exact) but it adds a dead ~8 s wait per
run and risks a too-early merge under load in Unit 1. Sources: code (06:51, 07:50) + src/shm_producer.c control
socket path + 05_oracle.sh:65 doing it right. FIXED: both poll `..._hot_${f}.sock`.

**[should-fix → FIXED] A5 — oracle empty-result false-PASS hazard.** `cmp_results.py` scores two empty files as
`exact|0|0`; combined with blk≥1 + rows==N a degenerate empty result could PASS. Did NOT fire (0 empty/0-exception
outputs in the committed run — LEAD per Agent A, confirmed by inspection). FIXED defensively: the verdict now
requires non-empty pure AND merge outputs and no `Exception` text.

**[minor → FIXED] A6/B6 — leak-check predicate precedence.** `backend_type ilike '%clickhouse%' OR query ilike
'%...%' AND pid<>pg_backend_pid()` — AND binds tighter, so the self-exclusion guarded only the 2nd disjunct. Did
not miscount (the counting backend matched neither branch). FIXED: parenthesized.

**[minor → FIXED] B2 — dead loop** in 05_oracle.sh (a `mk_merge_sql.py q5 … >/dev/null || true` no-op before the
partition check). FIXED: removed.

**[Unit-1 carry-forward] C1 — cold-arm filter reads 4 extra tuple columns.** For narrow queries the cold arm
reads UserID + the 4 boundary-tuple columns to evaluate `tuple<B`, vs pure-CH reading only projected columns
(Agent C measured q5: 762 MiB → 2.42 GiB read-bytes, +36% wall — a deterministic column-set delta, not noise; 15
eligible queries project a single column). Direction is CONSERVATIVE (disadvantages the merge) so it does not
flatter the thesis, but it co-varies with the pre-declared loss regime. ACTION (Unit 1): capture `read_bytes`
per cell and attribute narrow-query merge overhead to filter-column reads, NOT to failed overlap; D-HC-0404's
"filter cost is small" is corrected for narrow queries. (Optional mitigation: a precomputed per-f partition
column reduces the cold filter to one column — defer unless it materially distorts.)

**[Unit-1 carry-forward, BLOCKING for Unit-1 green] C2 — the pre-registered 2nd overlap instrument is dead on
this architecture.** `clickhouse_stream_relation` passes `out_stats=NULL` and runs inline (no worker), and the
`shm phase` elog is worker-only — so `shm_log_stream_stats` emits NOTHING; 06_bench's phase-split parse records all
zeros. Sources: shm_offload.c:1115 (NULL), shm_worker.c:~614 (worker-only elog), historic 0 phase lines. ACTION
(before Unit 1): replace the dead phase-split with a real 2nd producer-side instrument — producer active-window
(clickhouse_stream_relation start/end) vs the merge query window, and/or `/proc/<prodpid>/stat` utime+stime — so
overlap rests on ≥2 independent instruments (wall decomposition + CH query_duration + producer window), not one.

**[Unit-1 carry-forward] C3 — W=1 is a CPU-starvation artifact.** One shared core for producer+consumer makes
wall-overlap structurally impossible at W=1; report W=1 as a cap point, not as evidence against the mechanism.

**[Unit-1 carry-forward] C4 — high-cardinality GROUP BY (q32/q33) spills to disk** at the 4 GB external threshold
→ higher σ. ACTION: capture spill ProfileEvents, flag spilling cells, confirm median-of-N≥5 controls σ.

**[Unit-3 carry-forward] E2 — the dead-consumer liveness guard (`origin_pid`) is disabled on this producer path.**
Cross-process safety lives in the harness (statement_timeout + pg_cancel), not the C producer. Correct for the
orchestrated oracle (0 leaks observed); flagged for Units 2/3 where drains are longer.

**[should-fix → DONE] E4 — the "~19 GiB CH ceiling" understates the real config.** `max_server_memory_usage` ≈
26.4 GiB; `MemoryResidentMax` 22.19 GiB; a global OOM occurred on this host in a SIBLING tpch study (cgroup
`/pgch_mm_tpch`, not this study — attribution is a LEAD, the OOM-occurred fact is a FINDING). The 19 GiB figure was
an observed data-prep throttle, not the ceiling. ACTION: corrected in 10-REPRODUCTION.md; Unit 3 measures real
memory (VmHWM + cgroup memory.peak).

**[minor → DONE] B5/D-prereg — the pre-registration §1/§2 still describe the superseded hc_row_id/hits_idx split.**
Append-only doc → fixed via an Amendment pointer to D-HC-0403 (boundary-tuple split). 10-REPRODUCTION.md is already
boundary-based and self-consistent.

**[minor, documented] D1 — bench-common.sh not reused.** The harness re-rolls `chq`/`new_tag`/`shm_set_block` and
uses a GLOBAL `/dev/shm/pgch_*` leak count rather than the PID-scoped helper. Low risk here (single sequential
session, no concurrent benches), so KEPT for the POC; noted as an accepted deviation. (Reusing bench-common is a
reasonable future refactor.)

**[minor] D3 — per-iteration holistic note** is folded into each entry's "Learnings" line rather than a separate
"Holistic note" header (phase3 style). Will make the whole-system note explicit in Unit-1 entries.

**[minor] D4 — REPORT.md / evidence dir** were pending; this ADVERSARIAL-REVIEW.md creates `evidence/`. REPORT.md
is the closing cross-unit deliverable (written after Units 1–3).

### REJECTED
- **D2 (methodology-log entry format differs from phase3's bold-label style)** — REJECTED. The artifact follows the
  task's own §10 template (Goal/What/How/Verified/Result/Interpretation/Learnings/Verdict) verbatim; phase3's
  bold-label rendering is an older sibling style, not binding. No evidence of a §10 violation.
- **B4 (paren-aware projected_cols is defensive over-engineering)** — REJECTED as a change; kept. It is cheap and
  future-proofs DateTime64-projected schemas; a one-line "defensive" note suffices (added).
- Various LEADs (A5/A6 realized-impact, the OOM attribution in E4, C3/C4 magnitudes) — recorded as LEADs/carry-
  forwards, NOT promoted to findings (single source / not yet reproduced in-context).

### Cross-axis conflict resolution
- C's "fairness" concerns (C1) vs A's "correctness floor" (A1): no conflict — A1 (correctness) is fixed first and
  unconditionally; C1 is a Unit-1 attribution requirement, not a correctness defect. Correctness was not traded.

## BOTTOM LINE — Unit 0: **SHIP-WITH-FIXES (fixes applied; re-verification in progress)**
- The one BLOCKING finding (A1) is FIXED and re-verified by 3 independent sources incl. the 10M native-PG bridge
  and a hot-reaching probe. B1/A5/A6/B2 fixed; E4/B5 docs corrected; C1/C2/C3/C4/E2 carried to Units 1/3 with
  explicit actions (C2 is BLOCKING for **Unit-1** green — the 2nd overlap instrument must be real).
- No open blocking CORRECTNESS finding after the A1 fix. Unit 0 is GREEN once the post-fix full oracle + top-N
  verification confirm (112 exact + 14 proven-benign expected to hold by construction).
