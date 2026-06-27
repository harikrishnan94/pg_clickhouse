# Hot/Cold ClickBench-100M — DECISIONS (D-HC-04##)

Continues the `dev/hotcold/DECISIONS.md` series (last id there: D-HC-0303). Template per task §14.

---

### D-HC-0401 — Oracle re-basing: pure-CH-100M is the 100M ground truth; native-PG-100M & full-offload-100M are infeasible to measure — 2026-06-27  [HIGH-IMPACT]
- Context: §5/§8 demand "merged hot/cold == native-PG over full 100M". But a PG heap for 100M ClickBench
  rows ≈ 70–75 GiB (measured: 10M pg.hits = 6961 MB → 100M ≈ 70 GiB) > 66 GiB free. native-PG-100M and
  full-offload-100M (both need 100M rows resident in PG) are physically infeasible on this host. §13 itself says so.
- Options considered: (a) load 100M into PG anyway (impossible — disk); (b) drop CH cold store, load PG 100M,
  measure, reload CH (impossible — 75 GiB still > free, plus huge re-download); (c) re-base the oracle: use
  pure-CH-100M as the 100M ground truth and prove CH-template fidelity to native-PG semantics at the feasible
  10M scale.
- Criteria: correctness floor must remain demonstrable; baselines must be honest; no fabricated 100M numbers.
- Chosen option: (c). Primary MEASURED comparison at 100M = hot/cold-merge vs pure-CH-100M (`SELECT <agg> FROM
  hits_100m`). pure-CH-100M is the correctness ground truth; the merged result must equal it within fidelity
  bounds F1/F3/F5 + top-N tiebreak. CH-template fidelity to native-PG is established at 10M (the existing 42/43
  ClickBench match, dev/clickbench/FULL-OFFLOAD-RESULTS.md). native-PG and full-offload are MEASURED at 10M and
  presented to 100M only as clearly-labeled linear PROJECTIONS — never as measured 100M numbers (§9.6 ban honored).
- Evidence: pg.hits (10M) = 6961 MB; df free = 66 GiB; uniqExact/sort over 100M OOMs at the CH ~19 GiB live cap.
- Risks / tradeoffs: the headline "merge ≈ pure-CH" tests the COMPUTE/overlap thesis, not literally "beats native PG
  at 100M" (that is projected). Disclosed in REPORT. Reviewer (Agent C) must confirm projections are labeled.
- Revisit trigger: a 2nd host or ≥150 GiB free disk becomes available → measure native-PG-100M directly.

### D-HC-0402 — Merge architecture: standalone producer + direct merged CH query (no planner routing, no C change) — 2026-06-27
- Context: need a SINGLE CH execution computing `streamed_table(hot) UNION ALL (cold CH table)` with the hot
  transfer overlapping the cold scan.
- Options considered: (a) PG planner fuses `(foreign cold) UNION ALL (heap hot)` — REFUTED by spike (yields a
  PG-side Append+Aggregate, two independent arms, no streamed_table, no overlap — L0033); (b) modify the deparser
  to inject a cold UNION (C change + "routing layer", out of scope §8); (c) launch a standalone PG producer via
  the SQL function `clickhouse_stream_relation(rel,shm_name,rows_per_block)` and issue the merged query directly
  to CH.
- Criteria: single fused CH execution; no C/planner changes (§8); minimal, reviewable harness.
- Chosen option: (c). PROVEN by spike (L0033): a background psql runs clickhouse_stream_relation (creates the SHM
  ring + control socket, streams all rows, waits for the consumer); the harness then issues the merged query to CH
  (HTTP, allow_experimental_streamed_table_function=1). Returned the correct merged aggregate (93|5).
- Evidence: spike artifacts in METHODOLOGY-LOG L0033.
- Risks / tradeoffs: clickhouse_stream_relation is a SINGLE producer (W=1, SHM-adopt only) → hot-side has no
  parallel scan and over-streams all 105 columns (see D-HC-0405). Both are conservative (make the merge look
  worse, not better).
- Revisit trigger: a parallel/projecting standalone producer becomes available, or f=10% hot dominates at high W.

### D-HC-0403 — Split predicate: exact lexicographic recency boundary on the UNIQUE 5-tuple (supersedes the hc_row_id rank plan) — 2026-06-27
- Context: EventTime is `DateTime` (second granularity) → mass ties → a pure-timestamp cutoff cannot give an exact
  disjoint 1/5/10% split. Initial plan was a materialized `hc_row_id = row_number()` rank, but the global window
  sort of 100M × 105 cols OOMs at the CH ~19 GiB live cap (L0034).
- Options considered: (a) hc_row_id global window rank (OOM); (b) memory-frugal phys_id + narrow-rank + join
  (3 phases, heavy disk churn, ends in a wide re-sort — also OOM-prone); (c) boundary predicate on a unique tuple.
- Criteria: EXACT counts (count(hot)=N), provable disjointness, no OOM, leave hits_100m untouched (fair baseline).
- Chosen option: (c). The 5-tuple (EventTime,WatchID,UserID,CounterID,EventDate) is PROVEN unique over the
  99,997,497 rows (uniqExact == count, L0034). Recency order = all-DESC. hot(f) = tuple >= B(f), cold(f) = tuple <
  B(f), where B(f) is the N(f)-th most-recent row's tuple (boundaries.env). Verified EXACT for all three f
  (hot==N, hot+cold==total). No rank column, no heavy build; hits_100m stays canonical.
- Evidence: L0034 — uniqExact(5-tuple)=99,997,497; per-f hot/cold counts verdict EXACT.
- Risks / tradeoffs: the cold predicate is a lexicographic OR-chain / tuple compare, not a PK range → cold arm
  full-scans + filters hits_100m (acceptable; matches the task template "CH hits WHERE EventTime < T").
- Revisit trigger: a query needs cold-only scan efficiency (would require a materialized per-f cold table).

### D-HC-0404 — Cold arm = filtered subquery over hits_100m (no per-f materialization); pure-CH-100M = full scan of hits_100m — 2026-06-27
- Context: cold(f) holds 90–99M rows; materializing 3 cold tables ≈ 40 GiB (disk-prohibitive).
- Options considered: (a) materialize per-f cold tables (40 GiB); (b) cold = `hits_100m WHERE tuple < B(f)`
  filtered subquery (0 extra disk).
- Criteria: disk budget; fair vs pure-CH (same table, same layout).
- Chosen option: (b). pure-CH-100M baseline = `SELECT <agg> FROM hits_100m`; merge cold arm = `... FROM hits_100m
  WHERE tuple < B(f) ...`. Both scan the same canonical table → fair; the merge additionally pays the cold filter
  + the hot union (so merge ≥ pure-CH by construction — honest).
- Evidence: disk math (cold tables ≈ 40 GiB > headroom).
- Risks / tradeoffs: cold arm reads all 100M then filters (the hot N are dropped) — marginally more than a
  90–99M cold-only table; the filter cost is small vs scan+agg. Documented.
- Revisit trigger: cold filter overhead proves material (>noise) vs pure-CH → consider materializing cold.

### D-HC-0405 — Hot side single-producer (W=1), over-streams all 105 columns — accepted as the conservative case — 2026-06-27
- Context: clickhouse_stream_relation is single-threaded and streams every column of the regclass.
- Options considered: (a) build W parallel hot producers over W hot sub-tables (complex per-(f,W) setup);
  (b) accept single-producer, all-column hot streaming.
- Criteria: POC simplicity; the penalty must be conservative (not flatter the merge).
- Chosen option: (b). The hot transfer is single-threaded and carries all 105 columns even when a query needs
  few — both INFLATE the hot-transfer time, making the overlap HARDER to achieve. If overlap holds anyway, the
  thesis is stronger. The W-sweep axis is therefore the shared CPU-core cap (cgroup) + CH max_threads (the cold
  long-pole), with hot fixed at one producer.
- Evidence: src/shm_offload.c:1051 (clickhouse_stream_relation streams all columns of the relation).
- Risks / tradeoffs: at f=10% and high W (short cold scan) the single all-column hot stream may not hide →
  merge slower than pure-CH for light queries. Reported per query (losses included).
- Revisit trigger: Unit-1 shows hot-transfer dominating for the heavy-query majority → build a parallel/projected hot producer.

### D-HC-0406 — Data-prep ran with CH temporarily uncapped (leftover sweep cgroup); benchmark cgroup re-established fresh per cell — 2026-06-27
- Context: the live CH server was found inside a stale cgroup `/pgch_rep_tpch` capping it to 8 CPU cores
  (cpu.max "800000 100000") from a prior sweep, and CH's memory tracker throttled queries to ~free system RAM.
- Options considered: (a) leave it (slows data prep, distorts later baselines); (b) uncap CPU for data prep and
  re-establish the proper shared cgroup (PG tree + CH) fresh at benchmark time per §9.5.
- Criteria: data prep speed; benchmark fairness (the cap must be applied identically and freshly).
- Chosen option: (b). Set cpu.max=max on /pgch_rep_tpch for the build phase. Unit-1 will (re)create the shared
  cpu cap holding BOTH the PG postmaster tree and the live CH PID per cell, per wsweep_split.sh convention.
- Evidence: /proc/<CHPID>/cgroup = /pgch_rep_tpch; cpu.max was "800000 100000".
- Risks / tradeoffs: none for prep; benchmark cells MUST re-apply the cap (tracked in Unit-1 harness).
- Revisit trigger: any benchmark cell run without re-confirming the cap → void that cell.

### D-HC-0407 — Top-N LIMIT-boundary tie reshuffle accepted (proven benign), not excluded — 2026-06-28
- Context: 14 oracle cells (q18,q25,q31,q32,q39,q40 across fractions) returned cmp class DIFF — all LIMIT/top-N
  queries with ambiguous LIMIT boundaries (tied ORDER BY keys), and the failures were inconsistent across fractions.
- Options considered: (a) exclude these queries; (b) accept as the documented top-N tiebreak deviation if proven
  benign; (c) declare a merge bug.
- Criteria: §7 accepts "top-N deterministic-tiebreak reshuffle"; must PROVE benign (not assume).
- Chosen option: (b). 07_verify_topn.sh re-ran each with a deterministic TOTAL order appended → 18/18 EXACT
  (pure-CH == merge). The merge result multiset is identical to pure-CH; only WHICH tied rows survive the LIMIT
  differs (non-deterministic in CH itself, partition-dependent). These remain ELIGIBLE and correct.
- Evidence: results/topn/summary.tsv (18/18 exact); mirrors repo D0006 / dev/clickbench/tiebreak_check.sh.
- Risks / tradeoffs: the headline benchmark for these 6 queries should note the LIMIT result is tie-ambiguous in
  both pure-CH and merge (a property of the query, not the offload).
- Revisit trigger: any top-N query whose tiebroken comparison is NOT exact → that would be a real merge bug.
