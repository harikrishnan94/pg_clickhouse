# Hot/Cold ClickBench-100M — METHODOLOGY LOG

Append-only. Continues the repo's `L####` numbering (last used: L0030). Conventions per task §10.
Material claims require ≥3 independent converging sources; orientation/operational entries cite their source.

---

### L0031 — Discovery / orientation  [unit 0]  [discovery]  2026-06-27T23:20+05:30
- Goal / hypothesis: orient before pre-registration (§11) — env, harness, prior state, infra-green.
- What I did: verified env (branch streamed-table-shm-offload clean; 32-core aarch64, 61 GiB RAM, 66 GiB free disk);
  PG18 :5432 peer-auth `sudo -u postgres psql`; CH 26.6.1.1 RUN_ID=tpchcb HTTP:21002 (live PID via ss, manifest
  stale — D0013); FDW server ch_bench→127.0.0.1:21002. Inventoried CH (hits 10M, hits_100m 99,997,497, prior
  hits_cold_10_base 90M + view) and PG (pg.hits 10M heap 6961 MB; foreign ch.hits; NO 100M / hot in PG).
- How verified: OPERATIONAL — df/free/nproc/ss/ps; CH system.tables; PG pg_class/pg_foreign_server.
- Result: env-of-record recorded in 00-PRE-REGISTRATION §0. pg.hits 10M = 6961 MB ⇒ 100M ≈ 70 GiB (> 66 free).
- Interpretation: a prior uncommitted session loaded 100M + a partial cold split; must RE-VERIFY not trust (§4).
- Learnings: native-PG-100M physically infeasible here ⇒ oracle must re-base (→ D-HC-0401).
- Verdict: DONE (orientation).

### L0032 — Harness study (3 parallel Explore agents)  [unit 0]  [discovery]  2026-06-27T23:25+05:30
- Goal: understand dev/clickbench, dev/wsweep-report, dev/hotcold/phase3, verify_offload.sh, streamed_table().
- What I did: dispatched 3 Explore subagents; consolidated findings into the scratchpad.
- How verified: OPERATIONAL — agents returned file:line citations.
- Result (key): offload is deparser-driven (CustomScan ClickHouseShmScan); shm_set_block sets
  session_settings(allow_experimental_streamed_table_function 1, log_comment TAG)+enable_shm_offload; W via PG
  table opt parallel_workers; cgroup cap = mkdir /sys/fs/cgroup/pgch_*, echo PMPID+CHPID, cpu.max="W*100000
  100000"; phase split via shm_log_stream_stats LOG lines parsed by parse_phase awk; pushdown proof =
  query_log QueryFinish + streamed_table + ProfileEvents['ShmAdoptedBlocks']>=1 + read_rows; cmp_results.py tol
  1e-6 classes exact/float/approx/DIFF; 42/43 offload @10M (Q1 declines); tcp_send_delay_us microbench reusable for U2.
- Interpretation: I can REUSE the deparser to produce CH bodies; I do NOT need to hand-write CH ClickBench SQL.
- Verdict: DONE (orientation).

### L0033 — LINCHPIN spike: planner-fusion REFUTED; standalone producer + direct merged CH query PROVEN  [unit 0]  [discovery]  2026-06-27T23:30+05:30
- Goal / hypothesis: find a path to a SINGLE CH execution computing streamed_table(hot) UNION ALL (cold CH table).
- What I did: (a) created tiny PG heap spike_hot + foreign spike_cold(→CH); ran
  `SELECT sum(v) FROM (SELECT FROM spike_cold UNION ALL SELECT FROM spike_hot)` with enable_shm_offload=on.
  (b) launched a background `clickhouse_stream_relation('hits_hot_spike','pgch_spike2',65536)` and issued the
  merged query directly to CH (HTTP, allow_experimental_streamed_table_function=1).
- How verified (mechanism, 2 sources): (a) PG `EXPLAIN` + CH `query_log`; (b) CH result + producer return value.
- Result: (a) PG plan = `Aggregate -> Append -> (Foreign Scan spike_cold) + (Seq Scan spike_hot)`; CH saw only
  `SELECT v FROM clickbench.spike_cold` (ShmAdoptedBlocks=0). NO streamed_table, NO single CH query, NO overlap →
  planner-fusion REFUTED. (b) merged query returned the correct merged aggregate `93|5`; producer returned 2 rows.
- Interpretation: the POC must issue the merged query directly to CH with a standalone background producer (→ D-HC-0402).
- Learnings: clickhouse_stream_relation is W=1, SHM-adopt, streams ALL columns (→ D-HC-0405). Spike cleaned (no shm/socket/bgworker leaks).
- Verdict: DONE → architecture chosen.

### L0034 — Split design: hc_row_id rank OOMs; 5-tuple is UNIQUE ⇒ exact boundary predicate  [unit 0]  [iteration 1]  2026-06-28T00:05+05:30
- Goal / hypothesis: an EXACT, disjoint, total 1/5/10% recency split (EventTime is sec-granularity ⇒ a pure
  timestamp cutoff cannot be exact).
- What I did: (1) probe — `hc_row_id=rowNumberInAllBlocks()+1 over (ORDER BY EventTime DESC,…)` on 10M `hits`:
  EXACT bijection (count=min=max=uniqExact=10,000,000) in 55 s single-thread; hot(id<=1e5) = most recent (recency
  confirmed). (2) ran the 100M build (01_build_hits_idx.sh) → OOM "(total) memory limit exceeded: 19.73 GiB …
  MergeSortingTransform". Root cause: CH server stuck in stale cgroup /pgch_rep_tpch (cpu.max 8 cores) and memory
  tracker throttling to ~free RAM. (3) uncapped CH cpu.max + dropped pagecache; with external group-by, probed
  uniqueness: `uniqExact((EventTime,WatchID,UserID,CounterID,EventDate)) == count() == 99,997,497`. (4) computed
  boundary tuples B(1/5/10%) (N-th most-recent row) and verified hot/cold counts.
- How verified (≥3 sources for the EXACT-partition material claim): (i) uniqExact==count (5-tuple unique);
  (ii) countIf(tuple>=B)==N exactly for all three f; (iii) countIf(>=)+countIf(<)==count (total, disjoint);
  (iv) recency check (hot EventTimes most recent). Boundaries persisted in boundaries.env.
- Result: B(1%)=(2013-07-31 19:00:00, 6807085344353868475, 6613092639126911837, 3922, 2013-07-31) hot=1,000,000;
  B(5%)=(2013-07-29 21:09:11,…) hot=5,000,000; B(10%)=(2013-07-29 03:37:14,…) hot=10,000,000. All EXACT, all
  hot+cold=99,997,497.
- Interpretation: boundary-tuple split is exact AND leaves hits_100m canonical (fair pure-CH baseline) — strictly
  better than the rank build (no OOM, no extra table). → D-HC-0403/0404 supersede the hc_row_id plan.
- Learnings: the CH live memory ceiling (~19 GiB) forces external spill / capped aggregations on 100M; the leftover
  sweep cgroup must be reset (→ D-HC-0406). Failed 100M rank build (01_build_hits_idx.sh) left as a NULL-result artifact.
- Verdict: CONTINUE → build hot tables (CH+PG) from the boundary predicate.

### L0035 — Build hot subsets (CH + PG) + fidelity check (byte-vs-char false alarm)  [unit 0]  [iteration 1]  2026-06-28T00:20+05:30
- Goal: materialize the N(f) hot rows in CH (02_build_hot_ch.sh) and fill faithful PG heaps (03_build_hot_pg.sh).
- What I did: CH hits_hot_p01/p05/p10 = hits_100m WHERE tuple>=B(f) (counts 1M/5M/10M, verified == N). PG
  pg.hits_hot_p01/p05/p10 (LIKE pg.hits) filled via the repo's COPY ... FROM PROGRAM 'curl … FORMAT TabSeparated'.
- How verified: counts == N on both sides; checksums sum(WatchID)/sum(UserID)/max(EventTime) PG==CH (identical).
- Result / investigation: `sum(length(URL))` PG 83,057,738 vs CH 84,907,365 — alarming. Tested the typed FDW path
  → SAME PG value ⇒ not a TSV-escaping bug. Root cause: CH length()=BYTES, PG length()=CHARACTERS; ClickBench is
  Cyrillic-heavy UTF-8. CONFIRMED: PG `octet_length` == CH `length` exactly for URL/Title/SearchPhrase
  (84,907,365 / 104,152,205 / 6,044,977). Strings are byte-identical. NO fidelity bug.
- Interpretation: the CH→PG TabSeparated path is faithful (repo claim holds); my checksum must use octet_length.
- Learnings: always byte-compare strings across CH/PG. Cleaned up the FDW test table.
- Verdict: CONTINUE → build the merge/oracle harness.

### L0036 — Wire schema + view design + template capture  [unit 0]  [iteration 1]  2026-06-28T00:35+05:30
- Goal: get the full 105-col wire schema for the hot streamed_table; design pure-CH (hits_dt64 view) + merge sources;
  capture per-query deparsed CH bodies.
- What I did: captured the full wire schema from a zero-row offload of pg.hits_hot_p01 (names lowercased; the 3
  TIMESTAMP cols → DateTime64(6,'UTC'); EventDate→Date; HitColor→String). Confirmed only EventTime/ClientEventTime/
  LocalEventTime differ from hits_100m native types (DateTime) and need a cold-arm cast; the other 101 cols match.
  Confirmed even with parallel_workers=0 the deparse yields exactly ONE streamed_table call, projecting only needed
  columns. Wrote 04_capture_templates.sh to dump each query's deparsed CH body to templates/q<q>.ch.sql + eligibility.
- How verified: OPERATIONAL — system.columns diff; captured deparse strings; q5 deparse = single streamed_table.
- Result: templates capture launched (see capture.log / eligibility.tsv). Design: pure-CH source = view hits_dt64
  (lowercase + DateTime64 casts over hits_100m); merge source = inline (SELECT projcols FROM
  streamed_table('pgch_hot_p<f>','<FULL schema>') UNION ALL SELECT projcols FROM hits_100m WHERE tuple<B(f)).
- Interpretation: substitution of the single streamed_table source subquery yields fair (column-minimal) pure-CH and
  merge CH SQL from the same deparser-faithful body.
- Verdict: CONTINUE → build the oracle generator + runner; run the partition/result/pushdown oracle.

### L0037 — Full correctness oracle (42 eligible × 3 fractions)  [unit 0]  [iteration 2]  2026-06-28T01:30+05:30
- Goal / hypothesis (prereg §UNIT0): merged == pure-CH-100M within fidelity bounds, partition exact, push-down proven.
- What I did: ran 05_oracle.sh over all 42 eligible queries × {p01,p05,p10}. Fixed 3 harness bugs first:
  (a) mk_merge_sql projected_cols regex `Date32?` never matched bare `Date` (EventDate) → 9 queries broke; replaced
  with paren-aware top-level comma split. (b) pushdown read raced the async query_log flush → poll-retry for
  QueryFinish + accept ShmAdoptedBlocks+ShmCopiedBlocks. (c) a failing merge left the producer hung to its 240s
  statement_timeout → detect 'Exception' in merge.out and pg_cancel_backend the producer (clean; verified no leak).
- How verified (≥3 sources): (i) partition exactness query (countIf) — EXACT for all 3 f; (ii) cmp_results.py
  pure vs merge — class per cell; (iii) CH query_log ProfileEvents['ShmAdoptedBlocks'] poll-retry; (iv) producer
  returned-row count == N(f); (v) teardown leak check.
- Result: 112/126 PASS (class=exact); 14 FAIL (class=DIFF) — ALL on LIMIT/top-N queries {q18,q25,q31,q32,q39,q40},
  INCONSISTENT across fractions (q31 fails p01/p05 not p10; q18 only p10; q32 p05/p10) — the signature of tie
  reshuffle, not a systematic bug. Partition EXACT (p01 hot=1,000,000 cold=98,997,497; p05 5M/94,997,497; p10
  10M/89,997,497; each hot+cold=99,997,497). Push-down: ShmAdoptedBlocks=49/239/478 (p01/p05/p10) every cell.
  Producer rows == N(f) every cell. Leak check: 0 shm / 0 sockets / 0 backends. Raw: results/oracle/summary.tsv.
- Interpretation: aggregation + partition are correct (112 exact incl. heavy GROUP BY / count-distinct / regex /
  avg→Float64 q4=approx). The 14 DIFFs require root-cause (L0038).
- Verdict: CONTINUE → root-cause the 14 top-N DIFFs.

### L0038 — Top-N DIFF root-cause: benign tie reshuffle (collapses to exact under total order)  [unit 0]  [iteration 2]  2026-06-28T01:50+05:30
- Goal / hypothesis: the 14 DIFFs are tie-boundary reshuffle (which tied rows land at the LIMIT cutoff differs by
  partition), NOT a merge data error (§7 lists top-N tiebreak reshuffle as accepted; repo D0006 / tiebreak_check.sh).
  Prediction: with a deterministic TOTAL order appended to ORDER BY, pure-CH == merge EXACTLY (a real value bug
  would survive as DIFF/approx).
- What I did: 07_verify_topn.sh — for q18/q25/q31/q32/q39/q40, captured the deparser-faithful CH body for the
  TIEBROKEN PG query (ORDER BY ... + total-order tiebreak), then compared pure-CH-100M vs merge for ALL 3 fractions.
- How verified: cmp_results.py(pure, merge) for 6 queries × 3 fractions = 18 cells, each via a fresh producer +
  pushdown path (same oracle plumbing).
- Result: 18/18 EXACT (results/topn/summary.tsv). Confirms the DIFFs were purely LIMIT-boundary tie selection;
  the merge result MULTISET equals pure-CH for every top-N query under a defined total order.
- Interpretation: prediction CONFIRMED. The 14 DIFFs are the accepted top-N tiebreak deviation (§7), not value
  errors. Unit-0 correctness floor is satisfied: every eligible query × fraction == pure-CH-100M within the
  documented fidelity bounds (F3 avg→Float64 surfaces as cmp class 'approx' on q4; top-N tiebreak on the 6 queries).
- Verdict: DONE (Unit-0 correctness GREEN, pending adversarial review). → D-HC-0407.
