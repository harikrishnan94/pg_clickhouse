# Full SHM-offload of ClickBench — Decision log

Running log of: (a) decisions taken without a human where the spec left an
ambiguity (with alternatives + rationale), (b) every **intentional fidelity
deviation** accepted to unlock coverage (bounded + quantified), and (c) every
query concluded **cannot** be offloaded, with the root-caused blocking mechanism.

This is the artifact a human reviews later. It is append-only in spirit; entries
are dated. "Offloaded" always means proven from ClickHouse `system.query_log`
(a `streamed_table()` `QueryFinish` — or `ExceptionWhileProcessing` for a
client-cancelled stream — with `ShmAdoptedBlocks >= 1`, the heavy fragment in the
dispatched SQL), never from the PostgreSQL plan alone.

---

## Environment of record

- DB `clickbench`, schema `pg`, table `hits` (10M-row subset `hits_0..9`). PG 18.4
  @ 127.0.0.1:5432. `pg_clickhouse` 0.3.
- ClickHouse patched `streamed_table` build (v26.6.1.1), RUN_ID=tpchcb. HTTP port
  was 21002 at session start; **after the Phase-1b consumer rebuild + restart the
  server moved to HTTP 21000 / TCP 21001** (the start script picks a free port —
  D0013). The live pid + ports are resolved from the listening socket, **not** the
  manifest `CH_PID` (stale); harnesses read the port from the manifest, and the FDW
  server `ch_bench` is re-pointed after each restart (`ALTER SERVER … SET port`).
- `ubuntu` OS login has no PG role; all psql runs as the `postgres` OS user.
- Offload engaged via the `shm_set_block` SET block in `dev/bench/bench-common.sh`
  (`enable_shm_offload=on`, `shm_min_rows=0`, `session_settings` incl.
  `allow_experimental_streamed_table_function 1`, `final 1`, `group_by_use_nulls 1`,
  and a unique `log_comment` tag).
- Host: 32 cores, 61 GiB. Idle/dedicated.

---

## D0001 — 2026-06-25 — Environment: stale FDW server port

**Finding.** At session start the FDW server `ch_bench` (which `local_ch_server`
points at for the offload) had `port '21000'`, but the live ClickHouse HTTP port
is `21002` (matches the manifest). Offload failed with `communication error:
Failed to connect to 127.0.0.1 port 21000`.

**Decision.** Fix in place: `ALTER SERVER ch_bench OPTIONS (SET port '21002')`
(minimal; the 10M `pg.hits` heap + `ch` FDW import are intact, only the server
port option was stale). Verified: offload now fires (`Q2 → 202134`, matches
native; `last_query_used_clickhouse = on`). Did **not** re-run `make pg` (would
re-COPY 10M rows for no benefit).

---

## D0002 — 2026-06-25 — Oracle: poll-retry the query_log (async-flush race)

**Finding.** ClickHouse enqueues the `QueryFinish` row into the *asynchronous*
`query_log` buffer slightly **after** the query pipeline completes and the PG
backend returns. A single `SYSTEM FLUSH LOGS` + read immediately after the PG
query can therefore **miss** the just-finished `streamed_table` entry. This
produced **false negatives** (`strmd=0`) on the large-grouped-result queries
Q18/Q37/Q40 in the first scan, even though re-reading seconds later found the
entry (`read_rows=10000000, ShmAdoptedBlocks=164`, tag attached).

**Decision.** The oracle must **poll-retry**: `FLUSH LOGS` + read by tag, retry up
to 12×0.5 s until the entry appears (else conclude genuine non-offload). Applied
to `dev/clickbench/eligibility-scan.sh` and `dev/clickbench/wsweep.sh`
(`check_eligible`). After the fix Q37/Q40 correctly classify **fully**. A single
necessary-not-sufficient `SHOW last_query_used_clickhouse` is explicitly **not**
trusted alone — it reports `on` even when the tag/recency query_log read races.

---

## D0003 — 2026-06-25 — "Fully offloaded" classification rule (ClickBench shape)

**Decision.** ClickBench is single-table (no joins); the **heavy fragment** is the
aggregate / `GROUP BY` / `HAVING` (+ top-N) for aggregate queries, or the filtered
scan (+ `ORDER BY`/`LIMIT`) for the non-aggregate projections (Q20, Q24–27). A
query is **fully offloaded** iff BOTH independent sources agree:
1. CH `system.query_log` (by unique `log_comment` tag) shows a `streamed_table()`
   `QueryFinish` with `ShmAdoptedBlocks >= 1`, and the dispatched CH SQL contains
   the query's heavy operator (the `GROUP BY`/aggregate, or the filtered scan), AND
2. the PG `EXPLAIN` plan has **no residual `Aggregate`/`HashAggregate`/`GroupAggregate`**
   doing the query's reduction above the topmost `Custom Scan (ClickHouseShmScan)`.

`fully(cxl)` = the heavy fragment ran in CH (`ShmAdoptedBlocks >= 1`) but the
stream was **client-cancelled** (logged `ExceptionWhileProcessing`, Code 210
broken pipe) because PG's `LIMIT`-without-`ORDER BY` closed the socket after k
rows (Q18). The heavy op still executed in CH; counted as offloaded.

`scan_only` = a `ClickHouseShmScan` fires but PG still does the aggregate.
`none` = no `ClickHouseShmScan` / no `streamed_table` query in the log.

A residual PG `Sort`/`Limit` above the CustomScan does **not** demote `fully` (the
GROUP BY+aggregate is the heavy fragment); it is tracked separately as the
**top-N pushdown** perf lever (D0007).

---

## D0004 — 2026-06-25 — count(DISTINCT) deparses to ClickHouse uniqExact (EXACT)

**Finding.** `count(DISTINCT col)` deparses verbatim to ClickHouse `count(DISTINCT
col)`. ClickHouse's `count_distinct_implementation` setting **defaults to
`uniqExact`** (exact), so the offloaded result is **bit-exact** to PostgreSQL — NOT
approximate HyperLogLog. Measured exact for **every** ClickBench count(DISTINCT)
query: Q5, Q6, Q9, Q10, Q11, Q12, Q14, Q23 all `fidelity=exact` vs native (incl.
the tiebroken top-N variants). Independent re-derivation (confirmed by the
adversarial review): Q5 `count(DISTINCT UserID)` native == offload == CH
`uniqExact` == CH `count(DISTINCT)` all `1530334` (0 abs/rel error); Q6
`count(DISTINCT SearchPhrase)` all `835093`.

**Decision.** **Accept as-is.** No deparse change, no fidelity deviation. This is
the single biggest fidelity trap called out in the spec, and it is a non-issue
here. Should a future CH config set `count_distinct_implementation=uniq`
(approximate), this would regress — noted as a risk; not changing the default.

---

## D0005 — 2026-06-25 — avg() over a full-range Int64 column overflows in ClickHouse (Q4)

**Finding.** Q4 `SELECT AVG(UserID) FROM hits`. `UserID` is BIGINT spanning the
full signed-int64 range (`min=-9.22e18, max=9.22e18`). PostgreSQL `avg(bigint)`
accumulates the sum in arbitrary-precision `numeric` → exact `2513100748938099884`.
ClickHouse `avg(Int64)` accumulates the numerator in a fixed-width integer that
**overflows** over 10M large values → `-653315757734.87` (wrong sign + magnitude;
**rel err ≈ 1.0** — a wrong answer, not bounded round-off). Independent check:
`avg(toFloat64(UserID))` on CH = `2513100748938100700` ≈ native (rel ≈ 3e-16). So
the fix is to accumulate in Float64.

**Decision.** This is a **correctness bug**, not an acceptable deviation (the spec
forbids undocumented wrong answers). Fix in Phase 2 by deparsing `avg(<wide int>)`
as `avg(toFloat64(<col>))` — which both eliminates the overflow AND aligns with
the spec's accepted **avg()→Float64** policy (bounded ≤ ~1e-15 rel). Scope: only
Q4 hits this at ClickBench scale (every other `avg()` is over `ResolutionWidth`
SMALLINT or `length(...)`, whose sums stay well within range). Until fixed, Q4 is
logged as **offloaded-but-WRONG**, not accepted.

**RESOLVED (Phase 2).** `src/deparse.c` `deparseAggref`: when `node->aggfnoid ==
F_AVG_INT8` (i.e. `avg(bigint)`), the argument is wrapped `avg(toFloat64(<arg>))`.
Scoped to int8 only — `avg(int2/int4)` sums stay far inside Int64 even at 100M
rows, so those remain **bit-exact** (verified: `avg(ResolutionWidth)` native ==
offload == `1508.8046441`, unchanged). After the fix Q4 native
`2513100748938099884` vs offload `≈2513100748938099700` (the low digits are
Float64-rounding noise; summation-order-dependent): **max abs err ≈ 184–1900, max
rel err ≈ 6.1e-16 — Float64 epsilon**, the accepted avg()→Float64 deviation.
Independent: dispatched CH SQL is `avg(toFloat64(userid))`. Q4 reclassified from
**offloaded-WRONG** to **offloaded + bounded avg→Float64** (logged in the ledger).
Perf W=8: 1.29×. The comparator labels it `approx` (the avg result prints without
a decimal point, so its column reads as integer-valued); the deviation is the
avg→Float64 rounding, not an integer-count approximation — disambiguated here.

---

## D0006 — 2026-06-25 — Top-N tie-boundary reshuffle is a benign, documented deviation

**Finding.** Q22, Q32, Q33, Q39, Q40, Q41 showed `DIFF`/`approx` at baseline. Root
cause: these are `... ORDER BY <agg> DESC LIMIT k [OFFSET m]` where PG performs the
`Sort`+`Limit` over the streamed grouped result (top-N not pushed, D0007). The
`GROUP BY` counts/aggregates are computed **exactly** in CH (identical to native);
the only difference is **which equal-ranked tied rows** land in the top-k window —
non-deterministic in *both* engines (PG's native plan reshuffles ties run-to-run
too). Re-running each with a **deterministic tiebreak** appended (`ORDER BY <agg>
DESC, <group keys>`) yields **`exact|k|k|0|0`** for all six.

**Decision.** **Accept** as a benign tie-order deviation (explicitly allowed by the
spec). The fidelity oracle compares top-N results **tie-robustly** (deterministic
tiebreak or set-equivalence), never a brittle byte diff. Recorded per-query in the
fidelity ledger (`PHASE0-COVERAGE.md`). Q18 (`LIMIT 10` with **no** `ORDER BY`) is
the degenerate case — nondeterministic *by design*; comparing exact rows is
meaningless, so it is excluded from the byte oracle and judged on offload mechanism
alone (its GROUP BY offloads, D0003 `fully(cxl)`).

---

## D0007 — 2026-06-25 — Top-N / ORDER BY / LIMIT / OFFSET is NOT pushed (perf lever)

**Finding.** For **every** aggregate top-N query the GROUP BY+aggregate offloads
(`pg_agg=0`) but the `ORDER BY`+`LIMIT`(+`OFFSET`) runs in **PostgreSQL**: the plan
is `Limit → Sort → Custom Scan (ClickHouseShmScan)`, the dispatched CH SQL has
**no** `ORDER BY`/`LIMIT` (`ch_ord=no, ch_lim=no` for all), and CH streams the
**entire** grouped result back (`read_rows=10000000` feeding a grouped relation PG
then sorts). The deparser *has* `appendOrderByClause`/`appendLimitClause`
(`src/deparse.c:4857-4936`) but they do not engage for the streamed_table upper
path.

**Decision.** Pursue top-N pushdown as a **performance** phase (P3), not a coverage
blocker (these are already `fully` per D0003). Highest value where the grouped
result is large: Q16 (`GROUP BY UserID`), Q34/Q35 (`GROUP BY URL`), Q31–33
(`GROUP BY ClientIP`/`WatchID`) — streaming millions of grouped rows to take a
top-10 is wasteful. Quantify the win in `wsweep.sh` before/after.

---

## D0008 — 2026-06-25 — Timestamp columns cannot stream (coverage blocker, Q19/24/25/27/43)

**Finding.** The SHM wire type map (`src/shm_offload.c:112-121`,
`pgch_pg_type_to_ch_wire`) has **no `TIMESTAMP`/`TIMESTAMPTZ`** case — only
bool/int{2,4,8}/float{4,8}/date/text/varchar/bpchar/numeric. Any query that must
**stream** a `timestamp` column (project it, filter/sort on it, or feed it to a
CH-side function) is declined entirely. Confirmed: Q25 `... ORDER BY EventTime
LIMIT 10` plans `Limit → Sort → Parallel Seq Scan` (no CustomScan). Affects Q19
(`extract(minute FROM EventTime)`), Q24 (`SELECT *`, 3 timestamp cols), Q25/Q27
(`ORDER BY EventTime`), Q43 (`DATE_TRUNC('minute', EventTime)`).

**Decision.** Add `timestamp` wire support in Phase 1 (highest coverage: +5
queries). The patched ClickHouse consumer **already** adopts `DateTime` (UInt32)
and `DateTime64` (ColumnDecimal) — `AdoptionLayer.cpp:338,345` — so this is a
**PG-producer-side-only** change (map `TIMESTAMPOID` → a DateTime/DateTime64 wire
tag + converter), no ABI/consumer change. Fidelity to verify: PG `timestamp`
(µs since 2000-01-01) vs CH `DateTime` (s since 1970) — ClickBench `EventTime`
values are whole seconds, so second-resolution should be exact; will measure and
pick DateTime vs DateTime64(6) accordingly.

---

## D0009 — 2026-06-25 — REGEXP_REPLACE semantics differ slightly PG vs CH (Q29)

**Finding.** Q29 `GROUP BY REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$',
'\1')` offloads fully, but the per-group `COUNT(*)` deviates by **rel ≈ 9.9e-5**
(`off=223329 on=223307` on the largest group) even with a deterministic tiebreak —
so it is **not** a tie reshuffle. Root cause (hypothesis): PostgreSQL
`regexp_replace` (POSIX/PCRE) and ClickHouse `replaceRegexpOne` (RE2) extract a
slightly different host substring for a small fraction of `Referer` values (edge
cases in the optional `(?:www\.)?` group / anchoring / non-matching rows that
`\1`-collapse differently), shifting a few rows between groups.

**Decision.** Root-cause + quantify exactly in the regex phase (P4): enumerate the
divergent `Referer` rows, bound the max per-group rel error, and decide **accept
(bounded, documented)** vs **deparse to an RE2-equivalent that matches PG**. Until
then Q29 is logged as offloaded with a measured ~1e-4 count deviation (group-set
identical; counts shift). Not hidden.

---

## D0010 — 2026-06-25 — Q1 (`SELECT COUNT(*) FROM hits`) declines — expected

**Finding/Decision.** Q1 references **no column**, so there is nothing to stream;
the planner adds no `ClickHouseShmScan` and a parallel seq-scan `count(*)` wins.
Per the spec this is **expected, not a failure**. Logged as `none (expected)`.
Optional later experiment (P5): force a 1-column stream to offload the count;
low value, deferred.

---

## D0011 — 2026-06-25 — Phase 1: timestamp → DateTime64(6, **'UTC'**) (tz fix)

**Decision.** Map PG `timestamp` (`TIMESTAMPOID`) → ClickHouse **`DateTime64(6,
'UTC')`** over the SHM wire (`src/shm_offload.c` type map + `write_fixed_value`
`SHM_WIRE_DATETIME64` converter + the C++ `shm_deform.cpp` `TimestampToCh`
kernel). PG timestamp is int64 µs since 2000-01-01; the converter rebases to int64
µs since 1970-01-01 (`+ PGCH_TS_EPOCH_DIFF_US`). The consumer already adopts
`DateTime64` (Wire/Layout.h tag 18), so this is **producer-side only** — no ABI
change. Unlocks Q19, Q25, Q27, Q43 (Q24 needs the separate col-limit raise, D0012).

**Why DateTime64(6) over DateTime(s):** DateTime64(6) is lossless (full µs, signed
Int64 tick range), avoiding the 1970–2106 UInt32 DateTime window and any sub-second
truncation. ClickBench `EventTime` is whole-second, so both would be exact here,
but DateTime64(6) is the correct general mapping.

**Why the `'UTC'` qualifier is mandatory (correctness, not cosmetic).** Caught by
the Phase-1 correctness gate: with a bare `DateTime64(6)`, Q19 `extract(minute FROM
EventTime)` was **off by +30 min** on every row (`offload_m = (native_m+30) mod
60`). The CH server tz is `Asia/Kolkata` (+5:30); a bare `DateTime64` inherits it,
and `toMinute()` (the deparse of `extract(minute …)`) is tz-dependent, whereas PG
`extract` is tz-naive. `toStartOfMinute`/ordering are tz-invariant for whole-minute
offsets, so Q25/27/43 were exact even before the fix — only Q19 exposed it.
Pinning `'UTC'` makes CH interpret the streamed wall-clock exactly as PG does.
**Alternative considered:** set `session_settings` `session_timezone=UTC`.
Rejected — fragile (relies on a session GUC for correctness) and would also shift
any genuine tz-aware behaviour; pinning the column type is local and explicit.

**Fidelity:** Q19/25/27/43 exact vs native (tie-robust for the top-N Q19/Q43).
Logged: no deviation. **Perf** (W=8): Q19 1.51×, Q43 1.64×; **Q25 0.57×
(offload slower)** — a cheap-filter projection whose `ORDER BY EventTime LIMIT 10`
runs in PG, so offload pays a round-trip with nothing to amortize. Honest loss;
motivates top-N pushdown (D0007 / Phase 3).

---

## D0012 — 2026-06-25 — Q24 (`SELECT *`) blocked by the 64-column SHM cap (deferred)

**Finding.** Timestamp support is necessary but **not sufficient** for Q24
(`SELECT * FROM hits …`): `hits` has 105 columns and the SHM stream caps at
`SHM_IMPL_MAX_COLS = 64` (`src/shm_producer.c:50`) — enforced on BOTH sides
(consumer `IMPL_MAX_COLUMNS = 64`, `Wire/Layout.h:62`). The streaming worker fails
with `shm column count 105 out of range (1..64)` and the query returns 0 rows.

**Decision.** Defer to **Phase 1b** (task #7). The schema table / per-column
descriptor regions are **dynamically sized** (`n_columns * sizeof(...)`), so 64 is
a soft validation cap, not a fixed-array bound — raising it to 128 on both sides
(+ a ClickHouse rebuild/restart, keeping the consumer tests green) should unlock
Q24. Q24 is the **only** ClickBench query needing > 64 columns. Kept separate from
the timestamp phase because it is a distinct mechanism (column-count scaling) and
requires a consumer rebuild.

**RESOLVED (Phase 1b).** Raised `SHM_IMPL_MAX_COLS 64→128` (`src/shm_producer.c`)
and the consumer `IMPL_MAX_COLUMNS 64→128` (`ClickHouse .../Wire/Layout.h:62`),
rebuilt + restarted ClickHouse. This is the **only consumer/ABI-side change in the
whole ClickBench effort** (top-N pushdown, avg→Float64, regex are all PG-side
deparse). It is ABI-compatible: the schema table and per-column descriptor regions
are sized dynamically by `schema_count` (no fixed `[64]` array on either side), so
a 64-column stream still validates under a 128 cap. Verified:
- **Consumer tests green:** `unit_tests_dbms` SharedMemory/Adoption/Wire suites —
  **49 PASSED** against the new constant.
- **Q24 offloads + exact:** `SELECT *` (105 cols incl. 3 timestamp cols) →
  oracle `QueryFinish, read_rows=10M, ShmAdoptedBlocks=499`; `cmp_results.py` =
  `exact|10|10|0|0` vs native (tie-robust ORDER BY EventTime, WatchID). PG plan:
  `Limit → Sort → ClickHouseShmScan` (filter pushed; top-N in PG — per the
  non-agg-projection definition this is `fully`).
- **No regression:** `sanity.sh` green (PASS=17), zero `/dev/shm/pgch_*` leaks,
  zero stray stream workers.

Coverage after Phase 1b: **42/43 offload** the heavy fragment; only Q1 (no-column
decline, D0010) is intentionally native.

---

## D0013 — 2026-06-25 — Operational: CH manifest `CH_PID` is stale; restart by port

**Finding (cost me a debugging loop).** `dev/bench/ch-bench-server.sh stop` keys off
the manifest `CH_PID`, which is **stale** (the spec warned this). After a `stop;
start` the old server kept running (its watchdog respawns the server child), the
new server grabbed a different port, and the FDW pointed at the wrong one — so Q24
still hit the *old in-memory binary* (a running process keeps its loaded code even
after the on-disk binary is rebuilt) and reported the old `[1, 64]` limit.

**Decision / procedure for any CH restart (e.g. to load a rebuilt binary):**
1. Resolve the LIVE pid from the listening port (`ss -ltnp | grep :PORT`), not the
   manifest; kill the **watchdog + server** together (SIGTERM, graceful — lets SHM
   clean up) so the watchdog can't respawn the old child.
2. Confirm nothing is listening on the bench ports, then `start` fresh.
3. The start may pick a **new port** — re-point the FDW:
   `ALTER SERVER ch_bench OPTIONS (SET port '<new>')` (D0001 recurs on every
   restart). Harnesses read the port from the manifest, so they self-adjust; only
   the FDW server option must be re-set.
This is also why `wsweep.sh`/`sweep-capped.sh` resolve the CH pid from the port.

---

## D0014 — 2026-06-25 — Known latent issue: SUM(bigint) overflows in offload (out of ClickBench scope)

**Finding (raised by the Phase-1/2 adversarial review).** `SUM(<int8 col>)` over a
full-range bigint overflows the ClickHouse Int64 sum accumulator, same class as the
old Q4 avg bug: native `SUM(UserID)` = `25131007489380998843148972` (numeric, exact)
vs offload `-6533157577348666708` (wrong sign). The Phase-2 fix is deliberately
**avg-only** (F_AVG_INT8), so `sum(int8)` is unaffected.

**Decision: log, do not fix in-scope.** **No ClickBench query sums a wide int8** —
the only `SUM`s are over `int2`/`int4` (AdvEngineID, IsRefresh, ResolutionWidth+n),
whose sums stay far inside Int64 (10M×65535=6.6e11; even 100M×2.1e9=2.1e17 ≪ 9.2e18),
verified bit-exact. So this does not affect any of the 43 queries or the deliverable.
If a future workload sums a full-range int8 column, the fix mirrors Phase 2: deparse
`sum(int8)` → `sum(toInt128(col))` (Int128 accumulator: 10M×9.2e18 = 9.2e25 ≪ Int128
max 1.7e38 → exact, and PG `sum(bigint)`→numeric matches). Not implemented now to
avoid out-of-scope risk; flagged so it is not forgotten.

---

## D0015 — 2026-06-25 — Phase 3: top-N (ORDER BY/LIMIT/OFFSET) pushdown ATTEMPTED, REVERTED

**What.** Tried to push ORDER BY + LIMIT + OFFSET into the dispatched ClickHouse
SQL for the SHM offload (so CH returns only the top-k grouped rows instead of
streaming the whole grouped result for PG to Sort+Limit). Added UPPERREL_ORDERED /
UPPERREL_FINAL CustomPath creation in `shm_customscan.c` (mirroring the FDW's
`add_foreign_ordered_paths`/`add_foreign_final_paths`), with the ordered/final
fpinfo a shallow copy of the grouped fpinfo, and passed pathkeys/has_final_sort/
has_limit to the deparser.

**Result: the deparse was CORRECT but the execution was WRONG — reverted.** The
dispatched CH SQL was verified right (e.g. Q43 `... GROUP BY (toStartOfMinute(
eventtime)) ORDER BY count(*) DESC NULLS FIRST, ... LIMIT 10 OFFSET 1000`, and the
PG plan correctly collapsed to `Custom Scan` with no residual Sort, no double-apply
Limit). **But the ordered/final CustomScan returned 0 rows** (even a trivial
`GROUP BY AdvEngineID ORDER BY c DESC LIMIT 5` returned empty, while native
returned 5; CH logged a Code-210 broken pipe — PG closed the read early). Results
were also **intermittent** (10 rows once, 0 rows on repeat), a signature of state
corruption from the shallow `memcpy` of `CHFdwRelationInfo` (it shares List
pointers — grouped_tlist/remote_conds — with the grouped rel's fpinfo, which the
planner/executor can mutate). The bug is in the CustomScan result read-back / plan
tuple-descriptor setup for the new upper rels, not the deparse.

**Decision: revert** (`git checkout` of `src/shm_customscan.c`; deparse.c untouched).
Per the spec, a perf optimization must never produce a wrong/empty answer; coverage
is already 42/43 without it. Top-N therefore **stays in PostgreSQL** above the
offloaded GROUP BY — which is **correct** (CH computes the exact grouped result,
PG Sorts+Limits it). The only cost is perf: for high-cardinality GROUP BYs (Q16
UserID, Q34/35 URL, Q31-33) and cheap-filter projections (Q25), CH streams the full
grouped/filtered relation back instead of k rows (Q25 measured 0.57× — offload
slower). All top-N queries remain **fully offloaded** by the coverage oracle (the
GROUP BY/aggregate heavy fragment runs in CH; the residual PG Sort/Limit does not
demote `fully`, D0003).

**To finish later (not in scope now):** the correct fix is (a) a deep-copy (or
fresh) fpinfo for the ordered/final rel rather than a shared shallow copy, and
(b) correct CustomScan `custom_scan_tlist` / output-tlist wiring for the
ordered/final upper rel so the executor reads the bounded CH result. Worth
revisiting as a dedicated perf effort with executor-level gating.
