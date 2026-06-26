# Phase 0 — Independent Adversarial Review

Conducted by a fresh reviewer agent with a clean context that did NOT write the code
(per the spec's "launch a subagent so its context is clean"). Two passes: (1) implementation /
correctness / fidelity / oracle-soundness, run while the perf sweep was in flight; (2) performance /
convergence, run after the evidence was assembled.

---

## Pass 1 — Implementation, Correctness, Fidelity, Oracle (2026-06-25) — VERDICT: PASS

The reviewer attacked all 7 pre-specified angles using static analysis of the full diff plus
read-only `system.query_log` corroboration (8,403 historical `streamed_table` queries; 111 in copy
mode incl. 37 multi-source joins). It ran no offload query / rebuild / restart (to avoid perturbing
the in-flight sweep). Findings, condensed (file:line in the agent transcript):

1. **Slot-release-timing read-after-release race — SOUND.** Every raw SHM read (`adopt()` consuming
   the local `descs_vec` copy + `data_region` pointer; `convertToFullColumnIfAdopted` reading column
   data) completes BEFORE the RetainToken deleter fires at the end of the convert loop. All
   post-release uses go through the early-captured locals (`slot_index_local`, `row_count_local`,
   `eos_marker_local`, `this_seq`) — 100% coverage. The deleter writes into the `region_capture`-pinned
   mapping, valid even after producer reuse.
2. **State machine / double-drain — SOUND.** Precondition-24 trackers update in `findNextReadySlot`,
   independent of transport mode; copy only moves the P→E transition earlier. Single-threaded per
   source; `this_seq` is recorded before the next scan. `(prev_state+delta)%3` holds for both the
   consumer P→E (delta 1) and producer republish (delta 3). Copy is *less* racy than adopt.
3. **Counter integrity — SOUND (empirically confirmed).** `charge(copied=true)` bumps ONLY
   `ShmCopied*`, never `ShmAdopted*`, and vice-versa (mutually exclusive branches). Mode is global per
   query (one GUC → one `shm_transport_arg_literal()` → every source). Live: `mixed_both=0` across
   8,403 queries; `max(ShmAdoptedBlocks)=0` across 111 copy queries; joins carry exactly one
   `shm:copy` per `streamed_table` call.
4. **Edge cases — SOUND.** `count()` (empty projection): `full_cols.clear()` releases the slot before
   the convert block in both modes; `charge(copied=true)` already bumped `ShmCopiedBlocks` so the
   oracle fires. Subset projection: `ShmCopiedBytesLogical < charged` by design (pre-reg A1). Joins:
   all sources copy (confirmed live). EOS/stall/death read the slot table independent of mode.
5. **Fidelity (copy == adopt byte-identical) — SOUND.** `cloneResized` re-derived for all three
   adoptable column types: `ColumnVector`/`ColumnDecimal` exact `memcpy` (+ scale preserved);
   `ColumnString` copies offsets+chars and the owned PODArray allocator regenerates the `offsets[-1]`
   zero sentinel (`PODArray.h:210`) matching the validated adopted sentinel; 0-row blocks return a
   valid empty owned column. streamed_table columns are non-Nullable. No type differs under copy.
6. **Oracle soundness — SOUND, one NON-BLOCKING gap.** `verify_offload.sh` is mode-aware
   (`TRANSPORT=copy` → asserts `ShmCopiedBlocks≥1` specifically), so it proves the copy path ran.
   GAP: `wsweep_split.sh::check_eligible` uses `sum(ShmAdoptedBlocks + ShmCopiedBlocks) ≥ 1`, which
   does not by itself distinguish modes — a (hypothetical) silent GUC failure would still classify a
   cell eligible and time it as "copy". The per-mode proof therefore rests on `verify_offload.sh`,
   not the wsweep cells. **Recommendation: make the wsweep oracle assert the per-mode counter when
   `TRANSPORT=copy`.**
7. **MemoryTracker transient double-charge — SOUND, one COSMETIC note.** The adopted full-block charge
   overlaps the owned-copy allocation for ≤1 in-flight block, released within `drainSlot`. On a throw
   inside the convert loop, cleanup is correct via column-destructor RAII (un-converted `emitted_cols`
   hold retain+charge → unwind → deleter + ChargeHandle release). NON-BLOCKING: the `catch`'s
   `release_retain_if_local()` is dead after step 3, and the rollback comment understated the
   post-step-4 convert throw path.

**Overall: PASS. No blocking findings.**

### Actions on the two non-blocking findings
- **(7, cosmetic) FIXED** — `PollableShmSource::drainSlot` comment now documents that the copy/tcp
  convert loop runs after step 4, can throw, and is made exception-safe by column-destructor RAII
  (not the dead manual lambda). Commit rides into the next CH rebuild. No behavior change.
- **(6, oracle) ADDRESSED** — after the in-flight sweep finished (cannot edit a running bash script):
  (a) the wsweep `check_eligible` was made mode-aware (assert `ShmCopiedBlocks≥1` when
  `TRANSPORT=copy`); (b) a post-hoc `system.query_log` audit certified every copy-sweep cell had
  `ShmCopiedBlocks>0 AND ShmAdoptedBlocks=0` (see `evidence/copy-mode-audit.txt`). The phase-gating
  proof remains `verify_offload.sh` (137/137 copy, per-mode counter asserted).

---

## Pass 2 — Performance / Convergence (2026-06-25) — VERDICT: PASS

A fresh reviewer agent re-derived every headline number from the four raw `cells.tsv` WITHOUT the
author's `overhead-table.md`, and attacked the 8 perf/convergence angles. Verbatim-condensed:

1. **Headline re-derivation — SOUND (exact match).** Independent recompute: TPC-H median +0.500%,
   geomean +1.00%, range [−1.01%,+3.84%], within-floor 11/11; ClickBench median +0.340%, geomean
   +0.47%, range [−9.21%,+8.80%], 32/35. Comparable set honest (Q8/Q11 + 7 TIEBREAK_BENIGN excluded in
   BOTH modes; no inconvenient cell dropped, no mis-join).
2. **Baseline freshness — SOUND (strongest control).** phase0-adopt `off_med` differs from the stale
   `dev/wsweep-report` by +18–35% (ClickBench) / −1.2…−15.5% (TPC-H) → adopt was genuinely re-measured
   fresh, not reused. Host stability: native (transport-independent) drift adopt→copy mean ~1.1–1.2%;
   the 2 cells that drift >3% are the known-noisy ones. Comparison does not inherit drift.
3. **FINDING (NON-BLOCKING): the word "noise."** A sign test shows a statistically real small positive
   bias the 5% floor hides — re-derived independently by the author: **wall 30 copy>adopt vs 14
   copy<adopt (binomial two-sided p=0.0226); dCons 44 positive vs 2 negative (p=3.1e-11).** The CLAIM
   (small, byte-proportional overhead) is sound AND was pre-registered ("copy ≥ adopt on most cells"),
   so this CONFIRMS the mechanism — but "within measurement noise" is the wrong words for a real
   sub-floor effect. **ACTION: REPORT reworded** to "small, real, byte-proportional, below the 5%
   pre-registered reporting floor," with the sign-test numbers.
4. **Q2/Q24/Q23 — SOUND.** Q2 −9.2% legitimately dismissed (its NATIVE drifted −10% between runs;
   dCons≈0 — host variance, not a copy speedup). Q24 +8.8% corroborated by 3 instruments (largest
   dCons, ShmCopyTime 465ms ≈ ΔuserCPU 452ms, 8.18 GB copied).
5. **FINDING (NON-BLOCKING): C2 labeling.** Cold microbench 29.7 vs in-query 21 GB/s is a 1.4× gap,
   outside the ≈ rule. Defensible only because they are DIFFERENT quantities (isolated single-core
   memcpy vs memcpy under concurrent pipeline bandwidth pressure). **ACTION: C2 relabeled**
   "mechanism-consistent; ~1.4× slower in-query under bandwidth contention (cross-quantity, expected)"
   — not a tight numeric PASS.
6. **FINDING (NON-BLOCKING) → actually STRENGTHENS C3.** The "negative-Δ consumer CPU" cells were an
   artifact of summing user+sys; split out (author re-derived): the copy cost lands in USER time
   matching ShmCopyTime to **0.85–1.14×** (Q1 0.90×, Q6 0.63×, Q14 1.03×, Q19 0.85×, Q17cb 1.13×,
   Q19cb 1.14×, Q24 0.97×); the negative TOTALS are sys-time swings (Q14 dSys −108ms, Q17 dSys
   −118ms) between single-shot uncapped captures. **ACTION: REPORT C3 now reports the user/sys split**
   — copy cost is cleanly in user-CPU; C3 PASSES on user-CPU.
7. **FINDING (NON-BLOCKING): PMU not background-subtracted.** `perf stat -p <CHPID>` covers the whole
   CH process (merge/flush threads) over the 19.7s window with no idle baseline subtracted, so 6.31×
   is "soft" as a precise figure though directionally sound (order-of-magnitude: Δ cache-misses 1.305 G
   ≈ 51% of the 2.55 G single-pass line-touches for 8.18 GB×20). **ACTION: REPORT adds the
   no-background-subtraction caveat + the order-of-magnitude check.**
8. **FINDING (NON-BLOCKING): profile confound.** `cloneResized` (5 frames, multi-%) is nested under
   `validateAdoptedOffsets` (16–28%, runs in both modes), so the % is entangled; but qualitatively the
   adopt profile has exactly 1 incidental `cloneResized` @0.02% vs copy's 5 @multi-% — the grep did not
   miss it; copy genuinely appears only in copy. Honest in direction. **ACTION: REPORT notes the
   nesting + the 1-vs-5 line-count as the clean qualitative separator.**

**Overall: PASS. No blocking findings.** The headline survives exact independent re-derivation; the
baseline is provably fresh; the worst cell is triangulated. Single weakest point: the word "noise"
(now corrected to "below the 5% reporting floor / statistically real but small"). C2's 1.4× and the
PMU 6.31× are reported with the appropriate caveats rather than as tight numeric passes.

### Net effect on the phase verdict
All findings NON-BLOCKING; none flips "small overhead" to "large" or invalidates a gate. Actions
applied to `REPORT.md` (§4 wording + sign test, §5 C3 user/sys split, §6 PMU caveat, §8 profile
nesting, C2 relabel). **Phase 0 is GREEN.**
