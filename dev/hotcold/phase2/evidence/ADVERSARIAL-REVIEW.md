# Hot-Cold Phase 2 — Independent adversarial review log

Per the per-branch loop, a fresh-context reviewer that did NOT write the code attacks each step along
four angles (correctness, fidelity/honesty, performance & mechanism, holism), re-running gates and
re-deriving numbers. A branch/iteration is green only after the review passes.

---

## Branch 0 (io_uring + async transport) — 2026-06-26 — VERDICT: PASS (non-blocking findings)

Reviewer re-ran `TcpStreamSource.*` gtests (4/4, slow test 1188 ms, no hang under a 180 s hard timeout),
re-derived every A/B delta from `results/clean-*/{tpch,clickbench}/cells.tsv` (all match the report),
confirmed liburing is genuinely linked (`ldd` + undefined `io_uring_*` symbols) and the GUC propagates to
the bgworker (counter inverts io_uring↔blocking), and audited the async concurrency against the real
executor (`ExecutorTasks`/`PollingQueue`). No blocking defect that breaches a stated claim.

**Confirmed sound:**
- Correctness: 137/137 verify_offload (async default) + 4/4 gtests reproduce; `correct` column identical
  across modes. The executor-contract bug fix is real and right: single-thread `PullingPipelineExecutor`
  never calls `onAsyncJobReady` (only the multi-thread `processAsyncTasks` monitor does), level-triggered
  eventfd → the drain+reset MUST be at the top of `tryGenerate` (it is). Lost-wakeup safe (level-triggered
  poll). No double-free/leak on the recv buffer (ownership traced).
- io_uring on path: honest (deterministic counter; system-wide perf correctly noted as inconclusive).
  Copy count unchanged (IORING_OP_SEND, not _ZC) — correct.
- Deadlock re-derivation holds: `max_threads = Σ producers` is conservatively safe for both modes; a
  multi-source join cannot starve (per-source readiness fd independently epolled; node-status serializes
  onAsyncJobReady/tryGenerate per node).
- Memory/lifetime bounded (one frame buffer per source; RetainToken last-drop).

**Non-blocking findings + disposition:**
1. **(Latent race) `sock_fd` is a plain `int` touched by 3 threads** — `onCancel()` (external,
   `noexcept`, not lifecycle-fenced vs dtor) `::shutdown`s it, the bridge `poll()`s it, the dtor
   `::close`s it. Lifecycle-gated (ClickHouse destroys a processor only after the executor quiesces;
   never fired in 137 tests) but not enforced. **DISPOSITION: FIXED** — `sock_fd` made `std::atomic<int>`,
   dtor `exchange(-1)` before close, onCancel loads once; eliminates the double-close + torn-read. (Full
   shutdown-vs-reuse fencing would need a mutex in noexcept onCancel; the atomic exchange + the executor
   lifecycle close the practical hazard. Documented.)
2. **(Efficiency) The wake bridge spawns a fresh `std::thread` per partial-frame schedule cycle**
   (`startAsyncWakeBridge`). Correct (join-before-reassign) but heavier than the PollableShmSource
   precedent; it is the source of the consistent small async system-CPU overhead (`cons_sys_us` higher in
   every cell) and the Q24 +4.9% edge residual. **DISPOSITION: DOCUMENTED follow-up** — a persistent
   bridge thread (or epoll-on-socket-directly) would remove it; this directly explains the Q24 edge and
   is the clean optimization if Branch B needs to recover it. Not a correctness issue.
3. **(Honesty/doc) The Q24 "within noise band" headline clears the pre-registered floor by only 1.4 ms**
   and the two distributions are statistically separated (delta 55 ms = 2.6× baseline σ; Welch t≈6.3).
   The verdict survives only because the band is floored at 5%-of-baseline (which WAS pre-registered).
   **DISPOSITION: FIXED** — REPORT §4 + the overhead table footnote now state Q24 is a real *small*
   regression that clears a pre-committed floor by a hair (not "noise"), and the consistent async sys-CPU
   is called out. The "null confirmed" applies to 4/5 cells; Q24 is honestly a within-floor small regression.
- Pre-registered single-stream-overlap probe was NOT measured (clamp-gated). DISPOSITION: disclosed in
  REPORT §5 as a CONTINUE item with the technical reason (1-producer query → max_threads=1 → no overlap);
  mechanism is gtest-proven; no win was claimed that wasn't measured. Honest scope-narrowing.

**Outcome:** PASS. Findings #1 + #3 actioned in-branch; #2 documented as the clean follow-up that explains
the Q24 edge. Branch 0 is GREEN.

---

## Branch A (Apache Arrow IPC wire) — 2026-06-26 — VERDICT: PASS (non-blocking findings)

Independent reviewer (fresh context, did NOT write the code) re-ran BOTH gtest suites (8/8:
`ArrowStreamSource.*` 4/4 + `TcpStreamSource.*` 4/4), re-derived all 5 W=8 deltas from the raw
`results/bA-{arrow,tcp,adopt}/{tpch,clickbench}/cells.tsv` (matched the REPORT to ≤0.1%), read the
consumer decode + producer encoder line-by-line, re-read both verify logs, the pre-registration,
REPORT-branchA, the overhead table, and D-HC-0207.

**Confirmed sound (per angle):**
- **Correctness:** `copyArrowColumnToCH` String copy correct (base/total/`coffs[i]=offs[i+1]-base`,
  non-NUL-terminated; empty + all-empty cases reasoned out, no off-by-one); all 17 fixed-width TypeIndex
  cases map to the right column + memcpy width (Date→u16/2B … Decimal128→FixedSizeBinary16); body buffer
  freed AFTER `batch.reset()` (no use-after-free), in-flight bounded to one body buffer; 3-phase recv EOS
  (metadata_size==0) + producer-death (PeerClosed in any phase) correct; producer offsets-prepend correct.
  gtests 8/8 reproduce; `/tmp/bA_verify_arrow.log` = 137/137 with `arrow:` URLs + `ShmCopiedBlocks` oracle.
- **Fidelity:** every swept cell's `correct` verdict is byte-IDENTICAL across arrow/tcp/adopt (Q1
  `approx(<=1.4e-16)` is the same Decimal→Float64 bound in all modes, NOT an arrow-only deviation; rest
  `exact`). No new DIFF.
- **Performance/mechanism:** re-derived deltas (Q1 +1.35%, Q6 +0.65%, Q19 +2.39%, CB-Q2 −0.49%, CB-Q24
  +15.94%) match; fixed-width all within `max(5%,1σ)` (no fixed-width regression — blocking condition met);
  Q24 within the PRE-REGISTERED +10–30%. Mechanism confirmed: Q24 `cons_user` arrow−tcp = **+283.7 ms**
  (the String copy-decode), negligible (+4.2 ms) on fixed-width. All 3 modes ran on the SAME PG backend +
  CH server (fresh-baseline). Nothing cherry-picked.
- **Holism:** copy-decode doesn't hurt the broader W=8 dataflow (4/4 fixed-width parity); `copied=true`
  charge bumps the oracle correctly; extending TcpStreamSource didn't regress bespoke (tcp 137/137 + 4/4).

**Non-blocking findings + disposition:**
1. **(Safety/doc divergence) `readArrowSchema()` validated only field COUNT, not per-field layout** —
   D-HC-0207 requires field count **+ per-field layout**; the fixed-width memcpy trusted the SQL width
   with no check vs the Arrow buffer, so a width mismatch (e.g. Arrow Int32 vs SQL Int64) would be a heap
   over-read (not reachable in the shipped same-codebase flow, but a real defense-in-depth gap).
   **DISPOSITION: FIXED in-branch** — `readArrowSchema` now cross-validates each field: String ↔ a
   variable-binary Arrow field, and every fixed-width SQL type ↔ a fixed-width Arrow field of the SAME
   byte width (`FixedWidthType::bit_width()/8 == getSizeOfValueInMemory()`), throwing `SHM_SCHEMA_MISMATCH`
   otherwise. Rebuilt; gtests 8/8 + verify_offload arrow 137/137 re-confirmed.
2. **(Test strength) the stock-reader oracle is transitive** (both the custom decode and
   `ArrowColumnToCHColumn` are compared to shared literals, not to each other directly). Functionally
   sound. **DISPOSITION: DOCUMENTED** — the literals are the strongest oracle (known truth) and the
   end-to-end verify_offload independently cross-checks arrow vs native/other-transports; a direct
   chunk==chunk comparison is a minor future strengthening.
3. **(Branch-B mandate) the LargeBinary `offsets[0]==0` sentinel "MUST validate" (pre-reg) is not
   asserted** — the Branch-A COPY path is robust without it (it subtracts `base=offs[0]`), but the
   Branch-B ZERO-COPY adopter MUST validate `array.offset()==0 && arrow_offsets[0]==0` (it aliases
   `&arrow_offsets[1]` as the CH `offsets[-1]` sentinel). **DISPOSITION: carried into Branch B** as a
   binding requirement (it is already in D-HC-0201/0207 + 00-PRE-REGISTRATION).

**Outcome:** PASS. Finding #1 FIXED in-branch (per-field layout validation); #2 documented; #3 carried
into Branch B as a binding requirement. Branch A is GREEN.
