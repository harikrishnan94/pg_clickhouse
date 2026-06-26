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
