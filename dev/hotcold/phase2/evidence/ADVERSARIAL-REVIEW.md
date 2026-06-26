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

---

## Branch B (copy reduction: B-it1/it2/it3/it4) — 2026-06-26 — VERDICT: PASS (zero blocking findings)

Independent reviewer (fresh context, did NOT write the code) attacked all four angles, re-derived every
headline number from the raw cells, re-ran the gates, and **wrote a standalone test against the real
producer (nanoarrow) encoder** to attack the lean buffer-walk — specifically hunting the "is the null
spurious?" and "buffer-reuse corruption?" traps.

**Gates re-run (independently):**
- `unit_tests_dbms --gtest_filter='ArrowStreamSource.*:TcpStreamSource.*:AdoptedConvert.*'` → **15/15 PASS**
  (exercises BOTH lean and it2 ReadRecordBatch paths + copying decode + the stock-ArrowColumnToCHColumn oracle).
- `verify_offload TRANSPORT=arrow` (default, lean OFF) → **PASS=137 FAIL=0**, no DIFF.
- `verify_offload TRANSPORT=arrow` with `shm_arrow_lean_extract 1` FORCED → **PASS=137 FAIL=0** (lean correct e2e).
- Live binary md5 == on-disk; runtime `shm_arrow_lean_extract=0` confirms the D-HC-0208 flip is compiled in.

**Confirmed sound (per angle):**
- **Correctness — the lean null is REAL, not spurious.** Proven three ways that lean genuinely runs (does NOT
  silently bail to ReadRecordBatch): (a) arrow's `ArrayLoader::LoadCommon` (reader.cc:316) always advances past
  the validity slot regardless of null_count; (b) the nanoarrow encoder emits a flatbuffer Buffer per
  `n_buffers` = 2 fixed `{NULL-validity,data}` / 3 String `{NULL-validity,offsets,chars}` (shm_arrow.c:301-321);
  (c) a STANDALONE test against the real encoder confirmed `n_buffers` 2/3, `buffers[0]==NULL`, total 11, String
  offsets `[0,1,1,4,8]` sentinel 0 — exactly lean's `advance 2/3, validity at bi` assumption. Pass-1 bail
  conditions don't fire for ClickBench; e2e result correct (646|118934). The `adoptArrowColumnToCH` refactor
  (diffed vs f30ed9d6efc) preserves exact B-it2 behavior. 2-pass exception safety: `body_buf` has exactly one
  owner at all times (13 bail sites hand it to the reader untouched; pass-2 hands it to one RetainToken; a
  pass-2 throw unwinds the token, freeing once); metadata validated by Message::Open before the GetMessage walk;
  allocFrameBuffer 64B slack keeps adoption over-reads in-allocation. **msg_zerocopy buffer-reuse: NO corruption
  window** — tcp_send_all_msg_zerocopy blocks in tcp_zc_drain_until until the kernel released the buffer BEFORE
  returning; seq mirror wrap-safe; drain can't hang (CHECK_FOR_INTERRUPTS + origin_backend_dead + 100ms poll);
  SO_ZEROCOPY scoped to the ZC method only.
- **Fidelity/honesty:** B-it3 null honest (lean cons_user 668 is actually HIGHER than it2 627 — stated, not
  spun; ~0.37% decode re-derived from the perf file). B-it4 negative honest (`tcp_zc_copied++` fires only on
  `ee_code & SO_EE_CODE_ZEROCOPY_COPIED`; the banned "absence of copy_from_user" claim is avoided). B-it1 "1
  copy" defensible (apples-to-apples vs bespoke's one scratch copy; the truly-fused 1-copy is disclosed as NOT
  implemented / out of scope). No new DIFF (every cell `correct=exact`).
- **Performance/mechanism:** all wall deltas re-derived from raw cells match the log (lean−it2 −0.5% null,
  lean−tcp +7.0%, zcsend−iouring +7.3%, adopt cons_user −299ms vs copy); kernel-recv-copy ~35% dominant
  supported; mechanism shown, nothing cherry-picked (all modes FRESH same binary/session, W=8).
- **Holism:** default it2 path + default io_uring send unaffected; CMake flatbuffers include scoped (PRIVATE,
  SYSTEM, header-only, inside the parquet target — no link/ABI change); lifetime bounded; leak oracle 137/137.

**Non-blocking findings + disposition:**
1. **(Robustness) `tcp_send_all_msg_zerocopy` first-chunk `!sent_zc` ENOBUFS branch retries without a backoff
   poll** — only hot-spins under external `RLIMIT_MEMLOCK` exhaustion below the 1 MiB chunk; currently
   unreachable (drain-before-return keeps outstanding at 0 entering a new buffer, so the first chunk never
   ENOBUFS). **DISPOSITION: FIXED in-branch** — added the same `poll(POLLERR, 100ms)` backoff the `sent_zc`
   branch uses, so the defensive path can't busy-spin. (Re-gated: producer rebuild + msg_zerocopy offload
   correct + gtests 15/15.)
2. **(Disclosure) Pre-reg mechanism #1 "fuse deform+serialize" was downgraded to "confirm 1 serialize copy"**
   — not achieved (deform→buffer→body stays 2 stages, same as bespoke), but honestly disclosed in L0016 and
   the copy-budget. **DISPOSITION: DOCUMENTED** — a truly-fused deform-into-body is a separate future opt.

**Outcome:** PASS. Finding #1 FIXED in-branch; #2 documented. Branch B is GREEN.

---

## Branch B — B-it5 (drop validateAdoptedOffsets, D-HC-0209) — 2026-06-26 — VERDICT: PASS (zero blocking)

Focused independent reviewer (fresh context) attacked the single B-it5 gating change and re-ran its gates.

**Confirmed sound:**
- **All 4 call sites gated + default OFF everywhere:** TcpStreamSource.cpp:581 (bespoke) / :887 (arrow it2) /
  :1020 (arrow lean) + PollableShmSource.cpp:675 (SHM) each wrapped in `if (validate_adopted_offsets)`;
  Settings default `false`; wired via StorageShm to BOTH ctors; PollableShmSource init-list order matches
  declaration (no `-Wreorder`); the O(1) `offsets[0]==0` sentinel (adoptStringRaw + lean + AdoptionLayer)
  is UNCHANGED — only the O(n) scan is dropped. Reversibility is real (`=1` re-runs the genuine loop).
- **Gates re-run:** unit_tests_dbms **16/16** (incl. `DrainsWithOffsetValidationEnabled`); verify_offload
  **137/137 on ALL THREE default-OFF adopt transports** — `TRANSPORT=arrow` (it2+lean), `=adopt` (SHM),
  `=tcp` (bespoke) — no DIFF, no holism regression.
- **Numbers re-derive exactly:** Q24 wall off 1234(17) vs on 1244(18) = −0.80% (within the 35 ms noise band —
  not a hidden regression, not an overclaimed win); cons_user 442.8 vs 642.5 ms = −31.1%; perf
  validateAdoptedOffsets 12.39%→ABSENT, recv-copy share 35.47%→41.32%. L0017 is NOT spun ("Honest: it does
  NOT speed up the CB Q24 wall on this host"); the wall null matches the pre-registered contingency.
- **Safety disclosure (D-HC-0209) exemplary:** the OOB-on-producer-bug tradeoff + 4 mitigations + rejected
  alternatives, not buried.

**Non-blocking note:** the wall (−0.8%) landed at the bottom of the pre-registered "partial win 0…~14%" range
(effectively the null contingency) — reported transparently, not spun.

**Outcome:** PASS. B-it5 is GREEN.
