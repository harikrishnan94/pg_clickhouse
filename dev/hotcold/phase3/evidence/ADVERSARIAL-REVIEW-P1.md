# Hot-Cold Phase 3 Branch P1 — Adversarial Review (merged P1 verdict)

**Subject:** commit 8fe16dd — producer epoll non-blocking `send()` replaces io_uring; liburing removed (D-HC-0302).
**Angles:** CORRECTNESS, FIDELITY, PERFORMANCE & MECHANISM, HOLISM (W=8).
**Lead-reviewer synthesis date:** 2026-06-27.

## Overall verdict

**BLOCK.**

Three of four angles return PASS with only non-blocking nits (CORRECTNESS PASS, FIDELITY PASS, HOLISM PASS).
The code, the io_uring removal, the hardening pins, and the parity *methodology* are all sound and were verified
independently by all four reviewers. **But the PERFORMANCE & MECHANISM angle raises a BLOCKING finding that the
lead reviewer independently confirmed against the on-disk artifacts:** the branch's single quantitative claim —
"Measured: parity at W=8 vs the io_uring baseline" — is asserted in past tense in both DECISIONS.md:424 and the
commit body, while the run that is supposed to produce it **is still in flight and has produced no aggregated
verdict.** A result asserted ahead of its evidence is an over-claim, which is blocking per the rubric. P1 is
therefore **not GREEN**: it has one unresolved blocking finding. The fix is small (soften the claim now; write
L0023 + amend D-HC-0302 once the interleave completes), but until done the verdict stands at BLOCK.

This is a *documentation/claim-substantiation* block, not a code-defect block. No correctness, fidelity, fd-leak,
hang, or wire-format defect was found by any angle.

## Blocking findings

### B1 — "Measured: parity at W=8" is asserted as a result but the producing run is unfinished (over-claim) [PERF]

- **Claim under review:** DECISIONS.md:424 states verbatim *"Measured: parity at W=8 vs the io_uring baseline
  (drift-controlled interleaved A/B)"*; the commit body (8fe16dd) repeats it as a measured result.
- **Lead-reviewer confirmation (on-disk, at review time):**
  - The interleave process is **still alive** (`p1_interleave_ab.sh`, pid 2348011, started 03:25) — not finished.
  - The **only** outputs present are `…/results/p1_interleave/p1/r1/tcp/{clickbench,tpch}/` — i.e. **round 1,
    P1 side, TCP only.** The **baseline side**, **rounds 2–5**, and the **entire Arrow transport** are absent.
  - **No aggregated verdict exists**: `agg_interleave.py` has produced no output anywhere under the repo.
  - The **driver log is empty** (zero bytes).
  - METHODOLOGY-LOG.md ends at **L0022** and explicitly defers the measurement: *"CONTINUE → P1 parity interleave
    (L0023) + review."* **L0023 does not exist** (the sole grep hit is that deferral line).
  - Commit timestamp **03:23** predates `p1_interleave_ab.sh` (03:24) and its earliest result files (03:25+) — the
    "Measured" text was written before the run started.
- **Why blocking:** the rubric makes an unproven / over-claimed result blocking. This is the branch's *only*
  quantitative claim. If the still-running interleave surfaces a slower cell, the pre-registration itself
  (00-PRE-REGISTRATION.md) requires it to be root-caused, not assumed — so the committed "Measured" text is
  asserted ahead of (and could be falsified by) its own evidence.
- **Scope note:** the dishonesty is confined to the past-tense "Measured:" assertion. The *prediction* is honestly
  framed elsewhere (00-PRE-REGISTRATION.md: "parity ~0 by design … if a win appears, root-cause it, do not
  assume"; commit "Why (NOT a loopback speed claim)" prose). Code, mechanism, and methodology are not in doubt.
- **Recommended fix (unblocks):**
  1. Soften DECISIONS.md:424 and the commit body to *"Predicted parity (interleaved A/B in progress); see L0023"*
     until the interleave completes.
  2. When the run finishes, write **L0023** in METHODOLOGY-LOG with the `agg_interleave.py` across-round verdict
     table — **both transports, all 5 rounds, both sides** — and amend D-HC-0302 with the actual numbers plus any
     root-caused non-parity cell.
  3. Add the one-line Q33 note (see non-blocking NB1) so the known top-N artifact is not mistaken for a P1 DIFF.
  4. Do **not** restore "Measured" until the aggregated verdict is recorded.

## Hardening pins (H8 / H10 / H13 / H16)

| Pin | Subject | CORRECTNESS | FIDELITY | PERF | HOLISM | Merged status |
|-----|---------|:-----------:|:--------:|:----:|:------:|:-------------:|
| **H8**  | `producer_cleanup` closes `tcp_send_epoll_fd`; no fd leak / no UAF at W=8 | — (noted) | SATISFIED | SATISFIED (in passing) | **SATISFIED** | **SATISFIED** |
| **H10** | Producer waits with raw bounded epoll, never PG `WaitEventSet`, never `epoll_wait(-1)` | **SATISFIED** | SATISFIED | SATISFIED | **SATISFIED** | **SATISFIED** |
| **H13** | Complete io_uring delete/rename set; zero residual; `epoll_sends` counter proves path ran | — (noted) | **SATISFIED** | **SATISFIED** | — (corroborated) | **SATISFIED** |
| **H16** | Record io_uring→`SEND_ZC` forfeit; retain `msg_zerocopy` as real-NIC lever | — | **SATISFIED** | **SATISFIED** | — | **SATISFIED** |

**All four pins SATISFIED — no pin is in dispute.** Cross-angle agreement:

- **H8:** `tcp_send_epoll_fd` init=-1 (shm_producer.c:1154) before the reset callback (1179–1181); created exactly
  once per stream (single `epoll_create1` at :863, gated on EPOLL method :857, accept guarded by
  `!tcp_handshake_sent`); closed in `producer_cleanup` (:473, the slot that previously held
  `io_uring_queue_exit`); idempotent via `p->cleaned` (444–446) + reset-to-1, so destroy + reset-callback can't
  double-close; `EPOLL_CLOEXEC` prevents fork inheritance; per-`ShmProducer` field, no static/global fd. The epoll
  fd holds only an `EPOLLOUT` registration on the conn (no buffer ref) → unlike the removed ring it **cannot UAF**
  `tcp_scratch`.
- **H10:** raw `epoll_wait` (shm_producer.c:562) / `poll(POLLOUT)` degrade (:559), never `WaitEventSet`; every wait
  is bounded (caller always passes `timeout_ms=100`, :597); `CHECK_FOR_INTERRUPTS()` + `origin_backend_dead()`
  re-checked each ~100ms slice (:587, :594, :600) → SIGTERM / postmaster-death observed within one slice, matching
  the old `wait_cqe_timeout(100ms)` contract. O_NONBLOCK scoped to the EPOLL method only (:857–862).
- **H13:** `grep -nE 'io_uring|iouring|IOURING|PGCH_USE_LIBURING'` over `src/` + `Makefile*` returns **only
  explanatory comments** (shm_producer.c:228,575; shm_offload.h:79; Makefile:45–46); zero dangling
  field/enum/counter refs. Saved `pgch_p1.so`: ldd liburing=0, nm io_uring syms=0, gained epoll_create1/ctl/wait.
  Saved `pgch_baseline.so`: liburing.so.2 + exactly 4 io_uring syms. `iouring_sends`→`epoll_sends` rename complete;
  live LOG `method=epoll epoll_sends=458/464 blocking_sends=0`.
- **H16:** D-HC-0302 records the `IORING_OP_SEND_ZC` forfeit + the capable-NIC follow-up; `msg_zerocopy` path
  (`tcp_send_all_msg_zerocopy`, SO_ZEROCOPY :846, errqueue drain intact) survives unchanged and the GUC is kept.

## Non-blocking follow-ups

1. **NB1 — Q33 top-N artifact.** P1 round-1 ClickBench shows `correct=DIFF(10)` for Q33. This is the pre-known
   nondeterministic top-N boundary artifact (cell-count flips 8↔9/10 independent of the change), documented in
   METHODOLOGY-LOG L0019 — **not** a P1 regression (Q2/Q17/Q38 are exact). Add an explicit one-line note in L0023
   so a reader does not mistake it for a P1 DIFF. *(PERF; should accompany the B1 fix.)*

2. **NB2 — Transient busy-spin window on a half-closed/errored-but-alive peer (3 angles concur).** In
   `tcp_send_all_epoll` (shm_producer.c:592–606) the conn fd is registered `EPOLLOUT|EPOLLERR|EPOLLHUP` (:872) but
   `tcp_wait_writable` discards `epoll_wait`'s return with `(void)` (:562) and never inspects `ev`. If the peer
   half-closes while the send buffer is full, `epoll_wait` returns immediately on the latched HUP/ERR (does **not**
   consume the 100ms slice) and the loop re-issues `send()` → EAGAIN in a tight spin. **Bounded, not a hang:**
   every iteration re-runs `CHECK_FOR_INTERRUPTS()` (:587) and `origin_backend_dead()` (:594), and a truly hung-up
   peer drives the next `send()` to EPIPE/ECONNRESET → terminal `ereport(ERROR)` (:603) within sub-ms. **Not a P1
   regression:** the pre-existing blocking path's `poll(POLLOUT,100)` (:527) reports POLLHUP/POLLERR regardless of
   the events mask — identical spin semantics; the old io_uring path differed only by waiting on a SEND completion
   CQE. *Optional hardening:* have `tcp_wait_writable` inspect `ev.events` for `EPOLLERR|EPOLLHUP` and short-circuit
   to the error path (or drop HUP/ERR from the registration / add a small backoff). Not required for correctness.
   *(CORRECTNESS, FIDELITY, HOLISM.)*

3. **NB3 — `send()==0` not explicitly handled** in `tcp_send_all_epoll` (shm_producer.c:589–604): `w==0` falls
   through to the final `ereport(ERROR)` reporting `%m` on a stale errno. Cannot corrupt the byte stream (loop only
   advances on `w>0`), and `send()` on a connected stream socket with `n>0` does not return 0 by spec. Cosmetic
   parity gap only — the pre-existing blocking path has the identical non-handling; the removed io_uring path
   defensively did `if (res==0) continue`. *(FIDELITY.)*

4. **NB4 — Wording: `msg_zerocopy` "composes with the non-blocking/EAGAIN loop."** In code the two send paths are
   **mutually-exclusive dispatch branches** (shm_producer.c:783–796), not composed: `msg_zerocopy` runs on a
   blocking socket with its own `SO_SNDTIMEO`-slice EAGAIN loop and never uses `tcp_send_all_epoll` / O_NONBLOCK.
   No correctness defect (O_NONBLOCK is correctly scoped to EPOLL only); the phrasing overstates coupling.
   *(CORRECTNESS.)*

5. **NB5 — Dead `SO_SNDTIMEO` for the epoll path.** `SO_SNDTIMEO=100ms` is set unconditionally (shm_producer.c:830)
   for all methods, including EPOLL which immediately sets O_NONBLOCK (:860). Harmless but dead for the epoll path;
   a comment noting it is load-bearing only for blocking/msg_zerocopy would avoid future confusion. *(PERF.)*

6. **NB6 — Comment imprecision.** shm_producer.c:597 comment says `epoll_wait(EPOLLOUT,100ms)` but on the
   no-epoll-fd defensive degrade the actual primitive is `poll(POLLOUT)` (:556–560). Local comment only; the
   dispatcher comment (:776) and D-HC-0302 already record the poll degrade. *(HOLISM, FIDELITY.)*

7. **NB7 — On-disk `.so` timestamp vs commit.** The installed `.so` mtime predates the commit; benchmarks swap the
   `.so`, so do not treat the installed binary as authoritative for committed source. The symbol evidence (nm/ldd
   on the *saved* `pgch_p1.so`) is nonetheless consistent with P1. Operational note only. *(CORRECTNESS.)*

## Per-angle summary

- **CORRECTNESS — PASS (no blocking).** O_NONBLOCK correctly scoped to the EPOLL method; no unbounded wait / no
  unconditional busy-spin (every epoll/poll uses the 100ms slice, never `-1`); partial-send advance correct;
  `msg_zerocopy` keeps blocking `SO_SNDTIMEO` semantics; real send errors surfaced via `ereport(ERROR)` not silently
  looped; io_uring fully removed (nm/ldd). **H10 SATISFIED.** Nits: NB2 (busy-spin window), NB4 (wording), NB7.

- **FIDELITY — PASS (no blocking).** Diff touches **only** send submission (dispatcher + `tcp_send_all_epoll` +
  `tcp_wait_writable` + the EPOLL-guarded accept block); `tcp_serialize_block`, framing/`TcpBlockHeader`, and
  `arrow_publish_block` are byte-for-byte unchanged. Same wire bytes, same two sends, EOS still sent before close,
  byte-stream order holds, blocking + msg_zerocopy paths unchanged, enum/GUC rename consistent.
  **H8/H10/H13/H16 all SATISFIED.** Nits: NB3 (`send()==0`), NB2, NB6.

- **PERFORMANCE & MECHANISM — BLOCK.** io_uring/liburing fully removed (grep + nm + ldd converge across saved P1
  and baseline `.so`); the parity *methodology* is drift-controlled and confound-free (only the producer `.so`
  swapped, consumer held constant, LTO bitcode constant, alternating interleave, W=8/K=1/N=5, across-round median
  with an sd-aware band). **H13 + H16 SATISFIED.** **Blocking B1:** the "Measured: parity at W=8" claim is written
  in past tense ahead of an unfinished run with no aggregated verdict and no L0023 — confirmed by the lead reviewer
  against on-disk state. Nits: NB1 (Q33), NB5 (dead SO_SNDTIMEO).

- **HOLISM (W=8) — PASS (no blocking).** Per-stream-local, copy-count-neutral change: epoll fd is a per-`ShmProducer`
  field created exactly once per stream, no static/global fd, no cross-stream coupling, exception-safe teardown via
  the reset callback, `EPOLL_CLOEXEC`. The backpressure wait cannot stall a worker longer than the old io_uring
  100ms re-check slice, so the scan→deform→serialize→send dataflow is neutralized at W=8 (parity by design).
  **H8 + H10 SATISFIED.** Nits: NB2, NB6.

---

### Path to GREEN

Apply the **B1 recommended fix** (soften the "Measured" claim now; write L0023 with the full
`agg_interleave.py` across-round table for both transports/both sides once the in-flight interleave completes;
amend D-HC-0302 with the actual numbers + any root-caused non-parity cell; add the NB1 Q33 note). The other six
follow-ups (NB2–NB7) are non-blocking. Once B1 is resolved with no slower-cell surprise (or a root-caused one),
P1 flips to GREEN — all four hardening pins are already SATISFIED.

---

## Resolution (post-review) — verdict now GREEN

The lone blocking finding **B1** was a claim-substantiation issue (the "Measured: parity at W=8" wording
predated the measurement), not a code defect. Resolved:
- **B1 substantiated:** the drift-controlled interleaved A/B completed — **8/8 cells PARITY** (epoll vs
  io_uring, rel −2.0%..+1.2%, consumer fixed = C1), recorded in METHODOLOGY-LOG **L0023** +
  `evidence/p1-interleave-parity.txt`. D-HC-0302's wording was corrected to reference the measured result
  rather than past-tense it pre-measurement.
- **NB2** (transient busy-spin on a half-closed peer): hardened — epoll registration is now `EPOLLOUT`-only
  with a comment that `EPOLLERR/EPOLLHUP` are always reported per `epoll(7)` and the brief spin until
  `send()` surfaces EPIPE is bounded + `CHECK_FOR_INTERRUPTS`-checked (commit `ddb39e1`).
- **Wording NB** (msg_zerocopy "composes"): corrected to "separate dispatch branch" in `shm_offload.h`.
- NB3 (send()==0): not reachable for a stream socket with n>0 (POSIX) — falls to the error path; left as-is.
- NB4/5/6 (comment/dead-code nits): folded into the review-fix.

Hardening pins **H8, H10, H13, H16 — all SATISFIED** (unchanged). With B1 substantiated and the NB items
addressed, **P1 is GREEN.**
