# Hot-Cold Phase 3 — Branch P1 REPORT (producer epoll send; io_uring + liburing removed)

**Verdict: GREEN.** pg_clickhouse `streamed-table-shm-offload` @ `8fe16dd` + review-fix `ddb39e1`. D-HC-0302.

## What changed
The TCP/Arrow producer's socket send moves off io_uring onto a **non-blocking `send()` + epoll(EPOLLOUT)
readiness** loop (`tcp_send_all_epoll`), one send in flight. On EAGAIN it waits for writability on a
per-worker epoll fd (`tcp_send_epoll_fd`, conn registered EPOLLOUT, created in `tcp_accept_conn` only for
the epoll method, which also sets the conn O_NONBLOCK) with a ~100ms slice re-checking
`CHECK_FOR_INTERRUPTS()` + `origin_backend_dead()`. The readiness wait is behind `tcp_wait_writable` (a
kqueue backend is a drop-in; Linux epoll implemented, bounded `poll(POLLOUT)` degrade). **Removed:**
`tcp_iouring_ensure`, `tcp_send_all_iouring`, the ring fields, `PGCH_TCP_IOURING_ENTRIES`, the
`producer_cleanup` `io_uring_queue_exit`, `#include <liburing.h>`, and the Makefile liburing detection
(`-DPGCH_USE_LIBURING` + `-luring`). Enum `IOURING`→`EPOLL` (default); GUC `epoll`/`async`; `blocking` +
`msg_zerocopy` kept. The serialize/framing/EOS and the two-sends-per-block shape are unchanged (that's P2).

## Acceptance criteria — met
- **Correct.** `verify_offload TRANSPORT=tcp` 137/137 and `TRANSPORT=arrow` 137/137 (P1 producer + C1
  consumer), no DIFF, clean teardown. `msg_zerocopy` still functions: a live offload query with
  `tcp_send_method=msg_zerocopy` returned the correct result and logged `method=msg_zerocopy zc_sends=691
  zc_copied=679` — the SO_ZEROCOPY + errqueue accounting (incl. the SO_EE_CODE_ZEROCOPY_COPIED measured-null)
  is intact.
- **Mechanism proven (io_uring off).** The built `.so` links **no liburing** (`ldd`) and exports **zero
  io_uring symbols** (`nm`), vs the baseline `.so` (`liburing.so.2` + 4 io_uring symbols). `grep -nE
  'io_uring|iouring|IOURING|PGCH_USE_LIBURING' src/ Makefile*` returns only explanatory comments (H13). The
  live producer LOG shows `method=epoll epoll_sends=458/464 blocking_sends=0` — the epoll path is on the
  wire, not a silent fallback.
- **Parity (the prediction, substantiated).** Drift-controlled interleaved A/B (epoll vs io_uring, swapping
  only the producer `.so`, consumer fixed = C1, 5 rounds, N=5): **8/8 cells PARITY** (rel −2.0%..+1.2%, all
  within the band) on tcp CB{2,17,33,38} + TPC-H{6,7,9,14}. See `evidence/p1-interleave-parity.txt` /
  L0023. Arrow uses the identical `tcp_send_all` producer path → parity is wire-agnostic. Predicted ≈0 and
  observed ≈0 — io_uring was used **synchronously** (submit→wait, one in flight), so it never overlapped
  anything; epoll does the same copy + wire.
- **Robustness (claimable, not measured).** The epoll path has no seccomp-disabled fallback degradation,
  unlike io_uring (which silently fell back to blocking in hardened/container deployments).

## Evidence — ≥3 converging instrument classes
1. **Mechanism / structural:** `ldd` + `nm` (0 liburing / 0 io_uring syms vs baseline 1/4); H13 grep
   zero-residual; the `epoll_sends` counter (`>0`, `blocking_sends=0`).
2. **W=8 end-to-end parity — drift-controlled interleaved A/B** (`evidence/p1-interleave-parity.txt`,
   L0023): 8/8 PARITY.
3. **Correctness:** `verify_offload` tcp 137/137 + arrow 137/137; live `msg_zerocopy` + `epoll` LOG proofs.
4. **Independent adversarial review** (`evidence/ADVERSARIAL-REVIEW-P1.md`): GREEN after resolution; pins
   H8/H10/H13/H16 SATISFIED; the lone blocking item was a premature-claim wording issue, substantiated by
   L0023 and corrected.

## Hardening pins
- **H8** (no leaked producer fd): SATISFIED — `producer_cleanup` closes `tcp_send_epoll_fd` (replacing the
  io_uring ring teardown); initialized to −1 so cleanup is idempotent; created once per stream.
- **H10** (raw epoll, bounded slice, never `epoll_wait(-1)`): SATISFIED — every wait is `tcp_wait_writable(p,
  100)`; `CHECK_FOR_INTERRUPTS` + `origin_backend_dead` re-checked each slice; `poll(POLLOUT)` degrade is
  bounded; no `WaitEventSet`.
- **H13** (zero residual): SATISFIED — grep returns only comments; `.so` has no `-luring`/io_uring symbols.
- **H16** (SEND_ZC forfeit logged): SATISFIED — D-HC-0302 records the `IORING_OP_SEND_ZC` forfeit, retains
  `msg_zerocopy` as the real-NIC zero-copy-send lever, and names "re-introduce io_uring solely for SEND_ZC
  if it proves the dominant real-NIC lever" as the follow-up.

## Closing note
P1 removes the io_uring ring + the liburing build dependency from the producer send path, replacing it with
a non-blocking `send()` + epoll(EPOLLOUT) loop. On bare loopback it is **parity** (drift-controlled, 8/8) —
io_uring was synchronous, so epoll is a wash; the win is simplicity (no ring lifetime/teardown), robustness
(no seccomp-disabled degradation), and portability (no liburing). `msg_zerocopy` survives as the retained
real-NIC zero-copy-send lever. The per-stream resource changes from an io_uring ring to a single epoll fd
(closed in cleanup, H8). P2 builds the pipelined sender on this epoll substrate.
