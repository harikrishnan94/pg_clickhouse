# Hot-Cold Phase 3 — REPORT (epoll/kqueue transport + pipelined non-blocking sender)

Three branches, executed C1 → P1 → P2, each proven with ≥3 converging instrument classes + an independent
adversarial review. **All three GREEN.** Per-branch detail in REPORT-C1/-P1/-P2.md; methodology in
METHODOLOGY-LOG.md (L0018–L0029); decisions D-HC-0301/0302/0303; reproduction in 10-REPRODUCTION.md.

## SHAs
- ClickHouse `streamed_table`: C1 `c27379bcd` + review-fix `97fc5b8` (consumer epoll-fd). Parent/baseline
  `c286c6cc`.
- pg_clickhouse `streamed-table-shm-offload`: P1 `8fe16dd` + review-fix `ddb39e1` (D-HC-0302); P2 `a765218`
  + `e4cd7c0` + docs `8bf44d9` + review-fix `fa7e455` (NB-4 pool cap) (D-HC-0303). Parent/baseline `29a1e501`.

## Branch C1 — consumer epoll-fd readiness (GREEN)
`TcpStreamSource::schedule()` returns a source-owned epoll fd aggregating {`sock_fd`, a one-shot stall
`timerfd`}; the per-async `std::thread` wake-bridge + both eventfds are deleted; cancel stays
`::shutdown(SHUT_RDWR)`. **Resource win (the claim):** zero streaming-path threads — a slow async drain
parks 575× and spawns 0 threads (was ~1 bridge thread per park); deterministic gtest. fd delta net-0
(−2 eventfds, +epoll fd +timerfd). **Parity:** drift-controlled interleaved A/B (W=8, TCP+TPC-H) = 8/8
PARITY. Stall fires at 401ms (the timerfd is the SOLE wakeup in the single-threaded `wait(-1)` path); cancel
wakes a parked source at 300ms; no hot-spin (H2). verify_offload tcp+arrow 137/137. Review GREEN.

## Branch P1 — producer epoll non-blocking send, io_uring + liburing removed (GREEN)
Replaced the io_uring `IORING_OP_SEND` (submit-then-wait, one in flight) with a non-blocking `send()` +
`epoll(EPOLLOUT)` loop; removed the ring + `tcp_iouring_*` + the liburing build dep. `msg_zerocopy` +
`blocking` survive; enum default → `epoll`. **Mechanism (the claim):** the `.so` links no liburing and
exports zero io_uring symbols (vs baseline `liburing.so.2` + 4 syms); H13 grep zero-residual; the epoll path
runs (`epoll_sends>0, blocking_sends=0`). **Parity:** drift-controlled interleaved A/B (W=8) = 8/8 PARITY
(io_uring was used synchronously, so epoll is a wash; the win is simplicity / robustness — no
seccomp-disabled fallback / portability). `msg_zerocopy` intact (errqueue accounting). Review GREEN
(the lone blocking finding was a premature "Measured" wording, substantiated by the interleave).

## Branch P2 — pipelined sender, K blocks in flight (GREEN)
A non-blocking K-deep pipelined send reactor (GUC `pg_clickhouse.tcp_send_inflight_blocks`, default 2).
Two-cursor reactor (send cursor advances independently of the reclaim cursor → up to K frames in flight
concurrently, one sendmsg at a time = byte order); coalesced 2-iovec sendmsg (H9); async MSG_ZEROCOPY
completion (H4); tight per-block frame sizing; K=1 ≡ P1 (H14); EOS-drain (H5); leak/UAF-safe cleanup (H8).
**Throughput: an honest, root-caused NULL on this loopback host** (bare + netem-rate + netem-delay, epoll +
zerocopy) — the kernel SO_SNDBUF already pipelines the copying send, and loopback MSG_ZEROCOPY is a fast
deferred copy so the real-NIC completion latency K hides does not manifest (netem cannot emulate it). **The
overlap win IS proven** via the PROMPT-sanctioned injected-per-frame-latency microbench: K=4 fully hides a
20 ms/frame latency (2.7×), K=8 hides 40 ms (5.1×), and the **knee scales with the latency** (20 ms→K=4,
40 ms→K=8) and is **confirmed directly on the zerocopy path** (`METHOD=msg_zerocopy`, 20 ms/frame: K=2
1.85×, K=4 2.52× — NB-1). Mechanism: `overlap_frames` K=1→0, K=8→228/worker. Finding the win required fixing
two real reactor bugs the correctness tests missed (frame over-sizing forced zerocopy K=1; a single-cursor
reactor serialized sends behind completions). verify_offload tcp+arrow 137/137; K=1/2/8 × {epoll,zerocopy} =
native; partial-send RESUME verified end-to-end (SO_SNDBUF=4096 → ~16+ sendmsg/frame, both methods ==
native). **Review GREEN** (4 independent angles, zero blocking; the apparent flat-zerocopy-curve FAIL
resolved by the PROMPT's complementary-instrument clause + the now-direct injected-latency zerocopy proof;
10 non-blocking follow-ups, NB-1/2/3/4/7/8 resolved, NB-5/6/9/10 tracked for hardening).

## Closing note — what Phase 3 removed / changed (the honest scope)
- **C1 + P1 are resource/mechanism wins, parity on throughput** (as pre-registered): C1 eliminates a
  per-async thread create/join (≈ one per block under steady streaming) and the readiness eventfds; P1
  removes the io_uring ring lifetime + the liburing dependency (and its seccomp-fragile silent fallback).
  Neither moves the loopback wall — both are drift-controlled PARITY at W=8 (TCP + Arrow). Teardown:
  C1's dtor no longer joins a bridge thread; P1's cleanup no longer tears down a ring.
- **P2 is a real-NIC lever, a loopback null.** The pipeline is correctly implemented and demonstrably
  recovers throughput when send/completion-latency exists (the microbench), but on this loopback host the
  kernel buffer + the loopback deferred-copy already pipeline at K=1 — an honest, pre-registered, root-caused
  NULL. Default K=2 is cheap real-NIC insurance at ~0 measured loopback cost. The transport's copy budget
  is unchanged (1 serialize + 1 kernel send copy); P2 changed only the send scheduling.
- **The phase changed the I/O readiness + send-scheduling mechanism only** — not the wire format, the type
  mapping, or the copy count. No new `DIFF` in any mode (TCP/Arrow/SHM-adopt/SHM-copy); the few non-`exact`
  cmp verdicts are pre-existing query/float nondeterminism (e.g. ClickBench Q22 top-N ties — shown to vary
  on the baseline binary), not transport bugs.
