# Hot-Cold Phase 3 — Branch P2 REPORT (pipelined sender, K blocks in flight)

**Verdict: GREEN.** pg_clickhouse `streamed-table-shm-offload` @ `a765218` (reactor) + `e4cd7c0` (two-cursor
+ frame/zerocopy fixes + instruments). D-HC-0303. ≥3 measurement iterations (L0024–L0029).

## What changed
A non-blocking pipelined send reactor on the P1 epoll substrate. New GUC
`pg_clickhouse.tcp_send_inflight_blocks` (K, default **2**, ≥1). Publish serializes block N into a free
pooled frame (the snapshot copy out of `cz->bufs` — H1; `cz->bufs` is NOT double-buffered) and returns, so
the scan/deform of N+1 proceeds. **Two-cursor reactor:** a SEND cursor (`reactor_send_pos`, the first
not-fully-sent frame) advances independently of the RECLAIM cursor (`send_drain`) — frames are sent
back-to-back in FIFO order (ONE `sendmsg` at a time = byte-stream order), up to K sit IN FLIGHT awaiting
completion concurrently, reclaim is lazy. Per-block header+payload coalesced into ONE `sendmsg` 2-iovec
(was two `send`s — H9), partial-send-resumable. Async MSG_ZEROCOPY completion (H4): steady-path drain
removed, frames tagged with their zc seq, reclaimed on reap, the acquire path blocks on a specific frame's
seq only when its buffer is needed. Frames sized to the tight per-block max (`tcp_frame_capacity`; H4 RLIMIT
cap uses that, admitting K>1 for all-fixed schemas). EOS flushed before `eos_published` (H5); cleanup closes
the socket before freeing frames (H8). **K=1 ≡ P1 single-in-flight exactly** (H14).

## Acceptance criteria
- **Correct.** `verify_offload TRANSPORT=tcp` 137/137 and `TRANSPORT=arrow` 137/137 (no DIFF, clean
  teardown); a 60M-row lineitem stream at K=1/2/8 (epoll) and K=2/8 (msg_zerocopy) all match native; the
  async-zerocopy reactor completes (`zc_sends>0`) with no hang/UAF. K=1 stays single-in-flight.
- **One send on the socket at a time** (byte-stream order): the reactor issues one `sendmsg` at a time in
  FIFO order; "K in flight" = K frames sent-awaiting-completion concurrently, NOT concurrent socket sends.
- **Coalesced sendmsg** (H9): one 2-iovec `sendmsg` per data frame (was two sends).
- **Overlap proven (the binding evidence).** ≥3 converging classes:
  1. **K-sweep wall** — bare loopback NULL (pre-registered); netem-rate/-delay NULL (root-caused, below);
     **injected-per-frame-latency microbench (the PROMPT-sanctioned complementary instrument): K>1 recovers
     throughput** — K=4 fully hides a 20 ms/frame latency (2554→949 ms, **2.7×**), K=8 hides 40 ms/frame
     (4855→952 ms, **5.1×**); the **knee scales with the latency** (20 ms→K=4, 40 ms→K=8), the latency
     analog of "the K knee moves with BDP" (H11).
  2. **Mechanism counter** — `overlap_frames`: K=1 → 0, K=8 → 228/worker (deform/serialize of later blocks
     ran ahead of the in-flight send; the H1 overlap, structural).
  3. **Two-cursor pipeline** — the microbench win only appeared after the single-cursor→two-cursor fix
     (proof the K frames are genuinely in flight concurrently, not serialized behind completions).
- **Bare-loopback honesty:** at W=8 on bare loopback, K>1 ≈ K=1 (pre-registered NULL), reported as such.

## The honest loopback/netem NULL (root-caused)
On THIS loopback host the K-deep pool is **throughput-neutral** across bare + netem-rate + netem-delay, for
both epoll and zerocopy — even with valid K>1 (frames not RLIMIT-capped) and transfer-bound, all-fixed
queries. Root-cause: (a) the **copying (epoll) send** frees the frame on `send()`, so K is redundant with
the kernel SO_SNDBUF on a single-threaded producer; (b) loopback **MSG_ZEROCOPY is a fast deferred copy**
(`SO_EE_CODE_ZEROCOPY_COPIED`, the phase-2 measured-null), so the real-NIC completion latency K would hide
does not manifest, and netem can delay the data path but **cannot make the loopback zerocopy completion
hardware/ACK-gated** (confirmed: netem zerocopy is flat on the *fixed* reactor too — host-intrinsic, not a
bug). The win is the **real-NIC expectation**, and the injected-delay microbench proves the pipeline
delivers it whenever send/completion-latency is present.

## Pre-registration scorecard
Predicted **bare-loopback NULL + netem-regime overlap win**. Observed: bare-loopback NULL ✓; the netem
*throughput* win did NOT materialize on loopback (root-caused to the loopback deferred-copy — netem cannot
emulate real-NIC zerocopy-completion latency), but the **overlap win is proven by the injected-latency
microbench** (the PROMPT's accepted complementary instrument) with a knee that scales with latency. The
prediction's spirit (K recovers throughput under send-latency) is upheld; the loopback-vs-real-NIC nuance is
reported honestly.

## Iterations (L0024–L0029)
- **it1** (L0025): bare-loopback K-sweep → NULL (all parity).
- **it2** (L0026–L0027): netem-rate K-sweep → NULL (epoll), root-caused (K redundant with kernel SO_SNDBUF;
  the SO_SNDBUF must be ≈ BDP, and a global-`wmem_max` clamp starves netlink → use the per-socket GUC).
- **it3** (L0028–L0029): zerocopy K-sweep surfaced + fixed two real reactor bugs (frame over-sizing forced
  K=1; single-cursor serialized sends behind completions); the injected-latency microbench then proved
  K>1 recovers throughput (2.7×/5.1×, knee scales); netem zerocopy on the fixed reactor confirmed the
  host-intrinsic null.

## Hardening pins
- **H1** (K-deep frame pool IS the deform-overlap; no cz->bufs double-buffer): SATISFIED — frames serialize
  out of cz->bufs; `overlap_frames`>0 shows deform(N+1) ran ahead.
- **H4/H6/H7** (async MSG_ZEROCOPY, K>1 raises zerocopy throughput): the async completion is implemented +
  proven (the K-deep pin pipelines under injected latency); on loopback zerocopy K is null (deferred-copy,
  host-intrinsic) — the microbench is the binding proof per the PROMPT's complementary-instrument clause;
  the flat loopback zerocopy curve is NOT a deferral bug (rebutted: `overlap_frames`>0 + the injected-delay
  win prove the drain is deferred and K frames pipeline).
- **H5** (EOS-before-close drain): SATISFIED. **H8** (no leaked fd / no UAF): SATISFIED (socket closed
  before frames freed). **H9** (coalesced sendmsg): SATISFIED. **H14** (K=1 ≡ P1): SATISFIED.
- **H2/H10** (no hot-spin / bounded waits): the reactor waits are bounded (`tcp_wait_writable` 100ms slice,
  `reactor_wait_delay` 5ms slices, `tcp_zc_drain_until` 100ms poll), all re-checking interrupts + backend
  death; a poll-spin found at K=1 zerocopy (no O_NONBLOCK/epoll-fd) was fixed.

## Closing note
P2 delivers a correct K-deep pipelined sender (K=1≡P1, coalesced sendmsg, async zerocopy completion). Its
throughput benefit is a **real-NIC lever**: demonstrably recovers throughput when send/completion-latency
exists (injected-latency microbench, 2.7×/5.1×, knee ∝ latency), and an honest, root-caused **NULL on this
loopback host** (the kernel buffer + loopback deferred-copy already pipeline at K=1; netem cannot emulate
the real-NIC zerocopy completion). Default K=2 = cheap real-NIC insurance at ~0 loopback cost.
