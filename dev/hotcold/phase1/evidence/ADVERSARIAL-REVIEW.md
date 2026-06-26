# Phase 1 (TCP transport) — Independent Adversarial Review

Conducted by a fresh reviewer agent (clean context, did NOT write the code). It independently
re-ran the oracle + gtests and re-derived every headline number. Verdict: **PASS** (no blocking).

## Verdict by angle (condensed; full evidence in the agent transcript)
1. **Framing fidelity — SOUND.** `tcp_serialize_block` (shm_producer.c) and `adopt()` agree
   byte-for-byte: descriptors at offset 0, `cursor=align_up(n*56,8)`; each `value_offset`
   independently aligned (Decimal128 → 16B; recv buffer `aligned_alloc(64,…)` keeps base+offset
   16-aligned); String chars+SIMD-pad, then the `offsets[-1]` zero sentinel at `offsets_offset-8`,
   then offsets; `value/offsets_padding=64`; recv buffer over-allocated by +64 so over-reads stay
   in-buffer. Identical to the known-good SHM `publish_block` layout modulo frame-relative offsets.
2. **Correctness over the wire — SOUND.** Reviewer ran `verify_offload.sh TRANSPORT=tcp` →
   **137/137 PASS**: multi-source SEMI/LEFT joins (one TCP conn per relation, distinct ports),
   decimal, String, fail-closed (no leaked /dev/shm), teardown (**no leaked /dev/shm, sockets, or
   workers**). EOS = separate `payload_len=0` frame; mid-frame peer EOF → `SHM_PRODUCER_DEATH_BEFORE_EOS`;
   `onCancel` `shutdown(SHUT_RDWR)` unblocks recv; NULLs via forced `join_use_nulls=1`.
3. **Fidelity vs SHM/native — SOUND.** No new `DIFF`: every tcp cell's verdict matches adopt/copy
   (Q4 all-`approx`, tcp 6.1e-16 vs adopt 2.0e-16 — machine-epsilon float-rounding, far under
   threshold; Q22/Q41 pre-existing DIFF exclusions in all modes).
4. **Performance — SOUND.** Re-derived 3-way overhead EXACTLY: ClickBench tcp/adopt +0.6% median
   (+1.4% gm, [−11.5,+26.6]); TPC-H +7.5% (+7.6% gm, [+1.9,+13.1]); Q24 +26.6% (largest dCons
   +0.525). Parallelism explanation supported: `off_prod` median ≈3.97 (TPC-H, 4 producers) vs ≈7.4
   (ClickBench, 8) at W=8. Baseline genuinely fresh same-session/binary (cells.tsv all 2026-06-26).
5. **Convergence — SOUND.** perf-prof-tcp shows `__arch_copy_to_user ← skb_copy_datagram_iter ←
   tcp_recvmsg ← sock_recvmsg ← recvAll ← recvBlock` @6.63%; perf-prof-copy has ZERO data-path
   tcp_recvmsg/skb frames (only incidental tcp_ack/sack @0.01% from HTTP/metrics). Microbench 7.9 /
   in-query 6.4–6.9 / SHM-copy ~21 GB/s converge in mechanism + magnitude.
6. **Counter/oracle — SOUND.** Partition audit: zero mixed; TCP (597 q, 221k copied, 0 adopted)
   distinguished from copy by the `tcp:` SQL arg; oracle proves heavy-fragment TCP offload via the
   `tcp:127.0.0.1:<port>` literal + `ShmCopiedBlocks≥1` + full read_rows.
7. **Single-stream pathology — SOUND (disclosed, no deadlock).** ~26s single-stream serialization
   is honestly disclosed (REPORT §9), correct root-cause (blocking source + max_threads=1), and
   non-blocking-correct (completes). No deadlock: listener bound+listening before `WS_READY`,
   backend waits `WS_READY` before dispatch, so `connect()` always hits a listening socket; lazy
   `accept()` does not stall connect (kernel SYN queue).

## OVERALL: PASS. No blocking findings.
Reviewer reproduced 137/137, both gtests (incl. the 7.9 GB/s microbench), and every headline overhead
number; framing matches the known-good SHM layout; mechanism unfaked; teardown leak-free.

### Single weakest surviving point (NON-BLOCKING) + action
The blocking-source liveness relies on `max_threads = Σ producers ≥ #blocking TCP sources`, enforced
incidentally (the max_threads forcing exists for producer/consumer matching). **ACTION TAKEN:** pinned
this as a documented DEADLOCK-SAFETY INVARIANT with a comment at the `max_threads` forcing in
`shm_customscan.c` (`shm_build_offload_settings`). The durable fix — an async `TcpStreamSource`
(non-blocking socket + `schedule()`/`Async` + partial-frame buffering, like `PollableShmSource`) —
remains the recommended future work (REPORT §9). NON-BLOCKING for Phase 1.

**Phase 1 is GREEN.**
