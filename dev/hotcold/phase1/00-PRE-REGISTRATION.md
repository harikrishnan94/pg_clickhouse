# Phase 1 — TCP transport — PRE-REGISTRATION

Written BEFORE measuring. Amendments append-only at the bottom.
Author: Hot-Cold transport-substrate pass. Date: 2026-06-25.

## Restated scope + acceptance criteria
- Add a TCP transport alongside SHM (both adopt + copy still selectable), selected per query via a
  `streamed_table('name','schema','tcp:<host>:<port>')` arg. One producer per CH stream, each listening
  on its own TCP connection the consumer connects to. Build on the Phase-0 consumer copy path.
- TCP correct: passes the oracles; byte-stream framing matches the wire ABI semantics so adopted
  columns reconstruct identically. Clean teardown (no leaked workers, sockets, fds) on success, error,
  cancellation — reuse the leak-teardown assertions.
- Re-run the harness across ClickBench + TPC-H at W=8 over TCP. Quantify overhead vs BOTH SHM modes
  (zero-copy adopt and copy).

## Design (mechanism), per D-HC-0101..0104
- Producer worker binds `127.0.0.1:0`, reports the ephemeral port to the PG backend (worker header);
  the union SQL emits `tcp:127.0.0.1:<port_w>` per worker (D-HC-0102).
- On accept: HANDSHAKE frame (schema_count + `SchemaEntry[]`); then BLOCK frames `{payload_len,
  row_count, eos, descriptors_offset=0}` + the frame-relative data-region bytes (descriptors + column
  buffers, identical align/pad/sentinel layout); EOS frame closes the stream (D-HC-0103).
- Consumer `TcpStreamSource` connects, recvs the handshake (cross-validates schema), then per block
  recvs `payload_len` bytes into a 64B-aligned owned buffer and runs the unchanged `adopt()` over it;
  the buffer is freed by the RetainToken deleter on last drop (D-HC-0101/0104). Counters: `ShmCopied*`
  (TCP is a copy transport; `ShmCopiedBlocks` is the oracle signal).

## Hypothesis — MECHANISM (what the instruments must show)
1. **Correctness identical to SHM.** TCP result == copy == adopt == native within the SAME bounded
   deviations. No new `DIFF`. ShmCopiedBlocks≥1 (TCP path increments the copy family), ShmAdoptedBlocks==0.
2. **One extra copy + syscalls vs SHM-copy.** Relative to SHM-copy (which does SHM→owned memcpy), TCP
   does kernel send + kernel recv (two kernel copies on loopback) + per-block `send`/`recv` syscalls.
   Relative to SHM-adopt (zero copy), TCP adds the full transfer. PMU/profile must show socket
   `send`/`recvmsg` (or `__libc_recv`/`tcp_sendmsg`/`copy_user`) on the wire path and the kernel
   network stack (`tcp_*`, `skb`), with the producer's PUBLISH replaced by socket send and the
   consumer's adopt-over-buffer replacing slot adoption.
3. **Loopback ~ memory-bandwidth bound + syscall overhead.** Loopback TCP throughput is bounded by
   memory bandwidth (the kernel copies the bytes) plus per-`send`/`recv` syscall cost; expect it to be
   SLOWER than SHM (which moves data via shared pages, no kernel copy) but same order of magnitude.

## Hypothesis — PREDICTED MAGNITUDE (falsifiable)
Anchors: SHM-copy in-query rate ≈ 21 GB/s (Phase 0). Loopback TCP throughput on Linux is typically
several GB/s to low tens of GB/s; loopback does ~2 kernel copies (send-side and recv-side) plus
syscalls. So:
- **TCP per-byte cost > SHM-copy per-byte cost** (TCP adds a second kernel copy + syscalls over the
  single userspace memcpy of SHM-copy). Predicted TCP effective transfer rate **~3–10 GB/s** (loopback,
  with per-block frames of up to a few MB amortising syscall cost). ns/byte **~0.1–0.35**.
- **End-to-end wall (W=8) vs SHM-adopt:** TCP adds the full transfer cost (producer serialise+send +
  consumer recv) to the critical path where it is not hidden by other work. Predicted:
  - Compute-bound aggregates/joins (TPC-H Q1/Q9, high-card GROUP BY): transfer overlaps CH compute ⇒
    **single-digit to low-double-digit % overhead** vs adopt; modest vs copy.
  - Wide / high-byte cells (ClickBench Q24 8.2 GB, TPC-H Q19): transfer dominates ⇒ **the largest
    regression, tens of %**; worst case the producer becomes send-bound.
  - Thin producer-scan-bound cells (TPC-H Q6): the bytes are small ⇒ overhead modest.
- **vs SHM-copy:** TCP ≥ copy (the extra kernel copy + syscalls); the gap is the loopback kernel-copy +
  syscall surcharge, predicted a further few–15% on byte-heavy cells, within noise on compute-bound.
- **Net ordering predicted:** adopt ≤ copy ≤ tcp in wall time (each adds transfer work), all bounded;
  TCP the largest but still completing every offloaded query (no deadlock/stall) and preserving the
  offload wins on compute-heavy cells.

A result that contradicts this (TCP faster than SHM, or TCP failing/deadlocking, or a new DIFF, or
TCP wildly slower e.g. >3× on compute-bound) is a finding to INVESTIGATE.

## Correctness (gate BEFORE timing; never measure on red)
- TCP result == adopt == copy == native within documented bounds; no new DIFF (a framing/recv bug that
  corrupts bytes blocks the phase). Oracle proves the heavy fragment offloaded over TCP: new
  `streamed_table('…','tcp:…')` QueryFinish, `ShmCopiedBlocks≥1`, heavy operator dispatched, no residual
  heavy operator in the PG plan.
- Regression: `verify_offload.sh TRANSPORT=tcp` green (incl. leak teardown — no leaked workers, sockets,
  fds on success/error/cancel); the multi-source join cases prove each relation streams over its own
  TCP connection.
- Edge cases to verify explicitly: EOS, producer death before/after EOS (connection close), query
  cancellation mid-stream (consumer closes → producer send fails → worker exits cleanly), empty result,
  count() (no columns), subset projection, all adoptable types incl. Decimal/String/Date.

## Instruments (≥3 independent classes)
1. End-to-end timing — wsweep at W=8, TCP vs adopt vs copy (the 3-way overhead table), median+spread.
2. Producer phase split — READ/DEFORM unchanged; PUBLISH→socket-send; PUBLISH_STALL = blocked-send time.
3. Consumer cost — CH query_log consumer CPU (User+Sys) + ShmCopied* + (if added) ShmTcpRecvTime/Bytes.
4. PMU — perf stat consumer + producer: TCP adds syscall/kernel-network cycles + cache traffic; compare
   to SHM-copy (extra kernel copy) and SHM-adopt (none).
5. Profile — send/recv + tcp_* kernel frames appear; adopt-over-buffer present; no unexpected hotspot.
6. Microbench — a loopback send/recv throughput timer (per-block, per-byte ns) to fix the TCP per-byte
   transfer constant with no scan/consumer noise.

## Convergence gates
- T1: TCP result byte-set == copy result (== adopt within bounds) per query.
- T2: loopback microbench ns/byte ≈ in-query TCP transfer ns/byte (recv-time / bytes).
- T3: TCP consumer CPU − copy consumer CPU ≈ the extra recv/kernel-copy cost; producer send cost shows
  in the phase split.
- T4: teardown — zero leaked /dev/shm (n/a for TCP), sockets, fds, workers after success+error+cancel.

## Noise threshold: same as Phase 0 — "≈" := rel diff ≤ max(5%, 1 stdev).

## Amendments (append-only)
- (none yet)
