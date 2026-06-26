# Hot-Cold Phase 2 — PRE-REGISTRATION (Apache Arrow wire + copy reduction)

Written **before** measuring (evidence-standard rule 3). Records, per branch, the expected **mechanism**
and the **predicted magnitude**, so a prediction/result mismatch is a finding to investigate, not to
rationalize. Amended per branch at iteration start (append-only amendments, dated). Supersedes any
shorthand in `evidence/FINDINGS-format-and-sequencing.md` where they differ (PROMPT.md governs).

## Environment of record (verified 2026-06-26)
- AWS Graviton **aarch64**, 32c / 61 GiB, dedicated/idle (`uptime` load < 0.5 gate before each sweep).
- Kernel `7.0.0-1006-aws`. `lo` has no ring (`ethtool -g lo` → Operation not supported); ENA `ens34`
  reports `TCP data split: n/a` → **no NIC header/data split** (binding: kernel zero-copy recv infeasible).
- PG18 `:5432` (pid 2024103). CH **reldeb v26.6.1.1** `:21002` (live pid from `ss`, currently 2205376;
  manifest pid is stale). Shared cgroup-v2 `cpu.max` cap = `W*100000/100000`. **W=8** is the headline regime.
- Baseline SHAs at pre-registration: pg_clickhouse `3ac38402` (branch `streamed-table-shm-offload`),
  ClickHouse `2847326da921` (branch `streamed_table`). liburing-dev **2.14** (`/usr/include/liburing.h`),
  liburing.so.2 runtime. Arrow bundled in CH contrib = **23.0.1**. nanoarrow = to be vendored (Branch A).
- Sockets: `SO_SNDBUF`/`SO_RCVBUF` = 32 MiB; `net.core.{w,r}mem_max` = 64 MiB (verified set). Applied
  identically to every TCP cell (bespoke + Arrow).
- **Fresh baseline gate (this session, pre-change):** `verify_offload.sh TRANSPORT=tcp` = **137/137 PASS**
  on the green HEAD binaries. This is the correctness floor every branch must keep.

## Baseline numbers to beat (phase 1 REPORT — to be RE-MEASURED FRESH on each new binary)
- in-query loopback recv ≈ 6.4–6.9 GB/s; isolated gtest microbench 7.90 GB/s (0.127 ns/byte).
- W=8 bespoke-TCP vs SHM-adopt: ClickBench median **+0.6%**, TPC-H **+7.5%**, worst Q24 (`SELECT *`) **+26.6%**.
- Mechanism (decisive): TCP cost = kernel recv copy `tcp_recvmsg→skb_copy_datagram_iter→__arch_copy_to_user`
  (+4.1% cache-misses), **context-switches FLAT** → cost is the COPY (bandwidth), not syscalls.

## Evidence classes (≥3 independent, must converge in direction + magnitude)
1. **End-to-end timing** — `dev/wsweep-report/wsweep_split.sh` style: median of N≥5 warm runs + min/max/stdev,
   shared cgroup cap, W=8. Mirror phase0/1 `run_sweeps.sh`. ClickBench (`hits` 10M) + TPC-H (`lineitem` 59,986,052).
2. **Producer phase split** — in-code stopwatch (`shm_log_stream_stats` "shm phase": READ/DEFORM/PUBLISH/STALL)
   + a new **SERIALIZE** phase (Arrow) + send accounting. Cross-checked vs `/proc` off_prod + CH `query_log` consumer CPU.
3. **PMU** — `perf stat`/`record` on producer workers + CH consumer threads: cycles, IPC, LLC/cache-misses,
   branch-misses, context-switches/syscalls. ARM PMU `cycles` / `task-clock`. Used for **mechanism**.
4. **Profiles** — perf/flamegraph attributing time to functions/kernel paths: send/recv paths, Arrow
   serialize/deserialize frames, `io_uring_enter`, **absence** of `cloneResized`/per-column memcpy on the
   consumer pass-through, no new hotspot.
5. **Microbenchmarks / code timers** — gtests under `…/SharedMemorySource/tests/`: Arrow ser/deser ns/byte,
   zero-copy-send loopback throughput, allocation-count, `convertToFullColumnIfAdopted` materialize-on-mutate
   (Squashing/HashJoin/Nullable recurse), Arrow round-trip vs stock `ArrowColumnToCHColumn` oracle.
6. **Consumer `query_log` ProfileEvents** — `Shm*` counters, new per-mode counters.

Noise band := relative diff ≤ `max(5%, 1 stdev)`. If effect ≤ noise, say exactly that.

---

# Branch 0 — io_uring + async transport (bespoke wire UNCHANGED)

## Scope restatement
Move producer send (`tcp_send_all`) and consumer recv onto io_uring; make `TcpStreamSource` **async**
(overlap recv with downstream). Bespoke `TcpFrame.h` wire unchanged. Land as small commits, gated on
correctness + teardown. Provides the I/O substrate for Branch B zero-copy send.

## Mechanism (what changes in the dataflow)
- **Producer (PG bgworker):** `tcp_send_all`'s blocking `send(MSG_NOSIGNAL)` + `poll(POLLOUT)` → a
  per-worker io_uring ring (`io_uring_queue_init`), one `io_uring_prep_send` per buffer, submit + wait
  with a bounded timeout (`io_uring_submit_and_wait_timeout` or wait_cqe_timeout) so the loop still polls
  `CHECK_FOR_INTERRUPTS` + `origin_backend_dead`. The userspace→kernel copy (`copy_from_user`) is
  UNCHANGED (this is `IORING_OP_SEND`, not `_ZC`); io_uring only changes *how* the send is submitted.
- **Consumer (CH):** convert `TcpStreamSource` from blocking `ISource` to **async**, mirroring
  `PollableShmSource` (eventfd + async-wake bridge + `prepare()→schedule()→onAsyncJobReady()` contract).
  `schedule()` returns a readiness fd; while waiting, the source returns `Status::Async` and the executor
  epolls the fd, freeing the worker thread to process the previous chunk on the pipeline. io_uring recv
  variant: a completion thread (CH io_uring is NOT epoll-integrated — `IOUringReader` uses a dedicated
  monitor thread; zero `IORING_OP_RECV` in-tree today) that signals the readiness eventfd. The kernel recv
  copy (`copy_to_user`) is UNCHANGED. Recv buffer = the **to-be-adopted** buffer (so Branch A's Arrow
  adopter drops in without buffer rework).

## Holistic end-to-end note (process rule 3)
Whole path: PG deform → serialize(scratch, 1 copy) → **io_uring send** (kernel copy_from_user) → wire →
**io_uring/async recv** (kernel copy_to_user into to-be-adopted buf) → adopt → CH pipeline → drop. Branch 0
touches only the *submission model* of send/recv and the *scheduling* of the consumer source — **no copy is
added or removed**. The only behavioral change to the whole system is: the consumer source no longer pins a
thread inside `recv`, so a single stream can overlap recv with downstream processing when ≥2 pipeline
threads exist. At W=8 multi-stream the transfer is already overlapped across producers, so the system-level
delta is ≈0; the single-stream regime is where the relaxed thread-pinning helps.

## Deadlock-safety invariant (re-derived for async)
Blocking-era rule: `max_threads = Σ producers ≥ #blocking TCP sources` (a blocking source pins a thread for
its stream's lifetime, so threads must ≥ sources or a source starves). **Async re-derivation:** an async
source does **not** pin a thread inside recv — it returns `Status::Async` and the executor epolls its fd, so
one thread can service many ready sources. The invariant **relaxes** to: *every async source's readiness fd
is registered and the source is re-scheduled on readiness/cancel/stall* (no starved-of-wakeup source). New
liveness risks to guard + test: (a) a source that returns Async but never re-arms its fd (hang); (b) a
partial frame straddling schedule cycles never completing; (c) cancel/producer-death must still write the
readiness fd to unblock the executor epoll. Pin with a code comment + a liveness gtest incl. the
single-stream `max_threads=1`→`max_threads≥2` overlap case.

## Predicted magnitude (PRE-REGISTERED)
- **Multi-stream W=8 throughput delta vs bespoke-TCP: ≈ 0** (within noise band). This is the *expected,
  honest* outcome — Phase-1 proved the cost is the kernel copy, not syscalls; context-switches were flat, so
  io_uring's syscall-batching targets a non-bottleneck. A measured null result here is a PASS, not a failure.
- **Single-stream overlap win:** the genuine, separately-measured win. Phase-1 single-stream (1 TCP stream,
  `max_threads=1`) measured ~26 s on a 60M-row scan vs ~0.9 s with W producers. With async + `max_threads≥2`,
  predict the single-stream scan drops materially (target: recv overlapped with the aggregate, so wall ≈
  max(recv_time, process_time) not their sum). Predicted ≥1.5× on a single-stream scan-heavy query. Measured
  explicitly + separately (not in the default W=8 harness regime).
- **io_uring on the path:** profile shows `io_uring_enter` (producer) instead of blocking `sendto`; a
  syscall/context-switch counter delta recorded (predicted small/≈0 at W=8 per above).
- Floor: **no regression vs bespoke TCP at W=8** within the noise band.

---

# Branch A — Apache Arrow serialization (copying decode OK)

## Scope restatement
TCP carries an **Apache Arrow IPC record-batch** (encapsulated message: flatbuffer metadata + body) instead
of the bespoke `TcpFrame.h` payload. Producer emits Arrow via **nanoarrow** (vendored); consumer reads Arrow
and produces the same Chunks. Schema (names + types incl. Decimal scale / DateTime64 precision) carried in
the handshake/Arrow schema. Branch A correctness MAY use the **copying** decode path; zero-copy adoption is
Branch B. SHM transports unchanged + selectable.

## Per-type Arrow mapping (PRE-REGISTERED — the contract)
| CH type | Arrow type | layout | adoptable (Branch B)? |
|---|---|---|---|
| UInt*/Int*/Float* | matching Arrow primitive | contiguous LE == `PODArray` | yes (in place) |
| Decimal32/64/128 | Arrow `Decimal128`(16-byte 2's-complement LE) / raw int32/int64 | contiguous LE | yes (Decimal128 needs 16-byte align) |
| Date (UInt16) | **raw `uint16`** | contiguous LE | yes (non-semantic Arrow schema — D-HC) |
| DateTime (UInt32) | **raw `uint32`** | contiguous LE | yes (non-semantic — D-HC) |
| DateTime64(p) | **raw `int64`** | contiguous LE | yes (non-semantic; opt: Timestamp for p∈{0,3,6,9}) |
| String | **`LargeBinary`** (int64 offsets) | `values`==CH `chars`; `offsets`(N+1) | yes (`offsets`=`&arrow_offsets[1]`, leading 0 = `offsets[-1]` sentinel; MUST validate) |
| Nullable(T) | validity bitmap + nested | 1-bit bitmap | nested adoptable; null_map = transform copy |

- **LargeBinary, not Binary** (int32 offsets would force a widening copy). **LargeBinary, not LargeUtf8**
  (CH String is arbitrary bytes).
- IPC alignment **≥16 (use 64)** so Decimal128/SIMD buffers land aligned; buffer `(offset,length)` metadata
  consistent with the CH-padded body. (D-HC: nanoarrow IPC writer.)
- Adopter MUST validate `array.offset()==0 && arrow_offsets[0]==0` (Arrow only *recommends* the leading 0;
  sliced arrays violate it) → else fall back to copy/error.

## Mechanism
Producer: deform → nanoarrow `ArrowArray`/`ArrowSchema` → IPC encapsulated message (metadata + body) → send.
Consumer: recv metadata, parse schema/buffers; **Branch A** = decode buffers into owned columns (copying
path), build Chunk. Stock `ArrowColumnToCHColumn` is the independent decode **oracle** (decode the same
bytes via the general copying path, compare cell-for-cell).

## Holistic note
Branch A adds an Arrow serialize on the producer (replacing the bespoke frame layout — similar cost: still
one userspace copy into the send buffer) and, on the consumer, a **parse/decode pass** for the copying path.
For fixed-width that decode is a bulk `memcpy` (≈ parity with bespoke adopt-in-place's zero-copy, but bespoke
adopts so it's slightly cheaper). For String the copying decode rebuilds `chars`+`offsets` = an extra pass
the bespoke adopt avoids → a regression on String-heavy cells, recovered in Branch B.

## Predicted magnitude (PRE-REGISTERED)
- **Correctness:** byte-identical `cmp.py` verdicts vs SHM-adopt/copy/bespoke-TCP/native within the same
  documented bounds; **no new DIFF**. Cross-checked vs stock `ArrowColumnToCHColumn`.
- **Fixed-width / numeric cells:** **parity** with bespoke TCP at W=8 (within noise). A fixed-width
  regression **blocks** Branch A.
- **String-heavy cells (copying decode):** a **bounded, pre-registered regression** vs bespoke (which
  adopts). Predicted **+10–30%** on ClickBench wide `SELECT *` (Q24) and String-projection cells, driven by
  the consumer parse pass (extra ~1 GB-scale memcpy + offset rebuild). Branch A goes GREEN with this bounded
  regression measured + logged; **Branch B MUST recover it to parity** via zero-copy adoption. A String
  regression exceeding the bound, or ANY fixed-width regression, blocks Branch A. (Bound finalized to a
  number at Branch A iteration start, before measuring.)

---

# Branch B — copy reduction (zero-copy adoption + send-side null)

## Scope restatement
On the Arrow wire + io_uring/async substrate, minimize copies end to end and prove it. **True kernel
zero-copy is the real-NIC north star, NOT a loopback criterion** (this host cannot do it). Loopback success:
producer 1 userspace copy; send-zc = honest measured null; recv = single-copy; consumer = zero-copy adoption
on the pass-through path; prompt drop.

## Copy-budget PREDICTIONS (per type) — to be filled with MEASURED evidence in REPORT
| stage | bytes | predicted copy | mechanism | instrument |
|---|---|---|---|---|
| PG heap → deform → temp buf + Arrow serialize | logical | **1 (required)** | fused deform+serialize | per-block copy counter + profile |
| userspace → kernel (send) | logical | loopback **1 deferred** / real-NIC **0** | MSG_ZEROCOPY/SEND_ZC | loopback: `SO_EE_CODE_ZEROCOPY_COPIED` flag (null result); real-NIC: no copy_from_user |
| kernel → userspace (recv) | logical | loopback **1 (single-copy)** / real-NIC **0** | recv into to-be-adopted buf | single-copy proven (no 2nd userspace memcpy); kernel copy measured residual |
| recv buf → ColumnPtr (fixed-width + String LargeBinary) | 0 | **0 (eliminated)** | Arrow buffers adopted in place; offsets=`&arrow_offsets[1]` | no cloneResized; alloc counter |
| recv buf → ColumnPtr (Nullable null_map) | row_count B | **1 small (required)** | validity bitmap → byte null_map | profile + null_map alloc counter |
| ColumnPtr lifetime | — | — | RetainToken drop on chunk consume | MemoryTracker peak / leak oracle |

## Mechanisms (PRE-REGISTERED)
1. **Producer 1 userspace copy** — fuse deform+Arrow-serialize so the byte is written once into the send
   buffer. Prove via per-block copy counter + profile.
2. **Send zero-copy = measured NULL on loopback** — implement `IORING_OP_SEND_ZC` (or `MSG_ZEROCOPY`),
   handle the completion/notification, and prove via the **`SO_EE_CODE_ZEROCOPY_COPIED`** errqueue flag that
   the loopback path **defers a copy** (pessimization, not elimination). Real elimination is real-NIC only.
   Do NOT claim elimination from "absence of copy_from_user" alone.
3. **Recv single-copy** — recv the Arrow body into the to-be-adopted buffer; no userspace recopy. Two recvs
   (small metadata, then body) — the metadata read is parsed, not a data copy (not a single-copy violation).
   Kernel recv copy is the measured residual (no NIC header/data split here). Design the capable-NIC
   `TCP_ZEROCOPY_RECEIVE`/`IORING_OP_RECV_ZC` path on paper for the real-NIC future.
4. **io_uring zero-copy variants** on the Branch-0 substrate; profile `io_uring_enter`, syscall/cs counter
   delta (predicted ≈0 at W=8).
5. **Zero-copy adoption** — custom Arrow→`adopt()` path: fixed-width + LargeBinary `values`+`offsets`
   adopted in place (offsets via `&arrow_offsets[1]`; validate `offset()==0 && offsets[0]==0`). Only
   allocations = recv buffer(s) + per-nullable-column null_map; **zero realloc**, no per-column data memcpy.
   Prove with alloc counter + profile (absence of `cloneResized`).
6. **`convertToFullColumnIfAdopted` — NOT a blanket no-op.** MUST still materialize a mutable owned column
   where callers mutate (`Squashing.cpp:346`, `HashJoin.cpp:126`). Add `ColumnNullable::convertToFullColumnIfAdopted`
   that recurses into the nested adopted column. gtest through Squashing + HashJoin + Nullable. The no-op is
   permitted ONLY on a proven-non-mutating path; the pass-through/drop path never calls it, so the realistic
   scope is HashJoin build-side only, after proving it does not mutate. **Not a DoD item.**
7. **Prompt drop** — column memory released on last `RetainToken`/shared_ptr drop when the Chunk is consumed;
   peak in-flight bounded to ~K blocks/stream. Prove via MemoryTracker/RSS peak + lifetime test; no leak.

## Predicted end-to-end magnitude (PRE-REGISTERED)
- Floor = **parity** with bespoke TCP at W=8 (today's bespoke TCP already does single-copy-recv +
  zero-copy-adopt, so Branch B's data path EQUALS it). Realistic expectation = **parity**.
- The residual gap to SHM-adopt is the irreducible kernel recv copy and is **NOT closable on this host**.
  Predicting "close the gap to SHM-adopt" on loopback is **BANNED** (real-NIC only).
- Branch B must **recover** the Branch-A String regression to parity (zero-copy adoption removes the parse).
- Genuine wins: single-stream async overlap (Branch 0) + capable-NIC future (design-only here).
- A win claimed from a mechanism the profile doesn't show is BANNED.

## ≥3 evidence-based optimization iterations (Branch B, mandatory)
Each iteration: pre-register hypothesis + predicted magnitude → implement → gate correctness → measure ≥3
instrument classes → adversarial check → log DONE/CONTINUE. Candidate iterations (order may adjust):
- **B-it1:** fused deform+Arrow-serialize → 1 userspace copy (vs serialize-then-copy). Predict producer
  SERIALIZE phase ↓, copy counter == 1/block.
- **B-it2:** zero-copy adoption of fixed-width + LargeBinary (the parse-elimination) → recover Branch-A
  String regression to parity; alloc counter == recv buf (+null_map); no cloneResized in profile.
- **B-it3:** send-side `IORING_OP_SEND_ZC` measured-null (SO_EE_CODE_ZEROCOPY_COPIED) + confirm no
  end-to-end win on loopback (honest null). Possibly a 4th iteration tuning recv buffer sizing / batching.

---

## Amendment 2026-06-26 (Branch-A readiness) — nanoarrow IPC writer confirmed
De-risked D-HC-0202 before starting Branch A: **nanoarrow 0.8.0 ships an IPC encoder** — verified the
public API in `nanoarrow_ipc.h`: `ArrowIpcEncoderInit`, `ArrowIpcEncoderEncodeSchema`,
`ArrowIpcEncoderEncodeSimpleRecordBatch`, `ArrowIpcEncoderFinalizeBuffer` (+ a higher-level
`ArrowIpcWriter`). So the C producer can emit standards-valid encapsulated Arrow IPC (Schema message +
RecordBatch messages) via nanoarrow + its vendored flatcc runtime, and the stock `ArrowColumnToCHColumn`
can be the decode oracle — the producer path is viable as decided. Branch-A implementation sequencing:
fixed-width numeric round-trip first (highest confidence) → `LargeBinary` String → Nullable/Date. (This
amends, does not change, the Branch-A plan above.)
