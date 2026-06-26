# Hot-Cold Phase 2 — Apache Arrow wire + zero-copy TCP transport (Branch 0 · A · B)

## Mission & operating mode

You run **unattended**. Deliver the three branches below — **(0) io_uring + async transport**,
**(A) Apache Arrow serialization**, and **(B) copy reduction** — end to end, executed **0 → A → B**, and
prove each with converging evidence to the standard in this document. This is an **unreleased, experimental** product with **no backward-compatibility
constraint** — GUCs, ABIs, plans, defaults, wire formats, the `streamed_table()` argument list,
ProfileEvents, the SHM/TCP framing, anything may change if it serves the Goal. Whenever an ambiguity
arises, **exercise your own judgement and choose the option that most closely serves the Goal.** If a
decision genuinely cannot be made without a human, **record it in the Decision log**
(`dev/hotcold/DECISIONS.md`, format `D-HC-####`, dated, alternatives + rationale) and proceed with the
best-aligned default. **Never block; never wait.**

Three hard process rules specific to this task (in addition to the evidence standard below):

1. **You MUST spend at least 3 evidence-based optimization iterations before returning** (per branch
  where optimization applies — at minimum for Branch B). An "iteration" = pre-register a hypothesis +
   predicted magnitude → implement → gate correctness → measure ≥3 independent instrument classes →
   adversarial check → log the outcome with a DONE/CONTINUE verdict. Iterating once and declaring
   victory is a failure of the task.
2. **You MUST maintain an auditable methodology log** (`dev/hotcold/phase2/METHODOLOGY-LOG.md`,
  template in §"Auditable methodology log"). Every experiment, every change, every measurement gets an
   append-only entry: *what you did, how you did it, how it was verified, the result, your
   interpretation, learnings, and DONE-or-CONTINUE*. The log is a primary deliverable, not a courtesy.
3. **You MUST think holistically — end to end — before optimizing any one part.** Before touching code
  in an iteration, write down (in the log) how the change affects the *whole* dataflow (PG deform →
   serialize → kernel send → wire → kernel recv → column build → CH pipeline → drop), not just the
   local hotspot. A local win that pessimizes the system is a regression.
4. **Never block for more than 30s at a time.** When waiting on any long-running task — builds, sweeps,
  perf captures, background jobs, server restarts — poll at ~30s intervals: re-check progress/liveness
   each tick and keep moving, rather than blocking unattended on a job that may have hung or already
   finished. If a task legitimately needs longer, keep polling every 30s until it completes (or kill +
   diagnose it if it has clearly hung).

## Source control & commit discipline (hard constraint)

This task spans **two repos** — the producer (pg_clickhouse) and the consumer (ClickHouse). **All
changes must be made directly on the existing offload branches — no new feature branches, no
worktrees, no detached HEADs:**

- **pg_clickhouse** (`/home/ubuntu/pg_clickhouse`): commit on branch `**streamed-table-shm-offload`**.
- **ClickHouse** (`/home/ubuntu/ClickHouse`): commit on branch `**streamed_table`**.

Verify you are on the right branch in each repo before committing (`git rev-parse --abbrev-ref HEAD`).

**Commit as small, reviewable patches** — one logical change per commit, each independently
reviewable and (where it builds) buildable:

- Land the work as a sequence of focused commits, not one mega-diff. Natural seams (in 0→A→B order):
(1) io_uring producer send; (2) io_uring + async consumer recv; (3) Arrow serialize on the producer;
(4) Arrow reader on the consumer; (5) transport-spec/arg + mode-dispatch plumbing; (6) zero-copy adopt
of Arrow buffers (incl. `ColumnNullable` convert-recurse); (7) zero-copy kernel send
(MSG_ZEROCOPY / SEND_ZC); (8) tests/gtests; (9) each evidence/methodology doc with the change it
justifies.
- **Never commit on red.** Run the correctness gates (and build) for the touched repo before each
commit; a commit that breaks the build or an oracle is not reviewable. Measure only on committed,
green state.
- Write descriptive messages (what + why, the `D-HC-####` decision it implements, the gate that
proved it). Commit the `00-PRE-REGISTRATION.md` / `METHODOLOGY-LOG.md` / `REPORT.md` / `evidence/` /
`results/` artifacts alongside (or immediately after) the code they document, so the branch history is
itself the audit trail.
- Follow the repo git-safety rules: never touch git config; no force-push; no rebasing/amending commits
that are already pushed; do not push unless explicitly asked.

## Context (system under test) — current state, **verified, but re-verify; do not trust blindly**

SHM offload (`pg_clickhouse.enable_shm_offload`) streams a PostgreSQL heap relation's rows into a
co-located ClickHouse over a transport, exposed via ClickHouse's `streamed_table('<name>','<schema>' [,'<transport>'])` table function (`TableFunctionShm::parseArguments`). One PG background worker per CH
stream produces the rows (`shm_worker.c`, `shm_producer.c`); a vectorized page reader handles tuple
deforming + MVCC visibility (`shm_page_reader.c`). Three transports exist today, selected per query by
the 3rd literal arg (decision `D-HC-0001`, enum `ShmTransportMode` in
`ClickHouse/src/Storages/SharedMemorySource/Source/TransportMode.h`):

- `''` / `'shm'` / `'shm:adopt'` → **SHM zero-copy adopt** (default): consumer adopts producer blocks
straight out of the ring (`AdoptionLayer.cpp::adopt()`, `createAdopted`, charged in
`AdoptedByteCharger.cpp` → `ProfileEvents['ShmAdoptedBlocks']`, drained in `PollableShmSource`).
- `'shm:copy'` → **SHM copy** (Phase 0): `drainSlot` adopts then `convertToFullColumnIfAdopted()`
(a `cloneResized` deep copy) into owned memory, releasing the ring slot early. Counters: `ShmCopied*`.
- `'tcp:<host>:<port>'` → **TCP** (Phase 1): each producer worker binds an ephemeral `127.0.0.1` port,
reports it to the PG backend, and `shm_build_union_sql` emits one `tcp:127.0.0.1:<port_w>` per
worker (one connection per CH stream). See `DECISIONS.md` D-HC-0101..0104.

**The TCP path today (this is what Phase 2 rebuilds):**

- **Wire = a bespoke frame** (SHM-derived, not a standard columnar format). `ClickHouse/src/Storages/SharedMemorySource/Wire/TcpFrame.h`
defines a HANDSHAKE (`TcpHandshakeHeader` + `SchemaEntry[]`) then BLOCK frames (`TcpBlockHeader` +
`payload_len` bytes). The payload is the **SHM data-region bytes laid out frame-relative**:
`ColumnDescriptor[schema_count]` at offset 0, then each column's value (+offsets for String) buffer
with the SHM `align_up` / `PADDING_FOR_SIMD` / `offsets[-1]`-sentinel layout. The producer serializer
is `tcp_serialize_block` / `tcp_publish_block` in `src/shm_producer.c`.
- **Producer copies**: PG deforms heap → column payloads, then `tcp_serialize_block` lays them into
`p->tcp_scratch` (**one userspace copy**), then `tcp_send_all` → blocking `send(MSG_NOSIGNAL)` with a
`poll(POLLOUT)` on `EAGAIN` (the kernel `copy_from_user` is **a second copy**).
- **Consumer copies**: `TcpStreamSource::recvBlock` (`Source/TcpStreamSource.cpp`) `aligned_alloc(64,…)`s
an owned recv buffer (one per **block**), `recvAll`s `payload_len` bytes into it (the kernel
`__arch_copy_to_user` — **a third copy**), then calls the **unchanged `adopt()`** over that buffer
(zero-copy: columns alias the recv buffer), kept alive by a `RetainToken` whose deleter `free()`s it.
Counters: `ShmCopied*` family (`ShmCopiedBlocks`, `ShmCopiedBytesLogical`, `ShmCopyTimeMicroseconds`).
- `**TcpStreamSource` is a blocking leaf source** — a single TCP stream with `max_threads=1` serializes
recv with downstream processing (Phase 1 §limitations). Liveness today rests on
`max_threads = Σ producers ≥ #blocking TCP sources`.
- **Measured today (your baseline to beat, but re-measure FRESH):** in-query loopback recv
**~6.4–6.9 GB/s**; isolated loopback microbench **~7.9 GB/s (0.127 ns/byte)**; end-to-end at W=8 TCP
vs SHM-adopt **+0.6% median ClickBench / +7.5% TPC-H** (TPC-H worse because W/2 producers → fewer
consumer threads to overlap the transfer), worst cell ClickBench Q24 (`SELECT *`, ~8 GB) **+26.6%**.
Mechanism confirmed by profile: TCP's cost is the kernel `tcp_recvmsg → skb_copy_datagram_iter → __arch_copy_to_user` recv copy (Phase 1 REPORT §7).

ClickHouse already bundles **Apache Arrow** and an Arrow↔ClickHouse column bridge:
`ClickHouse/src/Processors/Formats/Impl/ArrowColumnToCHColumn.{cpp,h}`,
`ArrowBlockInputFormat.{cpp,h}`, `CHColumnToArrowColumn.{cpp,h}`, and the bundled lib under
`contrib/arrow*`. Study these before Branch A — they are the encoding you are moving the wire toward.
Note the stock bridge **copies** (general-purpose conversion); the zero-copy adopter you build for the
supported type subset is new code on top of `AdoptionLayer`, and the stock bridge becomes your
independent decode oracle.

## Goal (north star) — the two intentions

1. **Eventually remove the bespoke serialization format.** Move the `streamed_table` TCP wire onto a
   **standard columnar format — Apache Arrow** (Branch A; `LargeBinary` for `String`, the analogous
   fixed-width buffers, validity bitmaps for `Nullable`), so the transport speaks a standard, evolvable,
   cross-engine encoding instead of
   the SHM-derived `ColumnDescriptor` frame. The end state is that a future producer (even a non-PG one,
   or Arrow Flight) could speak Arrow and feed `streamed_table`, and the bespoke
   `TcpFrame.h`/`tcp_serialize_block` path can be retired. (**Apache Arrow is the chosen wire** — its
   columnar layout is byte-compatible with ClickHouse's in-memory columns, so it is the one standard
   format that can be adopted zero-copy here — see §"The deep design tension". Decision logged
   `D-HC-####`; evidence in `evidence/FINDINGS-format-and-sequencing.md`.)
2. **Reach the best performance — at least equal to today, and better — via copy reduction**
  (Branch B). The target dataflow: the producer **deforms heap tuples into temp, cache-resident
   buffers and serializes them into the **Arrow** body — exactly one userspace copy**; the Arrow bytes go
   over the wire **with no extra copies** — zero-copy kernel **send** (no `copy_from_user`), and
   zero-copy kernel **recv** (no `copy_to_user`) **where the hardware allows it** (note: infeasible on
   this loopback/ENA host — see §"deep design tension" + the feasibility review — so the kernel recv copy
   is a measured residual here, with the capable-NIC path designed for the real-NIC future); the received
   Arrow buffers are then **adopted directly into `ColumnPtr`s on the consumer with no per-column data
   copy and no per-column data allocation** (no `cloneResized` re-allocation on the pass-through path; the
   residual consumer copy is the small Nullable validity-bitmap→null_map transform); the
   adopted columns are **dropped as soon as the data is no longer needed** (the TCP-streamed block
   behaves like an in-memory table block whose `ColumnPtr`s are released once the chunk is consumed
   downstream). Use **io_uring on both producer and consumer** (Branch 0). The "no extra copies" intent
   is **symmetric** — it binds the consumer's userspace path exactly as the producer's; the *kernel*
   recv copy is the one symmetric item this host cannot remove (no NIC header/data split), so it is
   measured and reported, not hidden.

These two intentions appear to be in tension — but the tension is **resolved by the format choice**
(Apache Arrow; see §"The deep design tension"). Frame everything against the hot-cold transport
substrate: loopback is today's medium, but the eventual ceiling is **real cloud-network bandwidth
(~3 GB/s)** with overlapped hot+cold execution — so a design that wins on a real NIC but is neutral on
loopback is still correct to pursue, **provided you measure and say so honestly**. (A prior adversarial
feasibility review — `evidence/PROMPT-FEASIBILITY-REVIEW.md` — found that *kernel* zero-copy is
infeasible on this loopback/ENA host; treat its findings as binding and read it before pre-registering
any performance hypothesis.)

## The three branches (and how they converge)

Treat this as **three branches that converge**, executed **0 → A → B**:

- **Branch 0 — io_uring + async transport (structural precursor).** Move the producer send and consumer
recv onto io_uring and make `TcpStreamSource` **async** (overlap recv with downstream processing). This
lands *first* on the existing bespoke wire — it changes *how* I/O is submitted, not *what's on the
wire* — to de-risk the async + deadlock-safety change before the format changes, and to provide the I/O
substrate for later zero-copy send/recv. **Honest scope:** on loopback this is expected to be
**throughput-neutral** (Phase-1 showed the cost is the kernel COPY, not syscalls — context-switches were
flat); its real, measurable win is the **single-stream** regime (it fixes the `max_threads=1`
serialization limitation). Pre-register the ~0 multi-stream delta as the expected outcome.
- **Branch A — Apache Arrow serialization.** Replace the bespoke `TcpFrame.h` block payload with an
**Arrow** record-batch body (`LargeBinary` for `String`; contiguous little-endian buffers for
fixed-width; validity bitmap for `Nullable`). Producer writes Arrow; consumer reads Arrow. Correctness first, then performance parity
with today's bespoke TCP. This delivers intention (1).
- **Branch B — copy reduction.** On the Arrow wire, drive the copy count to the minimum: producer
one-userspace-copy serialize, zero-copy kernel send, consumer **zero-copy adoption** of the Arrow
buffers directly into `ColumnPtr`s on the pass-through path (no per-column data copy, no realloc),
prompt drop. (`convertToFullColumnIfAdopted` still materializes a mutable owned column where callers
mutate — see B6; it is NOT a blanket no-op.) This delivers intention (2).

### The deep design tension (resolved by the format choice — Apache Arrow)

Zero-copy adoption requires the wire bytes to **be** ClickHouse's in-memory column representation. Any
row-oriented or length-prefixed columnar encoding (one that stores String as per-row `varint(len)+bytes`)
does **not** satisfy this for variable-length columns — it must be *parsed* into the in-memory
`chars`+`offsets` layout = a copy. The bespoke frame is zero-copy but is exactly the format intention (1)
wants to retire. **Apache Arrow is the one standard format that is both — standard *and* zero-copy
here** — because this fork's `ColumnString` stores **non-terminated** strings (`ColumnString.h:43`),
which makes Arrow's variable-binary layout byte-compatible:

- **Numeric / Decimal** (UInt*/Int*/Float*/`Decimal128`): Arrow stores a contiguous little-endian array
  == the column's `PODArray` (Arrow `Decimal128` is 16-byte two's-complement LE, matching CH). **Adopt in
  place, zero copy** (Decimal needs 16-byte buffer alignment — see the SIMD/alignment caveat).
- **Variable-length** (`String` → Arrow **`LargeBinary`**; use `LargeUtf8` only for known-UTF-8 columns,
  since CH `String` is arbitrary bytes): Arrow = a `values` buffer (concatenated bytes, no terminators) +
  an `offsets` buffer of `N+1` **int64** values. CH `ColumnString` = `chars` (== Arrow `values`, adopted
  directly) + UInt64 `offsets` with an `offsets[-1]==0` sentinel. Set CH's `offsets` pointer to
  **`&arrow_offsets[1]`**: then `offsets[i]=arrow_offsets[i+1]` and CH's required `offsets[-1]` sentinel
  reads Arrow's leading `arrow_offsets[0]`. int64 ≡ UInt64. **Both buffers adopted, zero data copy** —
  **but** Arrow only *recommends* (does not guarantee) a normalized leading `0`; sliced/unnormalized
  batches violate it, so the adopter **MUST validate** `array.offset()==0 && arrow_offsets[0]==0` and fall
  back to the copying decode (or error) otherwise (mirror the sentinel check `AdoptionLayer.cpp:224-234`).
  (Full derivation + citations: `evidence/FINDINGS-format-and-sequencing.md`.)
- **Date / DateTime / DateTime64 — DECISION (`D-HC-####`): ship RAW for true zero-copy.** CH stores
  `Date` as `UInt16` (days), `DateTime` as `UInt32` (seconds), `DateTime64(p)` as a 64-bit tick count;
  standard Arrow has no 2-byte date / 4-byte timestamp (`Date32`=int32, `Timestamp`=int64) and
  `DateTime64(p)` only maps to a standard Arrow `Timestamp` unit for p∈{0,3,6,9}. To keep these
  **zero-copy adopt**, ship the column's **raw storage buffer** — Arrow `uint16` / `uint32` / `int64`
  respectively (`Date`/`DateTime` adopt through their fixed-width storage exactly as
  `AdoptionLayer.cpp:336-345` does today; `DateTime64`→`int64`). **Accepted tradeoff (logged):** the Arrow
  *schema* for these columns is non-semantic — a third-party Arrow reader sees plain integers, not a
  date/timestamp — which slightly weakens intention-1 interoperability for these three types in exchange
  for zero copy and matching today's adopt path. The CH type (and `DateTime64` scale/precision) is
  recovered from the handshake/SQL schema, not the Arrow logical type. **Oracle:** to compare against the
  stock `ArrowColumnToCHColumn`, supply ClickHouse type hints (it has raw `UINT16`/`UINT32` hint handling,
  `ArrowColumnToCHColumn.cpp:1833-1848`) or restore the `Date`/`DateTime`/`DateTime64` wrappers before
  comparison, so the oracle does not flag a spurious type `DIFF`. (Optional, no extra cost: a
  `DateTime64(p∈{0,3,6,9})` column MAY instead be tagged Arrow `Timestamp` of the matching unit — same
  int64 width, so still zero-copy AND semantic — if interop for those precisions is wanted; default is
  raw.)

**Mandatory caveats (respect these; they are not optional):**
- Use the **Large** variant (`LargeBinary`, int64 offsets) for `String`. Plain `Binary` (int32) would
  force an offset-widening copy.
- **Nullable:** Arrow nulls are a 1-bit validity bitmap; CH `ColumnNullable` uses a 1-**byte** null_map.
  Bitmap→bytemap is a small transform copy (~1 byte/row) + a null_map allocation; the underlying data
  column stays adoptable. Budget and measure it. **New requirement (round-2 review):** `ColumnNullable`
  has **no** `convertToFullColumnIfAdopted` override and the default does not recurse, so a `Nullable`
  wrapping an adopted nested column would never materialize and would throw `READONLY` in mutating
  callers — you MUST add a `ColumnNullable::convertToFullColumnIfAdopted` override that recurses into the
  nested column, with a gtest through `Squashing` and `HashJoin`.
- **SIMD pad / alignment:** CH `adopt()` needs `PADDING_FOR_SIMD` trailing slack + natural alignment per
  buffer. The producer must emit the Arrow body with CH-compatible trailing pad (still Arrow-readable),
  or the consumer recvs into a slacked buffer. Layout concern, not a per-byte copy.
- **Custom adopter, not the stock reader:** the generic `ArrowColumnToCHColumn`/`ArrowBlockInputFormat`
  path COPIES. Zero-copy requires a dedicated Arrow→`adopt()` path for the supported type subset
  (mirrors today's SHM `AdoptionLayer`). The stock Arrow reader is then a useful independent **correctness
  oracle** (decode the same bytes via the copying path and compare).

The **copy-budget table** (below) must still show, per type, exactly which copies remain (fixed-width: 0;
String: 0; Nullable: the small null-map transform) with an instrument proving each. Pretending Arrow is
trivially zero-copy for *all* cases (it isn't — see Nullable) is a banned, unproven causal story.

---

## Branch 0 — io_uring + async transport (structural precursor)

**Scope.** On the **existing bespoke wire** (do not change the format here), move the producer send
(`src/shm_producer.c`, `tcp_send_all`) and the consumer recv (`TcpStreamSource::recvAll`/`recvBlock`)
onto **io_uring**, and convert `TcpStreamSource` from a blocking `ISource` to an **async** source that
overlaps recv with downstream processing. Land this first, as its own small-patch series, gated on
correctness + teardown — it de-risks the async + deadlock-safety change before the format work and
provides the I/O substrate Branch B's zero-copy send/recv build on.

**Acceptance criteria.**

- Correct: passes every oracle in TCP mode (unchanged wire), **no new `DIFF`**; clean teardown (no leaked
  workers, sockets, fds) on success/error/cancel.
- **Deadlock-safety re-derived for the async source.** The Phase-1 invariant `max_threads = Σ producers
  ≥ #blocking TCP sources` was for *blocking* sources; re-derive and pin it (code comment + a liveness
  test) for the async model, incl. the single-stream `max_threads=1` case that the async source is meant
  to fix.
- io_uring actually on the path: profile shows `io_uring_enter` (not blocking `send`/`recv`); a
  syscall/context-switch counter delta is recorded.
- Performance: **no regression vs today's bespoke TCP at W=8** (within the noise band) is the floor.
  Pre-register that the **multi-stream throughput delta is ~0** (the cost is the kernel copy, not
  syscalls — Phase-1 §7). The intended *win* is the **single-stream** regime (recv overlapped with
  processing) — measure it explicitly and separately (it is not in the default W=8 harness regime).

**Pre-register** in `00-PRE-REGISTRATION.md`: the io_uring submission/completion model on both sides, the
async source's schedule/poll model and partial-frame buffering, the re-derived deadlock invariant, and
the predicted magnitudes (multi-stream ≈ 0; single-stream win quantified). Write the async consumer to
recv into the **to-be-adopted buffer** so Branch A's Arrow adopter drops in without reworking buffers.

---

## Branch A — Apache Arrow serialization

**Scope.** Make the TCP transport carry an **Apache Arrow IPC record-batch** (encapsulated message:
flatbuffer metadata + body) instead of the bespoke `TcpFrame.h` `ColumnDescriptor` payload. Producer
(`src/shm_producer.c`, the `tcp_*` path) serializes each block as Arrow (`LargeBinary` for `String`;
contiguous LE buffers for fixed-width; validity bitmap for `Nullable`); consumer (`TcpStreamSource`)
reads the Arrow message and produces the same `Chunk`s. Carry the schema (names + types incl. Decimal
scale / DateTime64 precision) in the Arrow schema / a handshake so the consumer recovers types exactly.
**Producer Arrow dependency — DECISION (`D-HC-####`): use nanoarrow.** `pg_clickhouse` is a C extension
with **no Arrow library** today; to emit *standards-valid Arrow IPC* (so the stock `ArrowColumnToCHColumn`
can be the decode oracle and a third party can read it) the producer vendors/links **nanoarrow** (its
IPC writer produces the encapsulated `Message`/`Schema`/`RecordBatch` metadata + body) — chosen over a
hand-rolled flatbuffer writer (error-prone) and over Arrow-GLib/full Arrow C++ (too heavy for a PG
bgworker). Add it as a build dependency (vendor the `nanoarrow` + `nanoarrow_ipc` amalgamation into
`src/` or via the extension Makefile) and log the version. Set IPC **alignment ≥ 16 (use 64)** so
`Decimal128`/SIMD buffers land aligned, and keep the `RecordBatch` `Buffer` (offset,length) metadata
consistent with the CH-padded body layout. The SHM transports (adopt, copy) are
unchanged and still selectable. **Branch A correctness may use the COPYING path** (decode Arrow into
owned columns) — zero-copy adoption is Branch B; here, prove the Arrow wire is correct and at parity.

**Acceptance criteria.**

- TCP-over-Arrow is selectable per query (keep `tcp:<host>:<port>`, or add a distinct spec token if you
want both bespoke and Arrow TCP selectable during the migration — log the choice `D-HC-####`).
- **Correct**: passes every oracle (below) in Arrow-TCP mode; results byte-identical to SHM-adopt /
SHM-copy / bespoke-TCP within the *same* documented fidelity bounds; **no new `DIFF`**. An
Arrow-encoding/decoding bug that corrupts bytes blocks the branch. Use the stock Arrow reader
(`ArrowColumnToCHColumn`) as an **independent decode oracle** cross-checking the custom path.
- All required types correct over Arrow: every fixed-width type, `LargeBinary` String,
  `Nullable` (validity bitmap ↔ CH null_map), empty strings, empty blocks, `count()` (no columns),
  subset projection, multi-source joins (each relation its own stream).
- The consumer's offload oracle proves the heavy fragment offloaded over Arrow-TCP (right counter
incremented), not just a base scan.
- Clean teardown (no leaked workers, sockets, fds) on success/error/cancel — reuse the leak-teardown
assertions.
- Performance gate (unambiguous): Branch A is implemented with the **copying** Arrow decode path
(zero-copy adoption is Branch B), so its gate is **per cell-class**: (i) **fixed-width/numeric cells
MUST be at parity** with today's bespoke TCP (within the noise band) — no regression there is allowed to
pass; (ii) **String-heavy cells MAY regress vs bespoke** (which already adopts), but only by a
**bounded, pre-registered magnitude** that is measured and logged, **not hidden** — Branch A goes green
with that bounded regression, and **Branch B MUST recover it to parity** via zero-copy adoption (a
Branch A String regression that exceeds the pre-registered bound, or any fixed-width regression, blocks
Branch A). Quantify Arrow-TCP vs bespoke-TCP vs SHM-adopt/copy at W=8 on ClickBench + TPC-H. Do **not**
pull Branch B's adoption into Branch A to dodge the regression — keep the branches separate.

**Pre-register (before measuring)** in `dev/hotcold/phase2/00-PRE-REGISTRATION.md`: the Arrow mapping per
type (numeric/`Decimal128`/`Date`(raw `uint16`)/`DateTime`(raw `uint32`)/`DateTime64`(raw `int64`) =
contiguous LE buffer == `PODArray`; `LargeBinary` `values`==`chars`, `offsets`==`&arrow_offsets[1]` with
leading `0` as the `offsets[-1]` sentinel; Nullable bitmap↔null_map), which types are adoptable vs
transform-required, and a **predicted magnitude** (fixed-width Arrow ≈ bespoke; String via the Branch-A
copying decode adds a parse pass → predicted +X% on String-heavy cells like ClickBench wide `SELECT *`,
recovered in Branch B). A prediction/result mismatch is a finding to investigate.

---

## Branch B — copy reduction (the concrete zero-copy protocol)

**Scope.** On the **Arrow** wire (Branch A) and the **io_uring/async** substrate (Branch 0), minimize
copies end to end and prove it. **True kernel zero-copy on both sides is the real-NIC north star, NOT a
loopback success criterion** (this host cannot do it — feasibility review §3). On this loopback/ENA host
the binding success criteria are: producer = **1 userspace copy**; send-zc = an **honest measured null
result**; recv = **single-copy** (one kernel copy of the body into the to-be-adopted buffer, no userspace
recopy); consumer = **zero-copy adoption** into `ColumnPtr`s on the pass-through path; prompt drop.
(`convertToFullColumnIfAdopted` keeps materializing where callers mutate — B6.) Implement and prove
**all** of:

1. **Producer: exactly one userspace copy.** Deform heap tuples into **temp, cache-resident buffers**
  and serialize into the **Arrow** body in that single pass (deform-and-serialize fused so the bytes are
   written once into the send buffer; no separate "lay into scratch then copy again"). Prove the byte is
   written once (profile + a per-block copy counter).
2. **Producer: zero-copy kernel send — real-NIC north star; on loopback a measured null result.**
  Implement `MSG_ZEROCOPY` (or io_uring `IORING_OP_SEND_ZC`) and handle the completion/notification
   (errqueue `SO_EE_ORIGIN_ZEROCOPY`, or the io_uring zc CQE) so the send buffer is reused only after the
   kernel releases it. **HOST REALITY (binding — review §3 + kernel `msg_zerocopy` docs):** on loopback
   every `MSG_ZEROCOPY` packet incurs a *deferred* copy — a pessimization, not an elimination — so the
   loopback success criterion is a **measured null/negative result, reported honestly**, with the deferred
   copy proven via the **`SO_EE_CODE_ZEROCOPY_COPIED`** notification flag (NOT "absence of
   `copy_from_user`", which loopback satisfies while still copying). Design the path so the real
   elimination (no `copy_from_user`) is the **real-NIC** payoff; do not claim it on this host.
3. **Consumer: zero-copy kernel recv (no `copy_to_user` in the recv path) — the symmetric aspiration to
  (2).** The ideal kernel→user delivery is NOT a `memcpy` into a staging buffer but a page-flip / DMA of
   the received bytes straight into the consumer's destination memory, via `TCP_ZEROCOPY_RECEIVE` (mmap
   page-flipping — page-alignment / full-page constraints the framing must respect) and/or io_uring
   zero-copy receive (`IORING_OP_RECV_ZC` / provided-buffer rings). **HOST REALITY (binding — feasibility
   review §3):** both require a NIC with **header/data split**, which **this host lacks** (`lo` reports
   `ethtool -g` "Operation not supported"; the ENA NIC reports `TCP data split: n/a`). So on this host the
   kernel recv copy **cannot** be eliminated. The deliverable here is therefore: (a) **design and document**
   the capable-NIC zero-copy-recv path (so it is ready for the real-NIC future the hot-cold model targets),
   (b) **measure and report the residual kernel recv copy** as the honest finding (it is the dominant TCP
   cost — Phase 1 §7), and (c) **eliminate the *userspace* recv copy** (recv straight into the to-be-adopted
   buffer; no second memcpy). **(b)+(c) together define the loopback success criterion: SINGLE-COPY RECV**
   — exactly one kernel copy of the record-batch **body** into the to-be-adopted Arrow buffer, with zero
   userspace recopy. (With Arrow IPC framing this is two recvs — a small **metadata** read then the
   **body** read; the metadata read is parsed, not a data copy, and is NOT a single-copy violation.) Do
   **not** claim the `copy_to_user`/`skb_copy_datagram_iter` frames vanished on this host — they cannot;
   claiming otherwise is banned. Pre-register all of (a)/(b)/(c) and the predicted residual-copy magnitude.
4. **io_uring zero-copy send/recv variants (on the Branch-0 substrate).** The base io_uring submission
  (both sides) + async source land in Branch 0; here, layer the **zero-copy** variants where the host
   allows: `IORING_OP_SEND_ZC` for (2). Prove via profile (`io_uring_enter` path) and a syscall/context
   -switch counter delta. (Pre-registered expectation from Branch 0: multi-stream syscall reduction is
   ~0 on loopback since the cost is the copy, not syscalls.)
5. **Consumer: zero-copy adoption of the Arrow buffers into `ColumnPtr`s — no per-column data copy, no
  realloc.** Beyond the kernel-path of (3), the received Arrow buffers are turned into columns with **no
   userspace data copy**: fixed-width buffers and `LargeBinary` `values`+`offsets` are **adopted in
   place** (offsets via the `&arrow_offsets[1]` pointer shift; Arrow's leading `0` = CH `offsets[-1]`
   sentinel) through a custom Arrow→`adopt()` path. **Allocations:** the only allocations are the recv
   buffer(s) holding the Arrow body (one per block, or per column if you recv per-column) — **no
   per-column data allocation, no `realloc`, no growth-doubling**. The **one** permitted residual copy is
   the **Nullable** validity-bitmap → CH null_map (1 byte/row) transform — budget and measure it. Prove
   with an allocation counter: per block, allocations == recv buffer(s) (+ one small null_map per
   nullable column), **zero realloc events**, and no per-column data `memcpy` in the profile.
6. **`convertToFullColumnIfAdopted` — DO NOT make it a blanket no-op (round-2 finding).** The intuition
  "the recv buffer is CH-heap-owned so we needn't clone" addresses only *lifetime*; it misses
   *mutability*. The method's **only callers** call it precisely to obtain a **mutable, owned** column and
   then mutate it: `Squashing.cpp:346` (the first chunk becomes the in-place squash accumulator —
   `insertRangeFrom`/`prepareForSquashing` would throw `READONLY` on an adopted column) and
   `HashJoin.cpp:126` (build-side materialization). A blanket `owned_full` no-op therefore returns a
   still-adopted (read-only) column and makes those callers **throw `READONLY`** on the first
   join/aggregate over a streamed table. **Required behavior:** `convertToFullColumnIfAdopted` MUST keep
   **materializing a mutable owned column wherever a caller mutates the result** (i.e. the SHM-copy
   semantics stay correct). The no-op optimization is permitted **only** on a path proven non-mutating —
   and note the **zero-copy pass-through/drop path never calls `convertToFullColumnIfAdopted` at all**, so
   the no-op buys nothing there; the realistic scope is the HashJoin build-side, and only after proving
   that site does not mutate the adopted buffer. **This is NOT a Definition-of-Done item.** (If a true
   *owned-and-mutable* no-op column is ever wanted, that requires the invasive PODArray
   "owned-external + custom-deleter, mutable" mode — scope and budget it separately; it is out of scope
   here.) The lifetime contradiction from round-1 (B3↔B6) is resolved by single-copy-recv (CH-heap
   buffer); the mutability constraint above is the remaining truth.
7. **TCP-streamed buffers behave like an in-memory table, dropped when no longer needed.** The column
  memory is released (RetainToken deleter / `shared_ptr` last-drop) as soon as the `Chunk` is consumed
   downstream — peak in-flight memory bounded to ~K blocks per stream, not the whole stream. Prove with
   the MemoryTracker / RSS peak and a lifetime test; no leak on success/error/cancel.

**The copy-budget table (mandatory deliverable).** Enumerate **every** byte movement end to end and
label each REQUIRED or ELIMINATED, with the mechanism and the instrument that proves it:


| stage                                                            | bytes moved | copy?                                  | mechanism                               | how proven                                                                                        |
| ---------------------------------------------------------------- | ----------- | -------------------------------------- | --------------------------------------- | ------------------------------------------------------------------------------------------------- |
| PG heap → deform → temp cache-resident buffer + Arrow serialize | logical     | **1 (required)**                       | fused deform+serialize                  | per-block copy counter + profile                                                                  |
| userspace → kernel (send) | logical | loopback **1** (deferred) / real-NIC **0** | MSG_ZEROCOPY / SEND_ZC | loopback: deferred copy ⇒ prove null via `SO_EE_CODE_ZEROCOPY_COPIED`; real-NIC: no `copy_from_user` |
| wire (loopback / NIC)                                            | logical     | n/a                                    | —                                       | —                                                                                                 |
| kernel → userspace (recv) | logical | loopback **1** (single-copy) / real-NIC **0** | loopback: recv into the to-be-adopted buffer; real-NIC: TCP_ZEROCOPY_RECEIVE / io_uring RECV_ZC | single-copy proven (no 2nd userspace memcpy); residual kernel copy measured (host has no NIC header/data split — review §3) |
| recv buffer → ColumnPtr (fixed-width + String `LargeBinary`)     | 0           | **0 (eliminated)**                     | Arrow buffers adopted in place; `offsets`=`&arrow_offsets[1]` | no `cloneResized`; alloc counter
| recv buffer → ColumnPtr (Nullable null_map)                      | row_count B | **1 small (required)**                 | Arrow validity bitmap → CH byte null_map | profile + null_map alloc counter                                                                  |
| ColumnPtr lifetime                                               | —           | —                                      | RetainToken drop on chunk consume       | MemoryTracker peak / leak oracle                                                                  |


Fill it with **measured** evidence per type (fixed-width, String, Nullable). The known residuals
(record them, don't bury them): the **kernel recv copy** (B3 is infeasible on this loopback/ENA host —
`evidence/PROMPT-FEASIBILITY-REVIEW.md` §3 — so this copy stays here; it is the real-NIC north star, not
a loopback win), and the **Nullable null_map** bitmap→bytemap transform. Everything else (fixed-width
and String data buffers) must be adopted with zero copy and proven so.

**Pre-register** in `00-PRE-REGISTRATION.md`: the full target dataflow, the copy-budget *predictions*
per type (fixed-width 0; String 0; Nullable null_map 1 small; kernel recv = residual on this host), the
zero-copy-kernel mechanisms you DO implement (send: MSG_ZEROCOPY / SEND_ZC), and — per direction — what
a **capable NIC** would show (no `copy_from_user`) vs what **loopback actually shows** (a deferred copy);
the correct loopback send proof is the `SO_EE_CODE_ZEROCOPY_COPIED` notification flag (a measured null
result), NOT "absence of `copy_from_user`". The recv-side reality: TCP_ZEROCOPY_RECEIVE / io_uring
RECV_ZC need NIC header/data split — unavailable on `lo` and on this ENA NIC — so the loopback success
criterion is **single-copy recv** and the kernel recv copy is a measured residual, with the capable-NIC
zero-copy design recorded for the real-NIC future. Predict the end-to-end magnitude vs today's TCP:
the floor and the realistic expectation are both **parity** — today's bespoke TCP already does
single-copy-recv + zero-copy-adopt, so Branch B's data path equals it; the residual gap to SHM-adopt is
the irreducible kernel recv copy and is **not closable on this host** (do not predict "closing the gap to
SHM-adopt" on loopback — that is real-NIC-only). The genuine wins are the single-stream async overlap
(Branch 0) and the capable-NIC future. **A win claimed from a mechanism the profile doesn't show is
banned.**

---

## Fidelity policy (read carefully)

The objective is **coverage + performance, not bit-exactness**. You **may** accept bounded output
deviations to unlock coverage (Decimal aggregate as Float64, `avg()`→Float64, display-scale, top-N tie
reshuffle under a deterministic tiebreak) — but **never hide a deviation**: every deviation must be
**detected, quantified, and logged** (which query, which column, max absolute + relative error, root
cause) in the Decision log. A silent mismatch is a failure; a measured, documented one is acceptable.

**Transport/serialization must not change results.** Arrow-TCP, zero-copy-adopt-TCP, bespoke-TCP, SHM-copy,
and SHM-adopt outputs must agree with each other and with native within the *same* bounds. The harness
`cmp.py` verdicts (`exact` / `approx ≤~1e-15` / `topN` / `empty-both` / `DIFF`) must be identical
cell-for-cell across transports; any **new** `DIFF` (or shifted `approx` bound) is a transport/encoding
bug that **blocks the branch**, not an allowed fidelity deviation. Re-derive every non-`exact` verdict
independently before accepting it. Arrow encoding edge cases to hammer: NULLs (validity bitmap ↔ null_map),
empty strings, max-length strings, Decimal scale, DateTime64 precision, empty blocks, `count()` (no
columns), subset projection, multi-source joins (each relation its own stream/socket), and the
EOS/producer-death/cancel paths.

## NON-NEGOTIABLE EVIDENCE STANDARD (this is the point of the exercise)

1. **No claim** — about speed, mechanism, correctness, or regression — may be stated unless **≥3
  INDEPENDENT sources of evidence converge** (agree in direction and roughly in magnitude). One source
   is a lead, not a conclusion.
2. **Independent = different layer/instrument that can fail differently.** Triangulate across as many of
  these CLASSES as the environment allows (these exact instruments exist in this repo — use them;
   discover more if needed):
  - **End-to-end timing** of the real workload — `dev/wsweep-report/wsweep_split.sh`
  `measure()`/`stats()`: median of **N≥5 warm** runs with **min/max + stdev**, warm-vs-cold stated,
  under the shared cgroup `cpu.max = W*period` cap (PG tree + CH server in one cgroup). **W=8**. Mirror
  the Phase-0/1 sweep drivers (`dev/hotcold/phase{0,1}/run_sweeps.sh`).
  - **Producer-side phase split** — the in-code per-phase stopwatch (`shm_log_stream_stats` "shm phase"
  LOG: READ/DEFORM/PUBLISH/STALL) **+ a SERIALIZE phase** (Arrow serialize) and the
  send accounting; cross-checked against `/proc` `off_prod` and CH `query_log` consumer CPU.
  - **Hardware counters (PMU)** — `perf stat`/`perf record` on the "pg_clickhouse shm stream" producer
  workers **and** the CH consumer threads: cycles, instructions (IPC), LLC/cache misses, branch
  misses, context-switches/syscalls. Host is **AWS Graviton aarch64** — use the ARM PMU `cycles` (or
  `task-clock` fallback). Use the PMU for the **mechanism** (a copy is bandwidth/cache-bound; a parse
  adds instructions/branches; io_uring cuts context-switches; zero-copy send removes `copy_from_user`
  cycles **on a capable NIC** — but on loopback the copy is deferred, so the authoritative send-side
  proof is the `SO_EE_CODE_ZEROCOPY_COPIED` errqueue flag, NOT the PMU/profile alone), not just the
  outcome.
  - **Profiles (perf / flamegraph)** attributing time to functions/kernel paths — show the predicted
  send/recv paths (or their absence), the Arrow serialize/deserialize frames, the `io_uring_enter`
  path, the **absence** of `cloneResized` (consumer), and **no unexpected new hotspot**. (Absence of
  `copy_from_user` on send is necessary but **not sufficient** on loopback — pair it with the
  `SO_EE_CODE_ZEROCOPY_COPIED` accounting; see Branch B item 2.)
  - **Isolated microbenchmarks / code timers** — extend the gtests in
  `ClickHouse/src/Storages/SharedMemorySource/tests/` (`gtest_tcp_stream_source.cpp` already has a
  `LoopbackThroughputMicrobench`; `gtest_adoption_layer`, `gtest_ac3_adoption_proof`,
  `gtest_in_process_producer`) — add Arrow-serialize/deserialize ns/byte, zero-copy-send loopback
  throughput, allocation-count, and `convertToFullColumnIfAdopted` materialize-on-mutate (incl.
  `Nullable` recurse) micro-tests to remove
  scan/visibility/consumer noise. Consumer `query_log` ProfileEvents are an additional instrument.
3. **PRE-REGISTER before measuring** (`00-PRE-REGISTRATION.md`): expected mechanism AND predicted
  magnitude. Then measure. A prediction/result mismatch is a finding to **investigate**, never to
   rationalize post-hoc.
4. **If sources disagree, you have NO result yet.** Root-cause the discrepancy (noise, wrong baseline,
  cache warmth, selection bias, instrument artifact) first. The effect must exceed measured noise
   (**noise band := relative diff ≤ `max(5%, 1 stdev)`** — the project's committed threshold); if effect
   ≤ noise, **say exactly that**.
5. **Control variance**: idle host (`uptime` load < 0.5 before each sweep), fixed datasets (TPC-H
  `tpch_sf10` lineitem 59,986,052; ClickBench `hits` 10M subset), warm/cold stated, identical build
   flags + DB tuning across compared modes, CPU cap identical, **socket-buffer / sysctl settings
   identical and recorded** (`net.core.{w,r}mem_max`, `SO_SNDBUF`/`SO_RCVBUF`; see
   `dev/hotcold/phase1/10-REPRODUCTION.md`). Re-measure **every** comparison mode FRESH on the SAME
   binary in the SAME session (the binary changes when your consumer changes). Record exact commands +
   env + SHAs (extend `dev/hotcold/phase2/10-REPRODUCTION.md`).
6. **BANNED**: "should be faster", "likely", "I think this helps", unproven causal stories, single-run
  numbers, cherry-picked best runs, claiming a win from end-to-end timing alone with no mechanism
   source, claiming a copy was eliminated without a profile/counter proving it, declaring done without
   the correctness oracle. If you catch yourself writing one, stop and get evidence.

## Auditable methodology log (mandatory, append-only)

Maintain `dev/hotcold/phase2/METHODOLOGY-LOG.md`. **Every** experiment/change/measurement gets an entry,
appended in order, never edited after the fact (corrections are new entries that reference the old). This
is the auditable trail the task is graded on. Template:

```
### L#### — <short title>  [branch 0|A|B]  [iteration N]  <ISO-8601 timestamp>
- **Goal / hypothesis:** what I expected and the pre-registered predicted magnitude (link the
  00-PRE-REGISTRATION line).
- **What I did:** the concrete change/experiment (files touched, commits/SHAs).
- **How I did it:** mechanism + exact commands (copy-pasteable).
- **How verified:** which ≥3 independent instrument classes, with the exact invocations.
- **Result:** RAW outputs — numbers, medians+spread, counter tables, profile excerpts (not summaries).
- **Interpretation:** what the evidence means; does it converge? noise vs effect; prediction vs
  observation.
- **Learnings:** what this changes about the design / the next step / the whole-system picture.
- **Verdict:** DONE (criterion met, evidence converges, review-ready) or CONTINUE (next hypothesis →).
```

Keep it honest: log the experiments that **failed** or showed **no effect** too — a null result that
killed a hypothesis is one of the most valuable entries.

## PER-BRANCH LOOP

For each branch (0, then A, then B), and for each optimization iteration within it:

a. **Restate** scope + acceptance criteria. **Pre-register** hypotheses + predicted magnitudes
   (`00-PRE-REGISTRATION.md`). Write the holistic end-to-end impact note (process rule 3) in the log.
b. **Implement** per spec. Write/extend the oracle + tests first where it fits (the Arrow
   encode/decode round-trip gtest cross-checked against the stock `ArrowColumnToCHColumn` reader, the
   allocation-count + `convertToFullColumnIfAdopted` materialize-on-mutate (Squashing/HashJoin/Nullable)
   micro tests, and the `streamed_table(...)` arg
   parsing are the natural TDD seams; `test/shm/verify_offload.sh` is the regression oracle). Keep the
   diff minimal and reviewable.
c. **Correctness gates** — run `test/shm/verify_offload.sh` (+ `verify_columnar.sh`,
   `verify_visibility.sh`) and the harness offload/liveness/correctness oracles in the **new mode**. The
   offload oracle must prove the heavy fragment offloaded over the new transport (new `streamed_table()`
   `QueryFinish`, the right per-mode counter ≥1, heavy operator dispatched, no residual heavy operator
   in the PG plan), not just a base scan. **If red, fix and repeat — never measure on red.**
d. **Measure** with **≥3 independent evidence CLASSES**. Produce an evidence log with **RAW** outputs:
   exact commands, env, raw numbers, medians+spread, counter tables, profiles, copy-budget table,
   prediction-vs-observation. Store under the repo convention: a phase dir mirroring
   `dev/hotcold/phase1/` (`results/<mode>/<bench>/{cells.tsv,RESULTS.md}`, `evidence/<artifact>.txt`,
   `REPORT.md`), plus cross-mode overhead tables (Branch 0: io_uring-TCP vs bespoke-TCP, + the
   single-stream overlap measurement; Branch A: Arrow-TCP vs bespoke-TCP vs SHM-adopt vs SHM-copy;
   Branch B: zero-copy-adopt-Arrow-TCP vs all).
e. **INDEPENDENT ADVERSARIAL REVIEW** (below).
f. If the review raises blocking findings **OR** evidence does not converge **OR** an acceptance
   criterion is unmet **OR** you have not yet completed 3 evidence-based optimization iterations →
   loop to (b)/(c)/(d). Else mark green, proceed.

## Independent adversarial review (after EVERY step)

After every step, run an **independent adversarial reviewer** — a fresh critical pass that did **not**
write the code. **Launch a subagent for this so its context is clean**, instructed to **attack** the
work and assume the implementer was over-optimistic, along four angles:

- **Correctness:** does the offloaded plan actually compute the query over the new transport? Arrow
encoding edge cases (NULLs via the validity-bitmap→null_map transform, empty/long strings, `LargeBinary`
offset boundaries, Decimal scale, DateTime64 precision, empty blocks, `count()`, subset projection,
multi-source joins each on its own stream, EOS/producer-death/cancel)? Cross-check against the stock
`ArrowColumnToCHColumn` decode. Is the `query_log` oracle proving the heavy fragment offloaded **and**
the intended mode ran (right counter), or just a base scan / the wrong mode?
- **Fidelity:** is every deviation vs native (and vs the other modes) detected and quantified? Is a real
encoding/framing/zero-copy corruption bug hiding behind the Decimal→Float64 allowance? Re-derive the
error independently.
- **Performance & mechanism:** ≥3 independent converging classes, pre-registration honored, noise vs
effect quantified, no cherry-picking, **mechanism shown** (PMU/profile), right baseline measured FRESH
under identical caps + sysctls. **Specifically attack the copy-budget claims**: is each "eliminated"
copy actually absent from the profile — no per-column data `memcpy`/`cloneResized` on the consumer, no
`realloc`, the String `offsets` truly aliased via the `&arrow_offsets[1]` shift (not rebuilt), and the
adopter validating `array.offset()==0 && offsets[0]==0` (not assuming Arrow normalizes)? Does
`convertToFullColumnIfAdopted` still **materialize a mutable owned column** where callers mutate
(`Squashing`/`HashJoin`) — i.e. NOT a blanket no-op that throws `READONLY` — and does `ColumnNullable`
recurse into its adopted nested column? Is io_uring actually on the path or
just linked? For the **send** side, is the loopback zero-copy claim honest — i.e. proven via
`SO_EE_CODE_ZEROCOPY_COPIED` (loopback defers the copy), not merely "absence of `copy_from_user`"? For
the **recv** side, is the residual kernel copy correctly reported as a host limitation (no NIC
header/data split) rather than hand-waved as eliminated? Is the **Nullable** null_map transform counted?
- **Holism:** does the local optimization help or hurt the *whole* dataflow under the real W=8
multi-stream regime (not a single-stream microbench)? Memory peak / lifetime correct (buffers dropped
when done, no unbounded in-flight)?

Record the verdict + any blocking findings in `dev/hotcold/phase2/evidence/ADVERSARIAL-REVIEW.md`. A
branch/iteration is **green only after the review passes.**

## Repo map (verified starting points — re-verify, don't trust blindly)

- **Harness:** `dev/wsweep-report/wsweep_split.sh` (`BENCH=tpch|clickbench … W_LIST=8 N=5 K=3`), driver
`run_all.sh`; outputs `results/<bench>/{cells.tsv,RESULTS.md}`. Oracles + phase-split parsing inside
(`check_eligible`, `correctness`→`cmp.py`, `ch_metrics`, `parse_phase`). Phase drivers:
`dev/hotcold/phase{0,1}/run_sweeps.sh`, `overhead_table.py`.
- **Conventions to mirror:** `dev/hotcold/phase{0,1}/{00-PRE-REGISTRATION,10-REPRODUCTION,REPORT}.md`,
`evidence/`, `results/<mode>/<bench>/{cells.tsv,RESULTS.md}`; decision log `dev/hotcold/DECISIONS.md`
(`D-HC-####`); adversarial-review format `dev/hotcold/phase1/evidence/ADVERSARIAL-REVIEW.md`.
- **Producer (PG extension, `src/`):** `shm_producer.c` — the `tcp_`* path:
`tcp_serialize_block`/`tcp_publish_block`/`tcp_send_all`/`tcp_accept_and_handshake`/`tcp_producer_setup`,
the `ShmColumnDescriptor`/`TcpHandshakeHeader`/`TcpBlockHeader` mirrors, `p->tcp_scratch`,
`p->tcp_conn_fd`, `p->listen_fd`, `p->tcp_port`. `shm_worker.c` (per-worker `tcp_port` in
`ShmWorkerSlot`, `pgch_shm_worker_tcp_port`), `shm_offload.c` (transport-mode GUC enum),
`shm_customscan.c` + `deparse.c` (`shm_build_union_sql` emits the per-worker `streamed_table(...)`
call). The deform path that fills column payloads is `shm_page_reader.c`.
- **Consumer (ClickHouse, `ClickHouse/src/`):** `Storages/SharedMemorySource/Source/TcpStreamSource.{cpp,h}`
(recv/connect/handshake/recvBlock/generate/onCancel — the async io_uring recv (Branch 0) + Arrow reader
+ zero-copy adopt (Branches A/B) land here),
`Source/TransportMode.h` (`ShmTransportMode`, `tryParseShmTransportSpec`), `Source/StorageShm.{cpp,h}`
(mode dispatch + host/port), `Adoption/AdoptionLayer.{cpp,h}` (`adopt()`, `createAdopted`),
`Adoption/RetainToken.h`, `Tracker/AdoptedByteCharger.cpp` + `ChargeHandle.{h,cpp}` (counters/charge),
`Wire/{TcpFrame.h,Layout.h,WireTypeMapping.h}` (current wire — basis to migrate/retire),
`Common/ProfileEvents.cpp` (the `Shm`* events — add any new ones),
`TableFunctions/TableFunctionShm.{cpp,h}` (arg parsing). Columns + adoption:
`Columns/{ColumnVector,ColumnString,ColumnDecimal}.{h,cpp}` (`createAdopted`,
`convertToFullColumnIfAdopted`, `cloneResized`), `Common/PODArray.h`.
- **Arrow format (Branch A target):** `ClickHouse/src/Processors/Formats/Impl/ArrowColumnToCHColumn.{cpp,h}`
+ `ArrowBlockInputFormat.{cpp,h}` / `CHColumnToArrowColumn.{cpp,h}` (the stock COPYING reader/writer —
use as the independent decode/encode **oracle**; the custom zero-copy adopter is new code built on
`AdoptionLayer`), the bundled Arrow lib under `contrib/arrow*`. Per-type column layouts to match:
`Columns/{ColumnString,ColumnVector,ColumnDecimal,ColumnNullable}.{h,cpp}`. **Producer side:** the C
extension emits Arrow IPC via **nanoarrow** (`nanoarrow` + `nanoarrow_ipc`, vendored into `src/` or via
the extension Makefile) — there is no Arrow lib in `pg_clickhouse` today.
- **io_uring (Branch 0):** `ClickHouse/src/Disks/IO/IOUringReader.{cpp,h}` (existing file-only io_uring —
pattern reference; the socket path is new); `liburing` is available at runtime. PG side: a per-bgworker
ring in `shm_worker.c`/`shm_producer.c` (PG18 already uses io_uring for read AIO — pattern reference).
- **Regression oracles + tests:** `test/shm/verify_offload.sh` (offload proven from CH system tables +
leak teardown), `verify_columnar.sh`, `verify_visibility.sh`; gtests under
`ClickHouse/src/Storages/SharedMemorySource/tests/` (esp. `gtest_tcp_stream_source.cpp`).
- **Environment of record:** AWS Graviton aarch64, 32c/61 GiB, dedicated/idle; PG 18 (`:5432`, log
`/var/log/postgresql/postgresql-18-main.log`); CH build `reldeb` v26.6.1.1, `RUN_ID=tpchcb`, HTTP
`:21002` (resolve the **live** pid from `ss -ltnp`, the manifest pid is stale); shared cgroup-v2
`cpu.max` cap. Build/restart/install recipe + sysctls: `dev/hotcold/phase1/10-REPRODUCTION.md`.
Confirm + re-record SHAs at pre-registration.

## Deliverables (per branch, plus the cross-cutting ones)

1. The branch work landed as **small, reviewable commits** on `streamed-table-shm-offload`
  (pg_clickhouse) and `streamed_table` (ClickHouse) — one logical change per commit, each green
   before commit — implementing the branches (Branch 0: io_uring + async; Branch A: Arrow
   serialize/deserialize; Branch B: the zero-copy adoption protocol + send-side zero-copy), their
   observability counters/phases, and the `streamed_table(...)` arg.
2. Green correctness gates (regression suite + harness oracles) in the new mode.
3. `dev/hotcold/phase2/00-PRE-REGISTRATION.md` (mechanism + predicted magnitude + copy-budget
  predictions, written first).
4. Evidence log with RAW outputs across ≥3 instrument classes: harness `cells.tsv`/`RESULTS.md` at W=8
  for ClickBench + TPC-H, producer phase split incl. SERIALIZE/send, PMU tables, perf profiles,
   microbench, the **copy-budget table** (per type, measured), and the **overhead tables** (0: io_uring
   -TCP vs bespoke-TCP + single-stream overlap; A: Arrow-TCP vs bespoke-TCP vs SHM-adopt/copy; B:
   zero-copy-adopt-Arrow-TCP vs all), each with prediction-vs-observation.
5. `dev/hotcold/phase2/METHODOLOGY-LOG.md` — the complete append-only auditable log (≥3 optimization
  iterations for Branch B), including failed/null experiments.
6. `dev/hotcold/phase2/evidence/ADVERSARIAL-REVIEW.md` with a passing verdict per step.
7. Decision-log entries (`D-HC-####`) for every judgement call — esp. the Arrow format choice; the
  producer Arrow-IPC emission path (**nanoarrow** — vendored/linked, version logged; IPC alignment ≥16/64); the
  per-type adoption resolution for variable-length / Nullable; and the **Date/DateTime/DateTime64 =
  raw `uint16`/`uint32`/`int64` (true zero-copy)** decision with its accepted non-semantic-Arrow-schema
  interop tradeoff — and every bounded fidelity deviation.
8. `dev/hotcold/phase2/10-REPRODUCTION.md` (exact commands, env, SHAs, GUCs, sysctls, cgroup + port
  resolution, build/restart recipe).
9. A `REPORT.md` per branch with the GREEN verdict, and a closing note on intention (1) — how close the
  bespoke format is to being retire-able — and intention (2) — the measured best-vs-today delta with
   mechanism.

## Definition of done (all required)

- Branch 0 green: io_uring + async transport correct on the bespoke wire, no new `DIFF`, clean teardown,
deadlock-safety re-derived; no regression vs bespoke TCP at W=8; the single-stream overlap win measured;
io_uring proven on the path; evidence converges, review passed.
- Branch A green: Arrow-TCP correct (cross-checked vs the stock Arrow decoder), no new `DIFF`;
fixed-width/numeric cells at parity with bespoke TCP (within noise), String-heavy cells within the
**pre-registered bounded regression** of the copying decode (measured + logged, recovered to parity in
Branch B); evidence converges, review passed.
- Branch B green: the copy-budget table is **measured** and each ELIMINATED copy is **profile-proven**;
consumer proofs (a) no per-column userspace data copy on the pass-through path (fixed-width + String
adopted; String `offsets` aliased via `&arrow_offsets[1]`, with `array.offset()==0 && offsets[0]==0`
validated), (b) no per-column data allocation / zero realloc (only the recv buffer(s) + the small Nullable
null_map), (c) `convertToFullColumnIfAdopted` still **materializes a mutable owned column** where callers
mutate (`Squashing`/`HashJoin` pass — NOT a blanket no-op) and `ColumnNullable` recurses into its adopted
nested, (d) prompt drop / bounded memory — all proven; the **send-side** zero-copy is
honestly proven (loopback: a measured null result via `SO_EE_CODE_ZEROCOPY_COPIED`, accepting the
deferred copy); the **recv-side** meets the loopback success criterion **single-copy recv** (one kernel
copy into the to-be-adopted buffer, no userspace recopy), with the residual kernel copy reported as a
measured finding (this host has no NIC header/data split — review §3) and the capable-NIC zero-copy
design recorded; end-to-end ≥ today's TCP at W=8 (and the mechanism explains any shortfall).
- ≥3 evidence-based optimization iterations logged in `METHODOLOGY-LOG.md`.
- Every claim backed by ≥3 independent converging instruments; every deviation logged; reproduction
recorded.
- All work committed as small, reviewable patches on `streamed-table-shm-offload` (pg_clickhouse) and
`streamed_table` (ClickHouse) — each commit green, no mega-diff, history is the audit trail.

