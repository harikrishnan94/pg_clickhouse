# Hot-Cold Data Model — Decision log (Phases 0 & 1)

Format mirrors `dev/{tpch,clickbench}/FULL-OFFLOAD-DECISIONS.md`: `D-HC-####`, dated,
alternatives + rationale. This is an unreleased experimental product with **no
backward-compatibility constraint** — GUCs/ABIs/plans/wire formats/the `streamed_table()`
argument list may change if it serves the Goal.

---

## D-HC-0001 — `streamed_table()` transport selected by a single 3rd literal arg (a "transport spec" string)
**Date:** 2026-06-25
**Decision.** Extend `streamed_table(name, columns)` to `streamed_table(name, columns [, transport])`
where `transport` is an optional literal string:
- omitted, `'shm'`, or `'shm:adopt'` → SHM zero-copy **adopt** (default; preserves current behavior)
- `'shm:copy'` → SHM **copy** mode (Phase 0)
- `'tcp:<host>:<port>'` → **TCP** transport (Phase 1)

The consumer (`TableFunctionShm::parseArguments`) parses this arg and selects the consumer path.
This is the spec's required "per-query, at query time, via a flag argument passed to
`streamed_table(...)`" mechanism — **no ClickHouse server GUC, no rebuild** to switch.

**Alternatives considered.**
- Separate boolean `copy` arg + separate `host`/`port` args. *Rejected:* arg-count explosion across
  phases; ordering ambiguity; the host/port only apply to TCP. A single self-describing spec string
  is more extensible and keeps `parseArguments` linear.
- A ClickHouse server GUC. *Rejected:* the spec explicitly forbids a consumer-side GUC for mode
  selection ("no ClickHouse server GUC, no rebuild to switch the consumer's behavior").

**Rationale.** One extensible string covers both phases and all three modes, keeps the wire/parse
surface minimal, and is trivially emitted by the PG deparser.

---

## D-HC-0002 — PG-side mode selector = new session GUC `pg_clickhouse.shm_transport_mode`
**Date:** 2026-06-25
**Decision.** Add a session-settable GUC `pg_clickhouse.shm_transport_mode` (enum: `adopt` (default),
`copy`, `tcp`). `deparse.c` reads it when building the `streamed_table(...)` call and emits the
matching `transport` arg (D-HC-0001). For `tcp`, host/port are supplied by the producer/worker
that owns the listening socket (Phase 1 detail; see D-HC-01xx). The benchmark harness `SET`s this
GUC per sweep to drive the mode.

This PG-side selector is **distinct** from the spec's "no GUC" requirement, which constrains the
*consumer's* selection mechanism (the `streamed_table()` arg, D-HC-0001), not how the PG deparser
decides which arg value to emit. Per the spec: "that PG-side selector is distinct from the spec's
'no GUC' requirement".

**Alternatives considered.**
- Encode the mode inside the existing `pg_clickhouse.session_settings` string. *Rejected:* that string
  is forwarded to ClickHouse as `SETTINGS`, not consumed by the deparser; mixing concerns.
- A per-table reloption. *Rejected:* mode is a per-query transport choice, not a table property.

**Rationale.** A first-class enum GUC is the cleanest orchestration seam, mirrors `enable_shm_offload`
and friends, and is what the harness already uses to vary offload behavior.

---

## D-HC-0003 — Copy mode = adopt-then-`convertToFullColumnIfAdopted` inside `drainSlot`, releasing the slot early
**Date:** 2026-06-25
**Decision.** In copy mode `PollableShmSource::drainSlot` runs the existing validated zero-copy
`adopt()` (pointer setup only, no data copy), then calls `convertToFullColumnIfAdopted()` on each
**emitted** column (the projected subset). For an adopted column this is `cloneResized(size())` —
a deep `memcpy` of the logical column content into owned ClickHouse memory; for an already-owned
column it is a no-op. The transient adopted columns (and the non-emitted ones) are then dropped
**within `drainSlot`**, which fires the `RetainToken` deleter and transitions the slot to `EMPTY`
**immediately** — before the Chunk is returned, vs. the adopt path which holds the slot until the
Chunk drops downstream. The owned (copied) columns flow downstream tracked by the normal allocator.

**Alternatives considered.**
- Bespoke direct path: compute each block's data-region byte extent, `memcpy` it into an owned
  buffer, rebase descriptor offsets, then `adopt()` over the owned buffer. *Rejected for Phase 0:*
  identical per-block copy cost (still ≈ logical bytes) but materially more code (span computation,
  offset rebasing, a buffer-lifetime retain analog) and more failure surface, for no measurable
  benefit. Kept as the conceptual model for **Phase 1 TCP** (the socket recv buffer IS the owned
  buffer adopt() reconstructs from).
- Convert the FULL column set (incl. non-emitted). *Rejected:* wasteful — only emitted columns are
  read downstream; non-emitted adopted columns are dropped anyway.

**Rationale.** Maximally reuses the battle-tested `adopt()` validation; the copy is a single, clearly
attributable per-block step (`convertToFullColumnIfAdopted`) that the COPY-phase timer wraps; the
early slot release is exactly the spec's "releasing the producer's ring slot immediately" and is the
producer-side observable (PUBLISH_STALL drop) we predict.

---

## D-HC-0004 — Copy-mode observability: distinct ProfileEvents, mode-aware charge
**Date:** 2026-06-25
**Decision.** Add ProfileEvents `ShmCopiedBlocks`, `ShmCopiedBytesCharged`, `ShmCopiedBytesLogical`
plus `ShmCopyTimeMicroseconds`. Final semantics (see pre-registration amendment A1):
- `ShmCopiedBlocks` — +1 per drained block in copy/tcp mode (charge-side); the offload-oracle signal.
- `ShmCopiedBytesCharged` — charge-side, FULL-schema charged bytes (incl. safe-read padding); a
  direct mirror of `ShmAdoptedBytesCharged` so the same block payload is accounted identically in
  both modes (the "C1 same-blocks" cross-check).
- `ShmCopiedBytesLogical` — measured at the CONVERT site = the actual emitted/projected bytes the
  per-block `memcpy` materialised (`convertToFullColumnIfAdopted` → `cloneResized`). For a
  full-projection query it equals `ShmAdoptedBytesLogical` (verified on Q1); for a subset
  projection it is smaller (copy only materialises emitted columns).
- `ShmCopyTimeMicroseconds` — wall time in the convert loop; `ShmCopyTimeMicroseconds /
  ShmCopiedBytesLogical` is the in-query ns/byte (cross-checked against the microbench, C2).

The charge path is mode-aware (`AdoptedByteCharger::charge(..., bool copied)`): in **copy** mode it
bumps `ShmCopiedBlocks`/`ShmCopiedBytesCharged` and NOT `ShmAdoptedBlocks`/`ShmAdoptedBytes*`; the
MemoryTracker alloc, current-bytes gauges, and retain accounting are identical in both modes.
`ShmCopiedBytesLogical` + `ShmCopyTimeMicroseconds` are incremented at the convert site in
`PollableShmSource::drainSlot`. Result:
- adopt mode ⟺ `ShmAdoptedBlocks ≥ 1` and `ShmCopiedBlocks == 0`
- copy mode  ⟺ `ShmCopiedBlocks  ≥ 1` and `ShmAdoptedBlocks == 0`

so the offload oracle can prove *which transport mode actually ran* (not just that a base scan
offloaded), satisfying the spec's "right counter incremented" requirement.

**Alternatives considered.**
- Bump both Adopted and Copied in copy mode. *Rejected:* the oracle could not cleanly assert "adopt
  mode did NOT copy" / "copy mode did NOT take the zero-copy adopt accounting path".
- Skip the MemoryTracker charge in copy mode (pass a null ChargeHandle). *Rejected:* the transient
  adopted columns are real allocations of SHM-backed bytes for the brief window they exist; charging
  them keeps MemoryTracker honest and reuses the single charge path. The transient (one block in
  flight) is negligible and released within `drainSlot`.

**Rationale.** Minimal new surface that makes the copy "observable and attributable" (spec AC) and
gives the oracle an unambiguous per-mode signal.

---

## D-HC-0101 — TCP wire frame = SHM data-region bytes, frame-relative; consumer adopt()s over the recv buffer
**Date:** 2026-06-25
**Decision.** The TCP transport reuses the existing `adopt()` seam unchanged. Per published block the
producer serialises the SAME bytes it would lay into an SHM slot's per-block data region —
`ColumnDescriptor[schema_count]` followed by each column's value (+offsets) buffer with the identical
`align_up`/`PADDING_FOR_SIMD`/`offsets[-1]`-sentinel layout — but with all offsets **frame-relative**
(descriptors at frame offset 0, `value_offset`/`offsets_offset` relative to the frame base). The
consumer `recv()`s the frame into a 64-byte-aligned **owned** buffer, treats that buffer as
`data_region_base`, reads the descriptors at offset 0, and calls the unchanged `adopt()`. The buffer
is kept alive by a `RetainToken` whose deleter `free()`s it on last-alias drop (the TCP analog of the
SHM slot release). TCP delivery is inherently one copy (kernel→userspace `recv`), so this is the
natural generalisation of Phase-0 copy mode (adopt over an owned buffer instead of over SHM).

**Alternatives.** (a) Replicate the full SHM mapping (handshake+slots+ring) over TCP — rejected,
the ring/slot machinery is SHM-specific and pointless on a stream. (b) Build columns directly on the
consumer without `adopt()` — rejected, duplicates all per-type/validation logic that `adopt()`
centralises. (c) Reuse `publish_block`'s exact output — rejected, its offsets are ring-region-relative;
a dedicated frame-relative serializer is cleaner than parameterising the ring offset base.

**Rationale.** Maximal reuse of the validated `adopt()` + Phase-0 copy lifetime model; the framing
"matches the wire ABI semantics so adopted columns reconstruct identically" (the AC) by construction.

## D-HC-0102 — One TCP listener per producer worker; ephemeral port reported back to the PG backend
**Date:** 2026-06-25
**Decision.** Each per-stream producer worker (one per CH stream, as today for SHM) binds a TCP
listener on `127.0.0.1:0` (OS-assigned ephemeral port), `getsockname()`s its port, and publishes it
into its shared worker header. The PG backend, after `pgch_shm_worker_wait_ready`, reads each worker's
port and `shm_build_union_sql` emits `streamed_table('<name_w>','<schema>','tcp:127.0.0.1:<port_w>')`
per worker. The CH consumer parses host:port from the arg and connects. This mirrors SHM (where the
worker creates the named ring and the consumer derives the socket path from the name) — for TCP the
worker creates the listener and reports the port, because the port is not derivable from the name.

**Alternatives.** Deterministic port = base+worker_index — rejected (collision-prone, needs a free-port
search anyway). Ephemeral + report is robust and race-free.

## D-HC-0103 — One-shot handshake + framed blocks + EOS; connection close = liveness
**Date:** 2026-06-25
**Decision.** After `accept()`, the producer sends a HANDSHAKE frame: `{tcp_magic, abi_version,
schema_count}` + `SchemaEntry[schema_count]` (the 128-byte name+type_string entries — so the consumer
cross-validates the schema and recovers decimal scale / DateTime64 precision exactly as the SHM
handshake does). Then a stream of BLOCK frames: `{payload_len, row_count, eos_marker,
descriptors_offset=0}` + `payload_len` bytes (the frame-relative data region). EOS is a frame with
`eos_marker=1`. Producer death before EOS = `recv` returns 0 / `ECONNRESET` with no EOS seen →
`SHM_PRODUCER_DEATH_BEFORE_EOS`; after EOS = clean. Backpressure = blocking `send()` (TCP flow
control) replaces the SHM ring-full wait; the producer charges blocked-send time to the PUBLISH_STALL
phase so the split stays comparable. Clean teardown (close listener+conn fds, free scratch) on the
same owner-context reset callback the SHM producer uses, so abort/cancel is leak-free.

## D-HC-0104 — Separate `TcpStreamSource` consumer class; mode-dispatched in StorageShm::read
**Date:** 2026-06-25
**Decision.** A new `TcpStreamSource : ISource` handles connect → recv handshake → per-block
recv+adopt, parallel to `PollableShmSource`. `StorageShm::read()` constructs `PollableShmSource` for
Adopt/Copy and `TcpStreamSource` for Tcp (host/port threaded from `TableFunctionShm`). Shared logic
(schema cross-validation, adopt+project, charge, ProfileEvents) is small and either factored or
duplicated minimally. Counters: TCP increments the `ShmCopied*` family (it is a copy transport;
`ShmCopiedBlocks` is the oracle signal), plus a distinct `ShmTcpRecvTimeMicroseconds`/`...Bytes` if a
TCP-specific cost split is needed.

See `phase1/00-PRE-REGISTRATION.md` for the predicted magnitude + convergence plan.

---
# Phase 2 — Apache Arrow wire + zero-copy transport (D-HC-02xx)

Phase 2 moves the `streamed_table` TCP wire onto **Apache Arrow** and drives copies to the minimum, on an
io_uring/async substrate. Sequenced 0 → A → B. These decisions are pre-registered from the three
adversarial feasibility reviews (`phase2/evidence/PROMPT-FEASIBILITY-REVIEW{,-v2,-v3}.md`), which are binding.

## D-HC-0201 — Phase-2 wire format = Apache Arrow (LargeBinary String, raw fixed-width); custom zero-copy adopter
**Date:** 2026-06-26
**Decision.** The Arrow record-batch body replaces the bespoke `TcpFrame.h` `ColumnDescriptor` payload.
`String` → Arrow **`LargeBinary`** (int64 offsets); fixed-width → contiguous little-endian buffers
(== CH `PODArray`); `Nullable` → validity bitmap + nested. A **custom** Arrow→`adopt()` path does the
zero-copy adoption (Branch B); the stock `ArrowColumnToCHColumn` (which COPIES) is the independent decode
**oracle**, not the fast path.
**Why Arrow (not Native).** This fork's `ColumnString` stores **non-terminated** bytes (`ColumnString.h:43`),
so Arrow's `values` buffer *is* CH `chars` and `offsets`=`&arrow_offsets[1]` makes Arrow's leading 0 double
as CH's `offsets[-1]` sentinel — Arrow is the one standard format that is both standard AND zero-copy here.
ClickHouse Native encodes String as per-row `varint(len)+bytes` → mandatory parse copy → rejected.
**Caveats (binding).** `LargeBinary` not `Binary` (int32 offsets force a widening copy); `LargeBinary` not
`LargeUtf8` (CH String is arbitrary bytes). Adopter MUST validate `array.offset()==0 && arrow_offsets[0]==0`
(Arrow only *recommends* the leading 0; sliced arrays violate it) → else copy/error.
**Alternatives.** ClickHouse Native — rejected (String parse copy, kills zero-copy intention). Keep bespoke —
rejected (it is the format intention 1 wants to retire). Arrow Flight transport — deferred (the wire is the
substrate; Flight is a future producer).

## D-HC-0202 — Producer emits Arrow IPC via vendored **nanoarrow**; IPC alignment 64
**Date:** 2026-06-26
**Decision.** `pg_clickhouse` is a C extension with no Arrow library. Vendor **nanoarrow + nanoarrow_ipc**
(amalgamation) into `src/nanoarrow/` and link via the extension Makefile. nanoarrow's IPC writer produces
the standards-valid encapsulated `Message`/`Schema`/`RecordBatch` metadata + body, so the stock
`ArrowColumnToCHColumn` can be the decode oracle and a third party can read the bytes. Set IPC write
**alignment = 64** (≥16 minimum) so `Decimal128`/SIMD buffers land aligned and the `Buffer` (offset,length)
metadata stays consistent with the CH-padded body. Version logged at vendoring time.
**Alternatives.** Hand-rolled flatbuffer metadata — rejected (error-prone, defeats the oracle). Arrow-GLib /
full Arrow C++ in a PG bgworker — rejected (too heavy). Body-only private framing — rejected (forfeits the
stock-reader oracle + cross-engine interop, i.e. intention 1).

## D-HC-0203 — Date/DateTime/DateTime64 ship RAW (uint16 / uint32 / int64) for true zero-copy
**Date:** 2026-06-26
**Decision.** CH stores `Date` as UInt16(days), `DateTime` as UInt32(secs), `DateTime64(p)` as int64 ticks.
Standard Arrow has no 2-byte date / 4-byte timestamp (`Date32`=int32, `Timestamp`=int64) and `DateTime64(p)`
maps to a standard Arrow `Timestamp` unit only for p∈{0,3,6,9}. To keep these **zero-copy adopt**, ship the
column's **raw storage buffer** as Arrow `uint16`/`uint32`/`int64`. The CH type (and DateTime64 scale) is
recovered from the handshake/SQL schema, not the Arrow logical type.
**Accepted tradeoff (logged).** The Arrow *schema* for these 3 types is non-semantic — a third-party Arrow
reader sees plain integers — slightly weakening intention-1 interop for these types in exchange for zero copy
and matching today's adopt path. **Oracle:** supply CH type hints (`ArrowColumnToCHColumn` has raw
UINT16/UINT32 hint handling) or restore the Date/DateTime wrappers before comparing, so the oracle does not
flag a spurious type DIFF. Optional: a `DateTime64(p∈{0,3,6,9})` column MAY be tagged Arrow `Timestamp` of
the matching unit (same int64 width → still zero-copy AND semantic); default is raw.

## D-HC-0204 — Branch-0 io_uring substrate: producer IORING_OP_SEND; consumer async (PollableShmSource pattern)
**Date:** 2026-06-26
**Decision.** Producer: a per-bgworker io_uring ring replaces blocking `send()` in `tcp_send_all`
(`IORING_OP_SEND`, submit + bounded-timeout wait so the loop still polls `CHECK_FOR_INTERRUPTS` +
`origin_backend_dead`). This is the substrate for Branch-B `IORING_OP_SEND_ZC`. Consumer: convert
`TcpStreamSource` to an **async** source mirroring `PollableShmSource` (readiness eventfd + async-wake bridge
+ `prepare()→schedule()→onAsyncJobReady()` contract) so recv overlaps downstream processing. CH's
`IOUringReader` is file-only and **not** epoll-integrated (zero `IORING_OP_RECV` in-tree), so an io_uring
recv variant uses a dedicated completion thread that signals the readiness eventfd.
**Honest scope (pre-registered).** Multi-stream W=8 throughput delta ≈ 0 (cost is the kernel copy, not
syscalls; Phase-1 context-switches flat). The genuine win is the **single-stream** regime (async overlap
fixes the Phase-1 `max_threads=1` serialization). The bespoke wire is UNCHANGED in Branch 0.
**Deadlock invariant re-derived.** Blocking rule `max_threads = Σ producers ≥ #blocking sources` relaxes for
async (a source no longer pins a thread inside recv); new invariant = "every async source's readiness fd is
registered and re-scheduled on readiness/cancel/stall". Pinned with a code comment + liveness gtest.

## D-HC-0205 — Arrow-TCP selected by a distinct transport token `arrow:<host>:<port>`
**Date:** 2026-06-26
**Decision.** During the migration both wires stay selectable: bespoke TCP keeps `tcp:<host>:<port>`; the
Arrow wire uses a new token **`arrow:<host>:<port>`** (parsed in `tryParseShmTransportSpec` → a new
`ShmTransportMode::ArrowTcp`). The PG GUC `pg_clickhouse.shm_transport_mode` gains an `arrow` value that the
deparser emits. This lets every sweep compare Arrow-TCP vs bespoke-TCP on the SAME binary, and lets the
bespoke path be retired later by flipping the default — not by deleting code mid-migration.
**Alternatives.** Reuse `tcp:` and switch the wire by a server GUC — rejected (can't A/B both wires in one
binary/session; the evidence standard requires fresh same-binary comparison). Replace `tcp:` outright —
rejected (loses the bespoke parity baseline mid-branch).

## D-HC-0206 — `convertToFullColumnIfAdopted` stays materializing where callers mutate; add ColumnNullable recurse
**Date:** 2026-06-26
**Decision.** `convertToFullColumnIfAdopted` MUST continue to return a materialized, **mutable, owned**
column wherever a caller mutates the result — its only callers, `Squashing.cpp:346` (in-place squash
accumulator) and `HashJoin.cpp:126` (build-side materialization), both mutate, so a blanket `owned_full`
no-op would make them throw `READONLY` on the first join/aggregate over a streamed table. Add a
`ColumnNullable::convertToFullColumnIfAdopted` override that recurses into the nested column (the default
does not recurse → a `Nullable(adopted)` would never materialize). A no-op is permitted ONLY on a path
proven non-mutating; the zero-copy pass-through/drop path never calls this method, so the no-op buys nothing
there. This is **NOT** a Definition-of-Done item. Proven with gtests through Squashing + HashJoin + the
Nullable recurse path.
**Why (round-2 review).** The round-1 `owned_full` idea addressed only *lifetime*; it missed *mutability*.
Resolved: single-copy-recv puts the bytes in a CH-heap buffer (lifetime fine), but the column stays in
adopted PODArray mode (mutators throw), so the mutating callers still need a real owned column.

## D-HC-0207 — `arrow:` wire = a standard Arrow IPC STREAM (Schema + RecordBatch* + EOS); no bespoke handshake/block headers
**Date:** 2026-06-26
**Decision.** The `arrow:<host>:<port>` transport (D-HC-0205) carries a **standard Apache Arrow IPC stream**
— a Schema message, then one RecordBatch message per block, then the stream EOS marker (`0xFFFFFFFF`
continuation + `0x00000000` length) — and **drops** the bespoke `TcpHandshakeHeader`/`TcpBlockHeader`
framing entirely on this wire. This is what makes the bespoke `TcpFrame.h` *retire-able* on the Arrow path
(intention 1): a third-party Arrow reader / Arrow Flight could consume the stream as-is.
- **Producer** (`src/shm_arrow.c`, behind `PGCH_PRODUCER_TRANSPORT_ARROW`): builds one `ArrowSchema`
  (struct of N children) from the column schema; per block builds a hand-rolled C-Data-Interface
  `ArrowArray` whose child buffers point **zero-copy** at the already-deformed `ShmColumnPayload` buffers,
  `ArrowArrayViewSetArray`s it, and emits `ArrowIpcEncoderEncodeSchema` (once) / `EncodeSimpleRecordBatch`
  (per block) + `FinalizeBuffer(encapsulate=1)`; sends the encapsulated metadata then the body.
- **Consumer**: recovers the **exact** CH types (Decimal scale, DateTime64 precision, Date-vs-UInt16 —
  intentionally non-semantic Arrow per D-HC-0203) from the **`streamed_table()` SQL schema argument**, which
  is authoritative and already in hand. The received Arrow Schema message is validated against that (field
  count + per-field layout) — this replaces the bespoke handshake's `ShmSchemaEntry[]` cross-check.
- **Per-block framing** = the Arrow IPC encapsulated message itself (`0xFFFFFFFF` + int32 metadata_size,
  then metadata_size padded metadata bytes, then `bodyLength` body bytes). The consumer reads the small
  metadata, parses it (Arrow C++ `arrow::ipc::ReadMessage`/`Message::Open`), then reads the body into the
  to-be-adopted buffer — **exactly the Branch-B single-copy-recv shape** (small metadata read = parse, not a
  data copy; body read = the one kernel copy).
**Type mapping (Branch A, non-Nullable — the PG producer emits non-Nullable columns; the same
`ShmColumnPayload` buffers as bespoke, so results are identical by construction).** Numerics → matching Arrow
primitive; String → `LargeBinary` (int64 offsets; producer builds the `N+1` offsets `[0,end0..endN-1]` from
its `N` END offsets); Date/DateTime/DateTime64 → raw `uint16`/`uint32`/`int64` (D-HC-0203);
Decimal32/64 → raw `int32`/`int64`; **Decimal128 → Arrow `FixedSizeBinary(16)`** (16 raw 2's-complement LE
bytes == CH `Decimal128` PODArray; avoids parsing P/S and keeps it zero-copy & non-semantic, recovered from
the SQL schema). Nullable (validity bitmap ↔ CH null_map) is a **consumer + gtest** capability for a future
Arrow producer with nulls (+ the D-HC-0206 `ColumnNullable` recurse), not exercised by the current PG
producer.
**Alternatives.** Keep the bespoke `TcpHandshakeHeader` + wrap each block payload in a bespoke
`TcpBlockHeader` carrying an Arrow message — rejected (the bespoke frame is exactly what intention 1 retires;
a third party couldn't read the wire). Send no Schema message and rebuild the `arrow::Schema` on the consumer
from CH types — rejected (forfeits a valid standalone Arrow IPC stream / third-party readability for the
trivial cost of one Schema message per stream).
**Consumer source class.** Extend `TcpStreamSource` with a wire mode (Bespoke|Arrow) rather than a new
class: the async recv / wake-bridge / `onCancel` / `RetainToken` / charge machinery is wire-agnostic and was
hardened in Branch 0 — only `tryRecvBlock` (framing) and `buildChunkFromPayload` (decode) get an Arrow
branch. Avoids duplicating ~400 lines of bug-prone async code.


## D-HC-0208 — `shm_arrow_lean_extract` defaults OFF (lean Arrow extraction is wall-neutral on loopback; parked as a tested alternative)
**Date:** 2026-06-26. **Context:** Branch B iteration 3 (METHODOLOGY-LOG L0014). The lean direct-flatbuffer
Arrow RecordBatch extraction (`buildChunkFromArrowLean`) was implemented to attack the +7.4% CB Q24 wall
residual that L0012 had attributed to `arrow::ipc::ReadRecordBatch`'s per-block `arrow::Array`/`ArrayData`/
`Buffer`-slice construction (~105 cols/block on `SELECT *`).
**Measured (3 converging instruments + perf, FRESH W=8):** lean vs the it2 `ReadRecordBatch` adopt = **−0.5%
wall (within noise) — a NULL**. perf attributes the entire it2 decode path to only **~0.37% of consumer CPU**
(present in it2, provably ABSENT in lean), so eliminating it cannot move the wall. The L0012 attribution is
**falsified**: the +7% arrow-vs-bespoke residual is the producer Arrow-serialize (+55 ms/worker, partly on
the W=8 critical path) + recv-side `cons_sys`, NOT the decode (and NOT wire size: arrow `send_bytes` +0.7%).
The dominant consumer cost is the kernel recv copy (~35%, irreducible single-copy recv on this NIC-less host).
**Decision:** ship `shm_arrow_lean_extract = 0` (the standard `arrow::ipc::ReadRecordBatch` adopt path is the
default). The lean path is correct (gtests + verify_offload 137/137) and a legitimate alternative — it
eliminates the per-block `arrow::Array` allocations, which is leaner for the **capable-NIC north star** (where
the recv copy vanishes and per-block allocations matter relatively more) — but it shows **no loopback wall or
CPU benefit** and adds a dependency on arrow's internal generated flatbuffer headers (`metadata_internal.h`
→ `generated/Message_generated.h`, requiring the flatbuffers include be exposed to `dbms`). Defaulting to the
simpler standard-API path with no extra dependency is the evidence-based, conservative choice; lean stays
selectable (`shm_arrow_lean_extract=1`) for the real-NIC future and for A/B measurement.
**Alternatives.** (a) Default lean ON — rejected: ships a more-complex, dependency-adding path for zero
measured loopback benefit. (b) Revert the lean path entirely — rejected: it is correct, tested, and a
documented alternative whose elimination of the per-block construction is real (perf-proven) and relevant to
the capable-NIC future; keeping it OFF-by-default preserves the option at no imposed cost.

## D-HC-0209 — drop `validateAdoptedOffsets` by default (`shm_adopt_validate_offsets=0`); trusted producer
**Date:** 2026-06-26 (user request). **Context:** Branch B iteration 5 (METHODOLOGY-LOG L0017). The L0014
perf surfaced `ColumnString::validateAdoptedOffsets()` — an O(rows) scan that the adopted String offsets are
monotonically non-decreasing + the terminal offset equals the chars buffer size — as **~12–14% of consumer
CPU** on the String-heavy `SELECT *` cell, run by ALL adopt transports (SHM / bespoke `tcp:` / `arrow:`).
**Decision:** gate it behind `shm_adopt_validate_offsets` (default **false** = dropped). The producer is a
trusted, same-codebase PG background worker that lays out monotonic offsets by construction, so the runtime
re-validation is unnecessary overhead. **Measured (L0017):** dropping it removes **−200 ms / −31% of
consumer user CPU** on CB Q24 (perf: 12.39% → absent); the wall is unchanged (−0.8%, within noise) because
the consumer is kernel-recv-copy-bound and the freed CPU was overlapped — a real CPU/energy win that is
wall-neutral on this loopback host (would help the wall on a capable NIC / a CPU-bound query). Correctness
unaffected: `verify_offload arrow` 137/137, no new DIFF.
**Safety tradeoff (accepted, explicit):** the scan is the OOB-read guard for adopted String offsets — a
non-monotonic offset would make `sizeAt` underflow and a downstream read go out of bounds; dropping it
trades a clean `SHM_BUFFER_LAYOUT_INVALID` throw for a potential segfault **on a producer bug** (not on
correct data). **Mitigations:** (a) the producer is same-codebase + trusted; (b) the cheap O(1)
`offsets[0]==0` leading-sentinel check (which makes the `&arrow_offsets[1]` alias sound) is STILL always
performed in `adoptStringRaw` / the bespoke adopt; (c) the harness result-vs-native oracle catches any
bad-offset corruption as a `DIFF` on any correct test; (d) **reversible** — set
`shm_adopt_validate_offsets=1` to restore the full monotonicity scan (defense-in-depth for an untrusted
producer or when debugging a producer that emits malformed offsets). The method + its throw-on-bad-data are
still unit-tested directly (`gtest_adoption_layer`).
**Alternatives.** (a) Hard removal — rejected: loses the reversible safety escape hatch + the same-binary
A/B baseline. (b) Debug-build-only (`chassert`) — rejected: would not let an operator re-enable it in a
release build for an untrusted-producer deployment, and complicates the A/B measurement on the `reldeb`
binary. (c) Vectorize the scan instead of dropping it — a future option if it is ever re-enabled by default;
out of scope given the user's trusted-producer decision.

## D-HC-0301 — consumer readiness: epoll fd {socket, timerfd} replaces the eventfd + per-async bridge thread; cancel via shutdown()
**Date:** 2026-06-26. **Phase 3 Branch C1.** **Context:** the TCP/Arrow async consumer (`TcpStreamSource`)
exposed one fd to the executor via `schedule()` — a per-stream readiness **eventfd** — and woke it with a
**fresh `std::thread` spawned per async wait** (`startAsyncWakeBridge`→`asyncWakeBridgeLoop`) that `poll()`ed
{`sock_fd`, a stop-eventfd} up to the remaining stall budget and `write()`-poked the eventfd. Under steady
streaming that is ~one thread create/join per block, plus two eventfds per stream.
**Decision.** `schedule()` returns a **source-owned epoll fd** aggregating {`sock_fd`
(EPOLLIN|EPOLLRDHUP|EPOLLERR), a one-shot `timerfd` armed to the remaining stall budget}; the executor epolls
it directly (`scheduleForEvent` default `{schedule(), EPOLLIN|EPOLLERR}`, registered **level-triggered** in
`Epoll.cpp` — no EPOLLET). **Deleted** the bridge thread + both eventfds + `wakeReadyEvent`/`requestAsyncWakeBridgeStop`/
`joinAsyncWakeBridge`/`startAsyncWakeBridge`/`asyncWakeBridgeLoop`. **Cancellation stays solely
`::shutdown(sock_fd, SHUT_RDWR)`** — once async-parked the socket is in the epoll fd so shutdown fires it;
during connect/handshake the executor is *inside* `ensureConnected` (not parked; the epoll fd does not exist
yet) and the cancel flag is polled each connect-retry / SO_RCVTIMEO recv slice. No cancel eventfd is kept.
**Why (not loopback speed — a resource/mechanism change).** (a) **Eliminates the per-async `std::thread`**
create/join (≈1 per block) — the gtest proves 575 async parks spawn **0** threads (pre-C1: ~575 bridge
threads). (b) **Simplicity** — no per-async bridge lifecycle (stop-eventfd, join, drain), one less moving part.
(c) The **timerfd is load-bearing**, not hygiene: the single-threaded `PullingPipelineExecutor` waits
`async_task_queue.wait(timeout=-1)` and never calls `onAsyncJobReady`, so a stalled producer is woken ONLY by
the timerfd firing → `SHM_PRODUCER_STALL` (gtest: fires at 401ms vs a 400ms budget). `armStallTimer` floors the
one-shot at 1ms so an all-zero `itimerspec` can never disarm the timer and hang that infinite wait.
**Hot-spin safety (H2).** The returned fd is level-triggered, so every wake drains the timerfd
(`drainCounterFd`) and drives recv to socket EAGAIN before re-returning `Status::Async`; EPOLLRDHUP/EPOLLERR
are driven to a terminal EOS/throw (never re-Async). Done in BOTH the `tryGenerate` early-path
(single-threaded) and `onAsyncJobReady` (multi-threaded). gtest asserts a BOUNDED async-park count (575 ≪
100×n_blocks) — a spin would explode it.
**fd accounting (honest).** Pre-C1: 2 eventfds/stream. Post-C1: 1 epoll fd + 1 timerfd/stream → net **0**
long-lived fd delta; the real resource win is the thread elimination, not the fd count.
**Alternatives.** (a) Keep a single cancel eventfd registered in the inner epoll (H3 escape hatch) — rejected:
proven unnecessary (no parked executor exists in the socket-not-yet-in-epoll window). (b) Edge-triggered inner
epoll — rejected: the executor's outer epoll is level-triggered and the recv state machine already drains to
EAGAIN; ET adds lost-wakeup risk for no benefit. (c) A shared per-process epoll/timer thread — rejected: more
complex than the per-source epoll fd and reintroduces a thread.
