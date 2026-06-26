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
