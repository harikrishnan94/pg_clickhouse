# Phase 2 — investigation: varlen zero-copy wire format + io_uring sequencing

Companion to `PROMPT-FEASIBILITY-REVIEW.md`. Records two investigations that reshaped the Phase-2 plan,
with the evidence that drove each decision. Evidence standard: ≥2 independent sources per load-bearing
claim, with a confidence ledger at the end.

---

## Q(b) — Which wire format avoids the EXTRA (userspace/parse) copy for variable-length columns on the consumer recv side?

**Question scope.** "Extra copy" here means the *userspace* copy on the consumer beyond the inherent
kernel→user TCP delivery — i.e. the parse/transform from the recv buffer into the in-memory
`ColumnString`. (The separate question of eliminating the *kernel* recv copy — `copy_to_user` — is the
zero-copy-kernel-recv item B3, which `PROMPT-FEASIBILITY-REVIEW.md` shows is infeasible on this
loopback/ENA host; that is orthogonal to the format choice and remains open.)

**Answer: ClickHouse Native cannot; Apache Arrow (Large\* variants) can — uniquely among standard
formats — because this ClickHouse fork stores non-terminated strings.**

### Decisive code fact: this fork's `ColumnString` is NOT zero-terminated

```40:44:/home/ubuntu/ClickHouse/src/Columns/ColumnString.h
    /// Maps i'th position to offset to i+1'th element. Last offset maps to the end of all chars (is the size of all chars).
    Offsets offsets;

    /// Bytes of strings, placed contiguously. Note that strings are not zero-terminated and could contain zero bytes in the middle.
    Chars chars;
```

Corroborating code in the same file:
- `insert` appends exactly `s.size()` bytes (no `+1` for a terminator), then `offsets.push_back(new_size)` (`ColumnString.h:193-204`).
- `sizeAt(i) = offsets[i] - offsets[i-1]` is the true length; `getDataAt` returns `string_view(&chars[offsetAt(n)], sizeAt(n))` with no `-1` (`ColumnString.h:77-81, 181-185`).
- `offsetAt(0) = offsets[-1]`, an 8-byte zero sentinel in the offsets buffer's `pad_left` (`ColumnString.h:74`; validated in `AdoptionLayer.cpp:150-235`).

So the in-memory representation is: `chars` = concatenated string bytes (terminator-free) + `offsets` =
UInt64 cumulative end positions, with `offsets[-1] == 0`. This is exactly what `adopt()` wraps zero-copy
today (`AdoptionLayer.cpp:306-313`, `ColumnString::createAdopted`). **This is a fork change** — upstream
ClickHouse stores a `\0` after each string — and it is what makes Arrow compatible.

### Apache Arrow variable-binary layout (Arrow columnar spec)

From `arrow.apache.org/docs/format/Columnar.html` (Variable-size Binary Layout), verified:
- A varlen column = an **offsets** buffer of **`length + 1`** signed ints + a **values** (data) buffer.
- Offsets are **int32** for `Binary`/`Utf8`, **int64** for **`LargeBinary`/`LargeUtf8`**.
- `slot_position = offsets[j]`, `slot_length = offsets[j+1] - offsets[j]`.
- The first offset is **recommended** to be normalized to **0** (NOT guaranteed by the spec — sliced /
  unnormalized arrays may differ, so the adopter MUST validate it); values are concatenated with **no
  null terminators**.
- Nulls are carried in a separate **validity bitmap** (1 bit/row), not inline.

### The mapping (zero data copy)

For `LargeBinary`/`LargeUtf8` (int64 offsets), with `N` rows:

| ClickHouse `ColumnString` need | Arrow `LargeBinary` provides | copy? |
|---|---|---|
| `chars` = concatenated bytes, terminator-free | `values` buffer == identical | **0 — adopt the values buffer directly** |
| `offsets[i]` = end of string i (UInt64), `i∈[0,N)` | `arrow_offsets[i+1]` (int64, ≡ UInt64) | **0 — set CH `offsets` ptr = `&arrow_offsets[1]`** |
| `offsets[-1]` == 0 sentinel | `arrow_offsets[0]` == 0 (Arrow *recommends* normalization; adopter MUST validate) | **0 — the leading Arrow offset *is* the sentinel (when validated 0)** |

`sizeAt(i) = offsets[i]-offsets[i-1] = arrow_offsets[i+1]-arrow_offsets[i] = length of string i`. ✓
`offsetAt(i) = offsets[i-1] = arrow_offsets[i] = start of string i`. ✓

So both buffers of a `LargeBinary` column are adopted with **zero data copy**; the offsets are aliased by
a one-element pointer shift and Arrow's leading `0` (recommended-normalized; the adopter MUST validate
`array.offset()==0 && arrow_offsets[0]==0`, else fall back to copy/error) doubles as CH's `offsets[-1]`
sentinel.
Fixed-width Arrow columns are a contiguous little-endian array == CH `PODArray` → also adopted directly.

### Why Native loses and the bespoke format isn't an option

- **ClickHouse Native** encodes String as per-row `varint(len)+bytes` (`SerializationString::deserializeBinaryBulk`,
  `NativeReader` always `createColumn()`+`reserve`+`readBig`/parse). No offsets array on the wire; the
  in-memory column must be *reconstructed* → a mandatory parse copy + growth reallocs. Native gives
  intention 1 (standard) but kills intention 2 (zero-copy) for varlen.
- **Bespoke SHM layout (today)** is zero-copy because it *is* the in-memory layout — but it is exactly
  the format intention 1 wants to retire (non-standard, PG-specific).
- **Arrow Large\*** is the only candidate that is **both** standard/evolvable (Arrow Flight, cross-engine)
  **and** zero-copy for varlen here. It resolves the "Native ⊕ zero-copy" tension the feasibility review
  called unresolvable — specifically by *not* using ClickHouse Native.

### Honest caveats (must be respected by the implementation)

1. **Large variant required.** Use **`LargeBinary`** for CH `String` (`LargeUtf8` only for known-UTF-8
   columns, since CH `String` is arbitrary bytes); int64 offsets ≡ UInt64. Plain `Binary`/`Utf8` (int32)
   would force an offset-widening copy.
2. **Nullable.** Arrow validity is a 1-bit bitmap; CH `ColumnNullable` uses a 1-byte `null_map`
   (`ColumnUInt8`). Bitmap→bytemap is a small transform copy (~1 byte/row); the underlying data column
   stays adoptable. (Many hot columns may be non-nullable; quantify the null-map cost.)
3. **SIMD padding / alignment.** CH `adopt()` requires `PADDING_FOR_SIMD` trailing slack and natural
   alignment per buffer. Arrow IPC pads buffers to 8/64 bytes but not necessarily CH's `pad_right`; the
   producer must emit the message body with CH-compatible trailing pad (still Arrow-readable) or the
   consumer recvs into a slacked buffer. This is layout, not a per-byte copy.
4. **Custom adopter, not the stock reader.** The generic `ArrowColumnToCHColumn` path COPIES. Zero-copy
   requires a dedicated Arrow→`adopt()` path for the supported type subset (mirrors today's SHM adopt).
   The stock `ArrowBlockInputFormat` is then useful as an independent *correctness oracle* (decode the
   same bytes via the copying path and compare).
5. **offsets[0]==0 must hold — and MUST be validated.** True for freshly built (non-sliced) arrays, which
   the producer emits; the adopter MUST still verify `array.offset()==0 && arrow_offsets[0]==0` and fall
   back to the copying decode (or error) otherwise.

**Decision (D-HC-#### to be logged): Branch A targets Apache Arrow (`LargeBinary` for `String` + the
analogous fixed-width buffers), with a custom zero-copy adopter.** Native is not pursued as the wire.

---

## Q(a) — Can io_uring (producer + consumer) land first, before the format/copy work?

**Answer: yes — as an independent "Branch 0" precursor — provided its scope is set honestly.**

- **It will not move loopback throughput.** Phase-1 measured the TCP cost as the kernel COPY
  (bandwidth-bound: +4.1% cache-misses) with **context-switches FLAT** (`phase1/REPORT.md §7`) — syscall
  overhead is not the bottleneck, so io_uring's batching targets a non-bottleneck. Pre-register ~0
  throughput delta at W=8; a measured null result is the expected, honest outcome.
- **Its value is structural and worth landing early:** (1) an **async `TcpStreamSource`** that overlaps
  recv with downstream processing, fixing the Phase-1 single-stream `max_threads=1` serialization
  limitation (a real win, measurable in the single-stream regime); (2) the required substrate for
  io_uring `SEND_ZC`/`RECV_ZC` on a future capable NIC; (3) it de-risks the async + deadlock-safety
  change (`max_threads ≥ #blocking TCP sources`, `phase1/REPORT.md §9`) before the wire format changes.
- **Feasible in both processes:** PG18 already uses io_uring for read AIO (a bgworker can own a ring);
  ClickHouse ships `IOUringReader` (file-only) — neither has a socket path, so the network ring is
  net-new code on both sides, but there is no fundamental blocker.

**Decision: sequence Branch 0 (io_uring + async) → Branch A (Arrow) → Branch B (copy reduction).** The
async io_uring consumer in Branch 0 should be written to recv into the *to-be-adopted* buffer, so the
Arrow adopter in Branch A drops in without reworking buffer management.

---

## Confidence ledger

- **Verified in code (this repo):** `ColumnString` non-terminated layout + `sizeAt`/`getDataAt`/`offsetAt`
  semantics; `createAdopted`/`AdoptionLayer` String contract incl. the `offsets[-1]` zero sentinel;
  Native String = varint+bytes parse; that the stock Arrow reader copies.
- **Verified in Arrow spec (2 docs):** variable-binary = `length+1` offsets + values buffer; int32 vs
  int64 (Large); first offset 0; no terminators; validity bitmap for nulls.
- **Inferred (not re-measured here):** all throughput/overhead numbers and the "cost is the copy,
  context-switches flat" mechanism (from `phase1/REPORT.md`); io_uring socket-path effort estimate.
- **Open (decision pending, see feasibility review):** zero-copy *kernel* recv (B3) — infeasible on `lo`
  and on this host's ENA NIC (`TCP data split: n/a`); orthogonal to the Arrow decision above.
