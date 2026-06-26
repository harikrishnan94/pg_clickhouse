# PROMPT.md feasibility review v2 — adversarial, evidence-based (round 2)

**Reviewer role:** independent adversarial reviewer, round 2. I did **not** write `PROMPT.md` nor the
round-1 review. I re-verified every load-bearing claim against the real code in `/home/ubuntu/ClickHouse`
(branch `streamed_table`) and `/home/ubuntu/pg_clickhouse` (branch `streamed-table-shm-offload`), plus the
Apache Arrow spec and Linux kernel facts (verified online + on-host).

**Host (re-verified this session):** `uname -r -m` = `7.0.0-1006-aws aarch64`. `ethtool -g lo` →
`netlink error: Operation not supported`. `ethtool -i ens34` → `driver: ena`; `ethtool -g ens34` →
`HDS thresh: n/a` (no header/data split). `/usr/include/linux/errqueue.h:39` defines
`SO_EE_CODE_ZEROCOPY_COPIED 1`; `SO_ZEROCOPY 60`; `MSG_ZEROCOPY` defined. Bundled Arrow = **23.0.1**
(`contrib/arrow-cmake/CMakeLists.txt:42`).

## Bottom line up front

The revision is, on balance, a **large and honest improvement** over round 1. The single best move —
switching the wire from ClickHouse Native to **Apache Arrow `LargeBinary`/`LargeUtf8`** — genuinely
resolves the round-1 "standard ⊕ zero-copy for String" contradiction, and it does so for a verifiable,
non-obvious reason: this fork's `ColumnString` stores **non-terminated** bytes (`ColumnString.h:43`), so
Arrow's `values` buffer *is* CH `chars`, and the `&arrow_offsets[1]` pointer-shift makes Arrow's leading
`0` double as CH's `offsets[-1]` sentinel. I verified this is mechanically legal in the adopted `PODArray`
mode. The reframed perf criteria (send-zc = measured null via `SO_EE_CODE_ZEROCOPY_COPIED`; recv =
single-copy; real-NIC zero-copy as a documented north-star not a loopback gate) are now honest and match
the host reality.

**But four things are still wrong or unproven, two of them blocking:**

1. **BLOCKING (new): the `owned_full` no-op `convertToFullColumnIfAdopted` is unsound.** Its *only* callers
   — `Squashing.cpp:346-347` and `HashJoin.cpp:126` — call it precisely to obtain a **mutable, owned**
   column and then mutate it. Returning the still-adopted aliasing column makes the subsequent
   `IColumn::mutate()` a no-op at refcount 1 and the following `insertRangeFrom`/`prepareForSquashing`
   throw `READONLY` via the adopted-mutator guards. The prompt reasons only about *lifetime* (ring-slot vs
   CH-heap) and misses *mutability*. Round-1 *recommended* this flag (rec 3); neither round-1 nor the
   prompt checked the callers.
2. **BLOCKING (new): `ColumnNullable` has no `convertToFullColumnIfAdopted` override** and the default does
   not recurse into the nested column. A `Nullable` wrapping an adopted nested column is therefore never
   materialized, so the same `Squashing`/`HashJoin` paths throw on every nullable streamed column. The
   prompt treats Nullable as "only a bitmap→bytemap copy" and misses this.
3. **The producer has no Arrow library** and is a C PostgreSQL extension. Emitting *valid Arrow IPC*
   (flatbuffer `Message`/`Schema`/`RecordBatch` metadata) — which the prompt itself mandates so the stock
   `ArrowColumnToCHColumn` can be the decode oracle — is un-budgeted, non-trivial C work (nanoarrow or
   hand-rolled flatbuffers).
4. **Two residual over-claims:** (a) Arrow's leading-offset-0 is **recommended, not guaranteed** by the
   spec — the adopter must validate it, the prompt says "Arrow guarantees"; (b) "close the gap toward
   SHM-adopt" is infeasible on this host because today's bespoke TCP **already** does single-copy-recv +
   zero-copy-adopt (`TcpStreamSource.cpp:288-342`), so Branch B's data path is **parity** with today's
   TCP, not a step toward SHM-adopt (the gap *is* the irreducible kernel recv copy).

None of these makes the project pointless; #1 and #2 are real correctness bugs the unattended agent will
hit the first time a join/aggregation runs over a streamed table, and the prompt currently asserts the
no-op as a Definition-of-Done item, so it will be implemented as written.

---

## 1. Round-1 findings status table

| Round-1 finding | Status | One-line evidence |
|---|---|---|
| A1 wire carries CH **Native** | **SUPERSEDED** | Wire is now Apache Arrow, not Native; the Native-specific analysis is moot. |
| A2 retire bespoke; non-PG producer | **PARTIALLY** | Arrow + Flight is a cleaner story, but SIMD-padding, raw `uint16`/`uint32` dates, and odd `DateTime64` precisions reintroduce CH-specifics (see Q8). |
| A4 "parity is the floor for every cell" vs predicted String regression | **FIXED** | Branch A now explicitly predicts + requires measuring a String-heavy regression from the copying decode, recovered in B (`PROMPT.md:274-275, 281-282`). |
| B2 send zero-copy infeasible on loopback | **FIXED** | Reframed to a *measured null result* proven via `SO_EE_CODE_ZEROCOPY_COPIED`, real-NIC as north star (`PROMPT.md:300-308`). |
| B3 recv zero-copy infeasible on this host | **FIXED** | Reframed to **single-copy recv** + design-only capable-NIC path (`PROMPT.md:309-323`); matches host (`lo` unsupported, ENA `HDS thresh: n/a`). |
| B4 io_uring low value (cost is copy, not syscalls) | **FIXED** | Recast as Branch 0 structural precursor; pre-registers multi-stream ≈0, single-stream win (`PROMPT.md:159-166`). |
| B5 "one alloc/col, exact size, zero realloc" impossible for String | **FIXED** | Arrow adopts pre-sized `values`+`offsets` buffers → no parse, no growth-doubling; budget is now "recv buffer(s) + null_map", not "one alloc per column" (`PROMPT.md:329-338, 366`). |
| B6 no-op convert infeasible w/o core fork; contradicts B3 | **PARTIALLY + NEW BUG** | Lifetime contradiction genuinely resolved (CH-heap-owned recv buffer); `owned_full` flag adopted — but it breaks the mutating callers (Q4, blocking). |
| B7 buffers dropped on chunk consume | **FIXED/UNCHANGED** | Already true; `RetainToken` last-drop frees (`TcpStreamSource.cpp:322-335`). |
| B8 "≥ today and better" | **PARTIALLY** | Floor=parity is honest; residual "close the gap toward SHM-adopt" (`PROMPT.md:386`) is still infeasible on loopback (Q6). |
| T "Native ⊕ zero-copy" unresolvable for String | **FIXED (verified)** | Arrow `LargeBinary` + non-terminated `ColumnString` + `&arrow_offsets[1]` shift is genuinely both standard and zero-copy for String (Q1). |
| §4.1 DoD self-contradiction on B3 | **FIXED** | DoD now requires single-copy recv + residual-copy-as-finding, no "prove the impossible" clause (`PROMPT.md:626-629`). |
| §4.2 B6 vs B3 | **FIXED (loopback)** | Resolved by single-copy recv into a CH-heap buffer; capable-NIC variant explicitly deferred (`PROMPT.md:347-350`). |
| §4.3 B5 one-alloc vs String two-array | **FIXED** | See B5 row. |
| §4.4 B2 instrument unsound | **FIXED** | `SO_EE_CODE_ZEROCOPY_COPIED` now mandated as the instrument (`PROMPT.md:306-308, 363`). |
| §4.7 io_uring syscall-count assumption | **FIXED** | Pre-registers ~0 multi-stream delta as the *expected* honest outcome. |

Net: round-1 is well addressed. The headline fix (Arrow) is real, not cosmetic. The remaining problems
are **new**, introduced by the revision (Q3, Q4, Q8) or surviving sloppy wording (Q6, Q1-caveat).

---

## 2. Verdict per goal (revised)

| Goal (revised) | Verdict | Reason |
|---|---|---|
| Branch 0: io_uring producer+consumer, async `TcpStreamSource` | **ACHIEVABLE-WITH-CAVEATS** | Async-source precedent exists (`RemoteSource` uses `IProcessor::Status::Async`+`schedule()`+`ReadContext`); both io_uring socket paths are net-new (CH io_uring is file-only, `IOUringReader.cpp:174`; producer has none). Deadlock invariant must be re-derived (Q5). |
| Branch A: Arrow wire, correctness, parity-with-bespoke floor | **ACHIEVABLE-WITH-CAVEATS** | Stock reader decodes `LargeBinaryArray` (`ArrowColumnToCHColumn.cpp:1824-1825`), Arrow 23.0.1 supports everything; but the **producer must emit valid Arrow IPC from C with no Arrow lib** (Q8, blocking-effort). String-heavy regression vs bespoke is expected (honest). |
| Branch B / Q1: zero-copy varlen Arrow→`ColumnString` adopt | **ACHIEVABLE-WITH-CAVEATS** | Pointer-shift is legal in adopted `PODArray` mode (verified); but leading-offset-0 is *recommended* not *guaranteed* — adopter must validate `offset()==0 && offsets[0]==0` (Q1). |
| Branch B / Q2: single-copy recv into one adopted Arrow buffer | **ACHIEVABLE-WITH-CAVEATS** | Spec allows extra inter-buffer padding (Buffer.length = logical); producer must use ≥16-byte (ideally 64) IPC alignment + emit consistent buffer offsets; recv body into a `PADDING_FOR_SIMD`-slacked aligned buffer (Q2). Today's bespoke path already does this (`TcpStreamSource.cpp`). |
| Branch B / Q3: `Nullable` = adopted nested + owned null_map | **AT-RISK (blocking)** | Structurally fine, but `ColumnNullable` lacks a `convertToFullColumnIfAdopted` override → breaks `Squashing`/`HashJoin` on nullable adopted columns (Q3). |
| Branch B / Q4: `owned_full` no-op `convertToFullColumnIfAdopted` | **INFEASIBLE-AS-STATED** | Its only callers mutate the result; a no-op makes them throw `READONLY` (Q4). The "no-op" win is largely illusory because the method's callers need a real owned column. |
| Branch B / B2: send zero-copy = measured null | **ACHIEVABLE** | Instrument exists on host (`SO_EE_CODE_ZEROCOPY_COPIED`); loopback deferred-copy is a kernel fact. |
| Branch B / B3: recv zero-copy on a capable NIC (design only) | **ACHIEVABLE (design) / INFEASIBLE (measure on host)** | `lo` and ENA both lack header/data split — cannot register an io_uring `ifq` or use `TCP_ZEROCOPY_RECEIVE`. Prompt now scopes this correctly. |
| Branch B / B8 + Q6: end-to-end ≥ today's TCP | **ACHIEVABLE as parity; "close the gap to SHM-adopt" INFEASIBLE** | Today's bespoke TCP already single-copy-recv + adopts; Branch B's data path equals it; the SHM-adopt gap is the irreducible kernel recv copy (Q6). |
| Intention 1: retire bespoke format | **ACHIEVABLE-WITH-CAVEATS** | True for String/numeric; Date/DateTime/odd-DateTime64 force a "raw-uint vs semantic-Arrow" choice (Q8). |

---

## 3. New / again findings (Q1–Q8)

### Q1 — Is the Arrow zero-copy varlen mapping correct & implementable?

**Verdict: YES, mechanically — but one spec over-claim must be fixed.**

**The pointer-shift is legal in adopted `PODArray` mode (verified).**
- The adopted `ColumnString` ctor builds the offsets array as `offsets(adopted_offsets, adopted_rows,
  &adopted_offsets)` (`ColumnString.h:112`) and `offsetAt(0)` is `offsets[-1]` (`ColumnString.h:74`).
- The adopted `PODArray` ctor sets `c_start = data` directly with **no CH-owned `pad_left`**
  (`PODArray.h:478-484`, base `PODArray.h:181-189`). Its own comment states adopted mode "does NOT
  initialise `c_start[-pad_left .. -1]`; `ColumnString` offsets that depend on the `data[-1] == 0` trick
  must arrange equivalent semantics **externally**" (`PODArray.h:178-180`). Setting
  `adopted_offsets = &arrow_offsets[1]` is exactly that external arrangement: `offsets[-1]` reads
  `arrow_offsets[0]`.
- `operator[](-1)` is permitted because `PaddedPODArray` uses `pad_left = PADDING_FOR_SIMD = 64`
  (`Core/Defines.h:26`), so the debug bound `n >= (pad_left_ ? -1 : 0)` passes (`PODArray.h:545-556`).
- `chars` adopts the Arrow `values` buffer directly (non-terminated — `ColumnString.h:43`); `int64`
  Arrow offsets ≡ `UInt64` CH offsets on this LE host.

So `sizeAt(i) = offsets[i]-offsets[i-1] = arrow_offsets[i+1]-arrow_offsets[i]` and `offsetAt(i) =
arrow_offsets[i]` — both correct, **zero data copy**, both buffers adopted. This is the genuine, clever
core of the revision and it checks out.

**Padding/sentinel satisfiable.** `PADDING_FOR_SIMD = 64` per buffer is required (`AdoptionLayer.cpp:
207-215` for the bespoke analog). Arrow IPC pads buffers to a multiple of 8 (64 recommended) and the
`Buffer.length` metadata is the *logical* length with padding allowed after — so emitting/reading 64-byte
trailing slack is spec-compliant (verified, Arrow Columnar "Buffer Alignment and Padding"; `Schema.fbs`
`Buffer` comment: "padding bytes may [follow] a buffer, but ... do not need to be accounted for in
length"). The custom adopter will *not* reuse `validateStringDescriptor` (that validates a
`ColumnDescriptor` against a data region); it needs an Arrow-native equivalent.

**Over-claim to fix (verified spec wording).** `PROMPT.md:192` ("Arrow's mandatory leading `0`") and
`FINDINGS-...md:57` ("`arrow_offsets[0] == 0` (Arrow guarantees)") are **wrong**. The Arrow Columnar spec
says: *"Generally the first slot in the offsets array is 0 ... When serializing this layout, we
**recommend** normalizing the offsets to start at 0"* (arrow.apache.org/docs/format/Columnar.html,
Variable-size Binary Layout). It is a recommendation, and it is **false for sliced arrays**, where the
physical offsets buffer is not normalized and `array.offset()` carries the slice (the stock reader handles
exactly this via `chunk.offset()` — `ArrowColumnToCHColumn.cpp:292-299`). The zero-copy adopter therefore
**must validate** `array.offset()==0 && arrow_offsets[0]==0` (and treat a violation as decode-via-copy or
error), not assume it. The producer emits fresh non-sliced normalized batches, so this holds in practice —
but the *invariant must be checked*, identical in spirit to the existing sentinel check
(`AdoptionLayer.cpp:224-234`).

### Q2 — Is "single-copy recv" actually achievable with Arrow framing?

**Verdict: YES, with the caveats the prompt already half-states — and "still interoperable Arrow" is TRUE.**

- Arrow IPC = a flatbuffer `Message` (metadata: schema/`RecordBatch` with per-`Buffer` `(offset,length)`)
  then the body. To get one kernel copy of the bulk: recv the small metadata, parse it, then recv the body
  into **one** aligned, `PADDING_FOR_SIMD`-slacked buffer and adopt column buffers as slices. This is
  exactly what today's bespoke path already does (`aligned_alloc(64, payload+PADDING+round)` at
  `TcpStreamSource.cpp:288-289`, one `recvAll` at `:297`, `adopt()` at `:342`) — so the mechanism is
  proven, only the layout source changes.
- **Extra inter-buffer SIMD padding is spec-compliant Arrow** (verified above): a third-party Arrow reader
  uses `(offset,length)` and ignores the gap. So injecting CH's 64-byte slack does **not** break
  interoperability — *provided the producer writes the `Buffer` offsets in the flatbuffer metadata to match
  the padded layout.* A stock `arrow::ipc` writer with `IpcWriteOptions::alignment = 64` does this
  automatically; a hand-rolled producer must keep body layout and metadata offsets consistent.
- **Alignment caveat (real):** Arrow IPC's *minimum* alignment is **8 bytes** (64 recommended). CH needs
  64-byte alignment for SIMD and `Decimal128` requires 16-byte natural alignment (the bespoke validator
  enforces `value_offset % elem_size == 0` — `AdoptionLayer.cpp:54`). So the producer **must** use ≥16
  (ideally 64) byte IPC alignment; the 8-byte default would yield mis-aligned `Decimal128` buffers that the
  adopter must reject. Setting alignment=64 is still valid Arrow.
- "No userspace recopy" is then true for the **body**; the metadata is a small separate recv that is parsed
  (negligible), so the single-copy claim holds for the data buffers. Note `PROMPT.md:242` ("recv into the
  to-be-adopted buffer") predates Arrow framing; with Arrow it is a two-recv split (metadata, then body) —
  worth stating so nobody scores the metadata recv as a violation.

### Q3 — Nullable: adopted nested + owned null_map

**Verdict: structurally feasible, but BLOCKING gap — `ColumnNullable` does not participate in the
adoption/convert protocol.**

- `ColumnNullable::create(nested, null_map)` calls `assumeMutable()` on both and stores them
  (`ColumnNullable.h:42-45`); `assumeMutable` does not copy, so a `ColumnNullable` *can* hold an **adopted**
  nested column (lifetime pinned by the nested column's own `AdoptionHolder`/`RetainToken`) plus an
  **owned** allocated `null_map` (`ColumnUInt8`). The bitmap→bytemap transform is indeed one small copy.
  That part of the prompt is right.
- **But `ColumnNullable` has no `convertToFullColumnIfAdopted` override** (verified: grep finds the override
  only in `ColumnString.h:156`, `ColumnVector.h:291`, `ColumnDecimal.h:174`; `ColumnNullable` falls through
  to the default `IColumn.h:150` which returns `getPtr()` and does **not** recurse). So when `Squashing` /
  `HashJoin` call `convertToFullColumnIfAdopted()` on a `Nullable(adopted)` column, the adopted **nested**
  is never materialized; the subsequent mutate path throws (see Q4). Today this is invisible because the
  existing `AdoptionLayer` produces **no** `Nullable` columns (it only adopts String + fixed-width —
  `AdoptionLayer.cpp:289-345`); Nullable is **new** in Phase 2, so this is an unflagged new requirement.
- Additional residuals the "only the null_map" claim omits: each nullable column needs (a) a null_map
  allocation, (b) a full bitmap→bytemap scan/expand pass (not free; ~1 byte/row write + 1 bit/row read),
  and (c) for the convert path, a `ColumnNullable` override that recurses into the nested adopter. These are
  small but should be in the copy-budget table, not just "1 byte/row".

### Q4 — The `owned_full` AdoptionHolder flag (no-op convert)

**Verdict: INFEASIBLE-AS-STATED. This is the headline new blocker.**

The flag's intent: set `owned_full` on `AdoptionHolder` so `convertToFullColumnIfAdopted` returns
`getPtr()` (no `cloneResized`), while `dealloc()` stays a no-op and the `RetainToken` frees
(`PROMPT.md:339-350`). The lifetime logic is sound (the recv buffer is CH-heap-owned). **The problem is
that the column stays in adopted `PODArray` mode (`isAdopted()` true, mutators throw), and the only two
callers of `convertToFullColumnIfAdopted` mutate the result:**

- `Squashing.cpp:337-348`: the first chunk's columns *become the in-place squash accumulator*:
  ```
  column = column->convertToFullColumnIfAdopted();
  mutable_columns.push_back(IColumn::mutate(std::move(column)));
  ```
  with the explicit comment that without materialization "the `prepareForSquashing()`/`insertRangeFrom()`
  below would throw `READONLY`". With `owned_full`, `convertToFullColumnIfAdopted` returns the *same*
  adopted column; `IColumn::mutate()` at refcount 1 is a no-op (COW clones only at `use_count>1` — see the
  regression test comment at `gtest_column_vector_adopted.cpp:127`); the following `insertRangeFrom`/
  `prepareForSquashing` then hit the adopted-mutator guard (`PODArray.h:353` etc.) → **`READONLY`
  exception**. This is a runtime correctness/availability bug for any squashed pipeline over a streamed
  table (i.e. most non-trivial queries).
- `HashJoin.cpp:118-134`: materializes the **build** side to release the producer ring slot
  (`actual_column = actual_column->convertToFullColumnIfAdopted();`, `:126`). For TCP the lifetime reason
  is moot (no ring), and the build side may not mutate the buffer — so a no-op *might* be safe here. But it
  is the **same method**, so you cannot make it a no-op for HashJoin without also making it a no-op for
  Squashing.

The prompt conflates two distinct reasons the copy exists: **(i) resource/lifetime** (ring-slot
starvation — TCP doesn't need the copy) and **(ii) mutability** (Squashing needs an owned, mutable
accumulator — TCP still needs it). `owned_full` only addresses (i). The existing contract test
(`gtest_column_vector_adopted.cpp:122-158`) asserts convert returns a **distinct heap-owned, mutable**
column; the prompt's proposed pointer-identity gtest would pass in isolation while the real pipeline
throws.

There is **no correctness bug of silent corruption** (the guards fail-closed with an exception), but the
query fails. So B6's "no-op convert" cannot be a blanket behavior. Either (a) keep
`convertToFullColumnIfAdopted` materializing (accept the copy at these two sites — it is exactly the
SHM-copy cost, and it only fires at join-build / squash, not on the pass-through path), or (b) introduce
the genuinely invasive PODArray "owned-external + custom-deleter, mutable" mode round-1 flagged. The
"prompt-drop" pass-through path never calls `convertToFullColumnIfAdopted` at all, so the no-op buys
nothing there. **The B3↔B6 contradiction from round 1 is genuinely resolved for lifetime, but a new
mutability contradiction is introduced and unaddressed.**

### Q5 — Branch 0 async + deadlock safety

**Verdict: ACHIEVABLE-WITH-CAVEATS; the prediction is sound.**

- Async sources are a real pattern in the CH processor model: `RemoteSource` uses
  `IProcessor::Status::Async` + `schedule()` + a `ReadContext` (epoll-backed). So an async
  `TcpStreamSource` is feasible following that precedent — but today it is a blocking `ISource`
  (`generate()`→`recvBlock()`→blocking `recvAll`, `TcpStreamSource.cpp:370-379, 93-139`) and
  `StorageShm::read` returns exactly one such source per stream (`StorageShm.cpp:88-92`). This is real
  surgery (readiness fd plumbed through the executor, partial-frame buffering across schedule cycles),
  not a flag.
- The re-derived invariant: the blocking-era rule was `max_threads = Σ producers ≥ #blocking sources`.
  Once sources are async they no longer pin a thread inside `recv`, so the invariant **relaxes** (one
  thread can service many ready streams). The *new* liveness risk is the readiness integration itself
  (a source that registers an fd but is never re-scheduled, or a partial Arrow body straddling schedule
  cycles, hangs). The invariant is still *meaningful* but changes shape: it becomes "no stream is starved
  of a wakeup", not "one thread per blocking source".
- "Single-stream overlap win, multi-stream ≈0" is **sound**: Phase-1 attributes the cost to the kernel
  recv copy with context-switches flat, and the W=8 harness already overlaps transfer across producers, so
  async overlap mostly helps the `max_threads=1` regime the harness avoids.

### Q6 — Performance honesty

**Verdict: mostly honest, but one residual over-claim.**

- **Today's bespoke TCP already does single-copy-recv + zero-copy-adopt** (`TcpStreamSource.cpp:288-342`).
  Therefore Branch B's data path (single-copy recv + Arrow adopt) is **parity** with today's bespoke TCP
  on the W=8 multi-stream regime — not an improvement, and **not** a step toward SHM-adopt. The +0.6%/+7.5%
  TCP-vs-SHM gap *is* the kernel recv copy, which is irreducible on this host (no NIC header/data split).
  So `PROMPT.md:386` ("vs SHM-adopt (close the gap)") is **infeasible on loopback** and should be deleted
  or explicitly marked real-NIC-only. The DoD's "end-to-end ≥ today's TCP at W=8" (`:629`) is the right,
  achievable floor.
- The real wins are correctly identified elsewhere: single-stream async overlap (Branch 0) and the
  capable-NIC future. The prompt should say plainly that multi-stream W=8 is expected **parity**, full
  stop.
- **Branch A's "parity is the floor" vs its own String-regression admission is now consistent** (round-1
  A4 fixed): A explicitly allows + measures a String-heavy regression from the copying decode
  (`PROMPT.md:274-275`), recovered to parity (not beyond) by B's adoption. Good — but note B *recovers to
  parity with bespoke*, it does not beat it, which reinforces the point above.

### Q7 — Residual contradictions / unprovable criteria

Covered inline and consolidated in §4. The major surviving ones: the `owned_full` no-op (Q4), the Nullable
convert gap (Q3), "close the gap to SHM-adopt" (Q6), and "Arrow guarantees leading 0" (Q1). The
copy-budget table itself is now internally consistent and honest about the residual kernel recv copy and
the null_map transform.

### Q8 — New things the revision introduced that are wrong/risky

1. **Producer cannot emit Arrow today (blocking effort).** `pg_clickhouse` has **no Arrow dependency**
   (grep: the only `arrow` hits in `src/` are the SQL `ARRAY` keyword in `deparse.c`), and the producer
   hand-rolls the bespoke frame in C (`shm_producer.c:560-618`). The prompt requires the stock
   `ArrowColumnToCHColumn` as the *independent decode oracle* (`PROMPT.md:204-207, 264`), which requires
   the producer to emit **standards-valid Arrow IPC** — flatbuffer-encoded `Schema`/`RecordBatch` metadata,
   not just the body buffers. From C this means adding **nanoarrow** (recent versions support IPC) or
   Arrow-GLib, or hand-rolling flatbuffers (error-prone). This is real, un-scoped work; the prompt never
   mentions a producer Arrow dependency or decision (`D-HC-####`). If the producer instead emits a
   body-only private framing, the stock-reader oracle is no longer usable and intention-1 interoperability
   is weakened.

2. **Date/DateTime width mismatch with standard Arrow (real tension, glossed over).** CH stores `Date` as
   `UInt16` (days) and `DateTime` as `UInt32` (seconds) — the existing adopter maps them through `UInt16`/
   `UInt32` (`AdoptionLayer.cpp:337-338`). Standard Arrow has **no** 2-byte date or 4-byte timestamp:
   `Date32` = int32 days, `Date64` = int64 ms, `Timestamp` = int64. So:
   - emit standard Arrow `Date32`/`Timestamp` → **not byte-compatible** with CH's `UInt16`/`UInt32` →
     a width-narrowing **copy** on adopt (defeats intention 2 for these types); or
   - emit raw Arrow `uint16`/`uint32` → adoptable zero-copy, but the Arrow schema no longer says
     "date/timestamp" → a third-party Arrow consumer sees plain integers (defeats intention 1's
     cross-engine semantic for these types).
   The doc's "Date* … contiguous LE array == `PODArray`" (`PROMPT.md:186-187`) is only true under the raw
   path. This is the *same* "standard ⊕ zero-copy" tension the doc claims Arrow fully dissolves — it
   survives for `Date`/`DateTime`.

3. **`DateTime64` precision vs Arrow `Timestamp` unit.** Arrow `Timestamp` units are {s,ms,us,ns}. CH
   `DateTime64(p)` allows p∈[0,9]; only p∈{0,3,6,9} map to a standard Arrow unit. Other precisions must
   ship as raw `int64` (adoptable, but not a standard Arrow timestamp) — same raw-vs-semantic tradeoff.
   (`Decimal128` itself is fine: Arrow `Decimal128` is 16-byte two's-complement LE — verified — matching CH
   on this LE host; just needs 16-byte alignment, see Q2.)

4. **`LargeUtf8` vs `LargeBinary` for CH `String`.** CH `String` is arbitrary bytes (may contain interior
   `\0`, may be invalid UTF-8). The prompt sometimes says `LargeUtf8`/`LargeBinary` interchangeably
   (`PROMPT.md:121, 188`). **`LargeBinary` is the correct choice**; `LargeUtf8` asserts validity that CH
   `String` does not guarantee, and a strict Arrow consumer/validator may reject non-UTF-8 bytes. The
   findings doc already leans `LargeBinary`; the prompt's wording should be tightened to `LargeBinary`
   only (or `LargeUtf8` only when the column is known-UTF-8).

5. **Bundled Arrow version is fine.** Arrow 23.0.1 supports `LargeBinary`/`LargeUtf8`, `Decimal128`,
   timestamps, and the stock bridge already reads `LargeBinaryArray` (`ArrowColumnToCHColumn.cpp:1824`).
   No version blocker on the **consumer** side. (The version is irrelevant to the **producer**, which has
   no Arrow at all — see #1.)

---

## 4. Internal-consistency check (quotes still inconsistent or unprovable)

1. **No-op convert vs its callers (blocking).** `PROMPT.md:344-347`: *"add an `owned_full` marker to the
   `AdoptionHolder` so `convertToFullColumnIfAdopted` returns `getPtr()` while `dealloc()` stays a no-op"*
   — contradicts `Squashing.cpp:346-347` + `HashJoin.cpp:126`, whose purpose is to obtain a mutable owned
   column. As written this is a `READONLY` regression. (Q4)

2. **Nullable "only" residual (blocking).** `PROMPT.md:200` / `:336-337`: *"the only residual consumer copy
   is the small Nullable validity-bitmap→null_map transform"* omits that `ColumnNullable` must gain a
   `convertToFullColumnIfAdopted` override to recurse into the adopted nested, or the same mutate paths
   throw. (Q3)

3. **"Arrow guarantees" the leading 0.** `PROMPT.md:192` "Arrow's mandatory leading `0`" /
   `FINDINGS-...:57` "(Arrow guarantees)" — the spec only **recommends** normalization; sliced arrays
   violate it. Must be validated by the adopter, not assumed. (Q1)

4. **"Close the gap toward SHM-adopt."** `PROMPT.md:386` — infeasible on this host; today's bespoke TCP
   already adopts, so the SHM-adopt gap is the irreducible kernel recv copy. Reword to real-NIC-only. (Q6)

5. **Producer Arrow emission unscoped.** The mandate to use the stock Arrow reader as an oracle
   (`PROMPT.md:204-207`) implies valid Arrow IPC from a C producer with no Arrow library — no decision or
   dependency is recorded. (Q8.1)

6. **`LargeUtf8`/`LargeBinary` used interchangeably** for CH `String` (arbitrary bytes). (Q8.4)

7. **`PROMPT.md:242`** "recv into the **to-be-adopted buffer**" is single-buffer language that predates the
   Arrow metadata-then-body split; the metadata recv is an expected extra small read, not a single-copy
   violation. (Q2)

---

## 5. Concrete recommendations (specific enough to edit the prompt)

1. **Fix B6 (Q4) — drop the universal no-op.** Replace `PROMPT.md:339-350` with: *"`convertToFullColumnIfAdopted`
   MUST continue to return a materialized, mutable, owned column wherever a caller mutates the result
   (`Squashing.cpp`, `HashJoin.cpp`). The `owned_full` flag may only be used to **skip the copy on paths
   that never mutate and only needed it for ring-slot release** — and since the pass-through/drop path does
   not call `convertToFullColumnIfAdopted` at all, the realistic scope of the no-op is the HashJoin
   build-side only, and only after proving the build side does not mutate the adopted buffer. Do not assert
   a blanket pointer-identity no-op as a Definition-of-Done item."* If a true owned-mutable no-op column is
   wanted, scope the invasive PODArray "owned-external + custom-deleter, mutable" mode explicitly and
   budget the hot-struct review.

2. **Fix Nullable (Q3).** Add a deliverable: *"`ColumnNullable` gains a `convertToFullColumnIfAdopted`
   override that recurses into the nested column (and returns a new `ColumnNullable` if the nested
   materialized); add a gtest for `Nullable(adopted)` through `Squashing` and `HashJoin` build."* Add the
   null_map allocation + bitmap→bytemap scan to the copy-budget table as their own rows.

3. **Fix Q1 wording + add a validation gate.** Change "Arrow guarantees the leading 0" to "Arrow
   *recommends* normalizing offsets to 0; the adopter MUST verify `array.offset()==0 &&
   arrow_offsets[0]==0` and fall back to the copying decode (or error) otherwise — sliced/unnormalized
   batches are not adoptable." Mirror the existing sentinel check (`AdoptionLayer.cpp:224-234`).

4. **Scope producer Arrow emission (Q8.1).** Add a decision `D-HC-####`: which C Arrow path the producer
   uses (nanoarrow IPC vs hand-rolled flatbuffer metadata), and require the cross-check that the bytes
   decode under the stock `ArrowColumnToCHColumn`. Require IPC write **alignment ≥ 16 (use 64)** so
   `Decimal128`/SIMD adoption is aligned.

5. **Fix Q6 over-claim.** Delete "vs SHM-adopt (close the gap)" from `PROMPT.md:386` or mark it real-NIC
   only. State explicitly: *"On loopback at W=8, Branch B is expected to be parity with today's bespoke TCP
   (which already does single-copy recv + adopt); the SHM-adopt gap is the irreducible kernel recv copy and
   is not closable on this host."*

6. **Tighten String type (Q8.4).** Specify **`LargeBinary`** for CH `String` (use `LargeUtf8` only for
   known-UTF-8 columns).

7. **Resolve Date/DateTime semantics (Q8.2-3).** Add a decision: for `Date`/`DateTime`/`DateTime64(p∉
   {0,3,6,9})`, either (a) ship raw `uint16`/`uint32`/`int64` (zero-copy, non-semantic Arrow) or (b) ship
   standard Arrow date/timestamp (semantic, with a measured conversion copy). Pick per-type and record the
   intention-1-vs-2 tradeoff; do not claim "Date* adopts zero-copy" unconditionally.

8. **Note the Arrow metadata recv (Q2).** Clarify that single-copy recv = one body copy after a small
   metadata recv; the metadata read is not a violation.

---

## 6. Confidence ledger

| Claim | Basis | Confidence |
|---|---|---|
| Non-terminated `ColumnString`; `offsetAt(0)=offsets[-1]` | `ColumnString.h:43, 74` | Verified (code) |
| Pointer-shift legal in adopted PODArray (c_start mid-buffer; `[-1]` ok with pad_left=64) | `PODArray.h:178-189, 478-484, 545-556`; `Core/Defines.h:26` | Verified (code) |
| `convertToFullColumnIfAdopted` overrides only in String/Vector/Decimal; `ColumnNullable` uses default no-recurse | grep; `IColumn.h:150`, `ColumnNullable.h` (no override) | Verified (code) |
| Squashing + HashJoin mutate the convert result → no-op throws READONLY | `Squashing.cpp:337-348`; `HashJoin.cpp:118-134`; `gtest_column_vector_adopted.cpp:122-158`; `PODArray.h` adopted guards | Verified (code) + inference (the throw is a logical consequence) |
| Today's bespoke TCP = single-copy recv + zero-copy adopt | `TcpStreamSource.cpp:288-289, 297, 342` | Verified (code) |
| `TcpStreamSource` is blocking; one source per stream | `TcpStreamSource.cpp:370-379`; `StorageShm.cpp:88-92` | Verified (code) |
| Async-source precedent exists | `RemoteSource` (`Status::Async`/`schedule`/`ReadContext`) | Verified (code presence) |
| CH io_uring is file-only; producer has none | `IOUringReader.cpp:174` (only `io_uring_prep_read`); grep producer | Verified (code) |
| Producer has no Arrow lib; emits bespoke frame with 2 userspace copies | `shm_producer.c:560-618` (memcpy at :585,:608); grep | Verified (code) |
| Bundled Arrow = 23.0.1; stock reader decodes LargeBinary | `contrib/arrow-cmake/CMakeLists.txt:42`; `ArrowColumnToCHColumn.cpp:1824-1825` | Verified (code) |
| Arrow leading offset 0 is *recommended*, not guaranteed; sliced arrays unnormalized | arrow.apache.org/docs/format/Columnar.html (Variable-size Binary Layout) | Verified (spec) |
| Arrow IPC buffer align min 8 / rec 64; `Buffer.length`=logical; extra padding allowed | Columnar.html (Buffer Alignment/Padding; RecordBatch); `format/Message.fbs`, `Schema.fbs` Buffer | Verified (spec) |
| Arrow `Decimal128` = 16-byte two's-complement, LE by default | Columnar.html; `Schema.fbs` Decimal | Verified (spec) |
| Arrow `Date32`/`Timestamp` widths ≠ CH `Date`(UInt16)/`DateTime`(UInt32) | Arrow Schema.fbs + `AdoptionLayer.cpp:337-338` | Verified (spec+code) |
| `MSG_ZEROCOPY` on loopback = deferred copy; `SO_EE_CODE_ZEROCOPY_COPIED` is the right instrument | kernel.org `networking/msg_zerocopy`; `/usr/include/linux/errqueue.h:39` | Verified (doc+host) |
| `lo` no ring; ENA no header/data split → recv zero-copy impossible on host | `ethtool -g lo` (Operation not supported); `ethtool -g ens34` (`HDS thresh: n/a`) | Verified (host, this session) |
| Multi-stream parity / single-stream win prediction | Phase-1 REPORT §7 mechanism (copy-bound, cs flat) | Inferred-from-prior-evidence |

**Not independently re-measured:** all throughput/overhead numbers (from `phase1/REPORT.md`). Everything
attributed to "code", "spec", or "host" was checked directly this session.
