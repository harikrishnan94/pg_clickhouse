# PROMPT.md feasibility review — adversarial, evidence-based

**Reviewer role:** independent adversarial reviewer. I did **not** write `PROMPT.md`; I attacked it.
**Scope:** is each stated GOAL of `dev/hotcold/phase2/PROMPT.md` achievable on **correctness** and
**performance** grounds, given the real code in `/home/ubuntu/pg_clickhouse` (branch
`streamed-table-shm-offload`) and `/home/ubuntu/ClickHouse` (branch `streamed_table`), plus verifiable
kernel facts.
**Host of record (verified):** `uname -r` = `7.0.0-1006-aws`, `uname -m` = `aarch64`, Ubuntu 26.04,
glibc 2.43. NICs: `lo`, `ens34` (driver `ena`, version `7.0.0-1006-aws`), `tailscale0`, `docker0`.
`liburing.so.2` present at runtime.

**Bottom line up front.** Branch A (Native on the wire) is achievable. Branch B as *written* is **not**:
its two headline guarantees — (a) **zero-copy kernel receive on loopback** and (b) a **no-op
`convertToFullColumnIfAdopted` over CH-heap-owned, zero-copy, exact-size, single-allocation columns** —
are individually infeasible-as-stated on this host *and* mutually contradictory, and the central
"Native ⊕ zero-copy for all types" promise is impossible for `String` by construction. The performance
north star ("≥ today and better") is unprovable on loopback because the only material cost (the recv
copy) cannot be removed on loopback or even on this host's real NIC (ENA reports `TCP data split: n/a`).
None of this makes the work pointless — but the prompt's success *criteria* must be reworded or they
will force the unattended agent to either fail honestly or fake a win.

---

## 1. Verdict per goal

| # | Goal (from PROMPT.md) | Verdict | One-line reason |
|---|---|---|---|
| A1 | TCP wire carries ClickHouse **Native** blocks (producer writes, consumer reads) | **ACHIEVABLE** | `NativeWriter`/`NativeReader` + per-type `ISerialization` already round-trip every required type incl. Decimal scale / DateTime64 precision. |
| A2 | Eventually **retire bespoke `TcpFrame.h`**; a non-PG producer could speak Native | **ACHIEVABLE-WITH-CAVEATS** | True for pure Native. But Branch B resolution (ii) ("Native-compatible adoptable" String) re-introduces a bespoke variant → the bespoke layout is *renamed*, not retired. |
| A3 | Correct: no new `DIFF` vs other transports | **ACHIEVABLE** | Native is lossless for the supported type set; the existing handshake/type-string validation carries scale/precision. |
| A4 | **Perf parity with bespoke TCP** is the floor (all cells) | **ACHIEVABLE-WITH-CAVEATS** | Fixed-width: parity. String-heavy cells: Native adds a parse pass the bespoke frame avoids — the prompt itself predicts a regression there, so "parity (floor) for every cell" is mis-stated. |
| B1 | Producer: **exactly one userspace copy** (fused deform+serialize into Native) | **ACHIEVABLE** | Today is already ~1 userspace copy (`tcp_serialize_block`); fusing deform+serialize is a straightforward refactor. |
| B2 | **Zero-copy kernel SEND** (no `copy_from_user`) via `MSG_ZEROCOPY`/`SEND_ZC` | **INFEASIBLE-AS-STATED (loopback)** | Kernel doc: every `MSG_ZEROCOPY` packet **looped to a local socket incurs a deferred copy**; on loopback it is a pessimization, not an elimination. |
| B3 | **Zero-copy kernel RECV** (no `copy_to_user`/`skb_copy_datagram_iter`) — a **HARD** requirement | **INFEASIBLE-AS-STATED (this host)** | `TCP_ZEROCOPY_RECEIVE` needs page-aligned payloads via NIC header/data split; `io_uring` zcrx needs a HW Rx queue + flow steering. `lo` has neither; ENA reports `TCP data split: n/a`. Impossible on loopback **and** this machine's real NIC. |
| B4 | **io_uring on both sides** | **ACHIEVABLE (mechanically), LOW-VALUE** | Wireable in both processes, but Phase-1 profile shows the cost is the **copy (bandwidth)**, not syscalls (context-switches flat) — io_uring removes a cost that isn't the bottleneck. |
| B5 | Consumer: **one allocation per column of exact size, zero realloc** | **INFEASIBLE-AS-STATED** | `String`: chars total is unknown from the Native stream → `SerializationString` uses growth-doubling `resize_exact` + a final shrink-realloc; and `ColumnString` needs **two** arrays (chars+offsets), so "one allocation per column" is structurally impossible. Fixed-width: one exact reserve, but that path is a **copy**, not zero-copy. |
| B6 | `convertToFullColumnIfAdopted` a **no-op** over a **zero-copy, CH-heap-owned** column | **INFEASIBLE-AS-STATED (without core fork)** | The no-op branch fires only when `adoption_ == nullptr`, but then `PODArray::dealloc()` calls `TAllocator::free` on a buffer it didn't allocate. Needs a brand-new "owned-external-with-custom-deleter" `PODArray` mode. Also contradicts B3 (page-flipped foreign pages are not CH-heap). |
| B7 | Buffers dropped when the chunk is consumed; bounded in-flight memory | **ACHIEVABLE** | Already true today: `RetainToken` is a `shared_ptr<void>`; last drop frees. |
| B8 | End-to-end **≥ today and better** | **AT-RISK / "better" INFEASIBLE on loopback** | The removable cost (recv copy) cannot be removed on loopback/this NIC; realistic best case is *approach* SHM-adopt, not *beat* today's TCP. "Better" is only plausible on a zcrx-capable NIC the host does not have. |
| T | The "Native ⊕ zero-copy for all types" tension; resolutions (i)/(ii)/(iii) | **NOT RESOLVED by any candidate** | (i) keeps Native, sacrifices zero-copy for String; (ii) keeps zero-copy, sacrifices Native for String (it's bespoke-in-Native-clothing); (iii) stages = honest partial. They *trade* which intention dies; none satisfies both. |

Legend: ACHIEVABLE / ACHIEVABLE-WITH-CAVEATS / AT-RISK / INFEASIBLE-AS-STATED.

---

## 2. Correctness feasibility (Q1–Q3) — detailed, with code citations

### Q1. The Native-vs-zero-copy contradiction, per type

**Fixed-width (UInt/Int/Float/Date/DateTime/Date32/Decimal/DateTime64).**
On a little-endian host (this host is aarch64 LE) the Native on-wire bytes of a fixed-width column are a
contiguous LE array that **is** the in-memory `PODArray` representation. That is exactly what today's
adopt path already relies on:

```111:114:/home/ubuntu/ClickHouse/src/Storages/SharedMemorySource/Adoption/AdoptionLayer.cpp
    auto * data_ptr = reinterpret_cast<T *>(
        const_cast<char *>(data_region_base) + desc.value_offset);
    return ColumnVector<T>::createAdopted(data_ptr, desc.value_count, retain_token, charge_token);
```

So for fixed-width, **Native and zero-copy adoption are jointly achievable in principle** — *provided you
do not use `NativeReader`*. `NativeReader` always allocates a fresh column and copies:

```222:233:/home/ubuntu/ClickHouse/src/Formats/NativeReader.cpp
            serialization = column.type->getSerialization(*info);
            auto new_column = column.type->createColumn(*serialization);
            new_column->reserve(rows);
            read_column = std::move(new_column);
```

```227:234:/home/ubuntu/ClickHouse/src/DataTypes/Serializations/SerializationNumber.cpp
    typename ColumnVector<T>::Container & x = typeid_cast<ColumnVector<T> &>(column).getData();
    const size_t initial_size = x.size();
    x.resize(initial_size + limit);
    const size_t size = istr.readBig(reinterpret_cast<char*>(&x[initial_size]), sizeof(typename ColumnVector<T>::ValueType) * limit);
```

`readBig` is a `memcpy` from the `ReadBuffer` (which would wrap the recv buffer) into the column. So
"Native via `NativeReader`" for fixed-width is **one exact-size allocation + one deserialize copy**, *not*
zero-copy. To get zero-copy for fixed-width you must bypass `NativeReader` and alias the recv bytes
(the existing `adopt()` mechanism) — which requires the fixed-width payload to be laid out exactly as the
column expects and addressable in place. Native *as a packed stream* (column name + type string + varints
+ bytes, no per-column alignment/padding) does not give you that alignment for free; you would have to
add framing constraints, at which point it is "Native-framed", not stock Native.

**Verified.** Confidence: high (read the code paths end to end).

**Variable-length (`String`, later Array/Map/Nullable).**
Native encodes `String` as per-row `varint(len) + bytes`; the in-memory `ColumnString` is a contiguous
`chars` `PaddedPODArray<UInt8>` **plus** an `offsets` `PaddedPODArray<UInt64>`. Reconstructing the
in-memory column from the Native stream **requires a parse pass that writes both arrays** — i.e. a copy.
The bulk deserializer proves it:

```162:214:/home/ubuntu/ClickHouse/src/DataTypes/Serializations/SerializationString.cpp
    size_t offset = data.size();
    /// Avoiding calling resize in a loop improves the performance.
    data.resize(std::max(data.capacity(), static_cast<size_t>(4096)));

    for (size_t i = 0; i < limit; ++i)
    {
        ...
        UInt64 size = 0;
        readVarUInt(size, istr);
        ...
        offset += size;
        if (unlikely(offset > data.size()))
            data.resize_exact(roundUpToPowerOfTwoOrZero(std::max(offset, data.size() * 2)));
        ...
            istr.readStrict(reinterpret_cast<char*>(&data[offset - size]), size);
        offsets.push_back(offset);
    }
    data.resize_exact(offset);
```

There is no way to "alias" the Native String bytes as a `ColumnString`: the on-wire bytes are
length-prefixed and interleaved, whereas `ColumnString` needs (1) a *separate* monotonic `offsets`
array that does not exist on the wire, and (2) a contiguous `chars` array *without* the inline varints.
**`String` Native ⇒ parse pass ⇒ copy ⇒ two arrays. Always.** This is fundamental, not an implementation
artifact.

**Therefore the two intentions collide exactly as the prompt fears, and worse than it admits:**
- **Use Native** ⊕ **zero-copy** is **jointly achievable only for fixed-width**, and only by *not* using
  `NativeReader` (custom alias reader).
- For `String` it is **jointly impossible**.

**Do the three candidate resolutions resolve it? No — they relabel the sacrifice:**
- **(i) Native + single exact-size deserialize copy.** Honest, but (a) it is *not* the minimum for
  fixed-width (adopt-in-place beats it by the whole copy), and (b) "single exact-size allocation, no
  realloc" is *false* for `String` (see Q2). It keeps intention 1, abandons intention 2 for String.
- **(ii) "Native-compatible adoptable" layout** that ships `chars`+`offsets` for String "under a Native
  extension/variant". This is the bespoke SHM layout wearing a Native handshake. It keeps intention 2,
  but the wire is **no longer Native for String** — a third-party "Native" producer/consumer would not
  interoperate with it. It therefore **defeats intention 1 (retire the bespoke format)** precisely where
  the bespoke format exists. It does not resolve the tension; it renames the bespoke frame.
- **(iii) Stage** (Native+adopt fixed-width; minimum-copy Native String). The only intellectually honest
  option, but it explicitly *concedes* intention 2 is unmet for String. That is a plan, not a resolution.

**Conclusion (Q1):** the prompt's framing ("resolve the tension with evidence") presumes a resolution
exists. For `String` it does not. The agent should be told to *pick which intention loses for variable-
length types*, not to "resolve" a contradiction.

### Q2. "One allocation per column of exact size, zero realloc" — achievable via `NativeReader`?

- **Fixed-width: partially.** `NativeReader` does `reserve(rows)` then a bulk read; for numbers that is
  one allocation sized to `rows*sizeof(T)` (+pad) and the subsequent `x.resize(initial_size+limit)` stays
  within reserved capacity → no realloc. **But it is a copy, and it is *not* "into the recv buffer" —
  it's into a fresh column buffer.** So the goal as worded ("land each column's bytes directly into its
  column buffer … no userspace copy") is contradicted by the very path that gives the exact-size
  allocation. You can have exact-size-one-alloc **or** no-userspace-copy for fixed-width, not both via
  `NativeReader`.
- **`String`: no.** Two independent reasons:
  1. **Two arrays.** `ColumnString` is `chars` + `offsets` (two `PaddedPODArray`s). "Exactly one
     allocation per column" is impossible; the floor is two. The prompt half-acknowledges this ("you need
     2 arrays") yet the success criterion in Branch B item 5 and the DoD still say "one allocation per
     column … allocations-per-block == #columns". For any block containing a String column that assertion
     is **false by construction**.
  2. **Size not known up front + growth-doubling reallocs.** chars total length is not in the Native
     stream as a single number; it is the sum of per-row varints, known only after parsing. The bulk
     impl therefore grows with `resize_exact(roundUpToPowerOfTwoOrZero(...))` (line 184 above) and then
     does a final `resize_exact(offset)` (line 214) — a shrink that itself reallocs/copies. The
     `avg_value_size_hint` reservation (`SerializationString.cpp` 238–266) is a *heuristic*, never exact,
     and "never reserve for too big size" caps it at 256 MiB. "Zero realloc events" is therefore
     **unattainable** for String through this path unless you ship the chars total as **extra metadata**
     (then the wire is no longer stock Native — back to the Q1 (ii) problem) **or** do a two-pass parse
     (still two allocations and a full copy).

**Conclusion (Q2):** the literal goal must be reworded. Defensible restatement: *"fixed-width columns:
one allocation of exact size, no realloc; String columns: two allocations (chars+offsets), chars sized
from shipped metadata to avoid realloc; all variable-length columns incur exactly one parse-copy."*

**Verified.** Confidence: high.

### Q3. No-op `convertToFullColumnIfAdopted` over a zero-copy column

The no-op vs copy decision is keyed on the per-column `adoption_` holder, **not** on the PODArray flag:

```150:150:/home/ubuntu/ClickHouse/src/Columns/IColumn.h
    [[nodiscard]] virtual Ptr convertToFullColumnIfAdopted() const { return getPtr(); }
```
```156:160:/home/ubuntu/ClickHouse/src/Columns/ColumnString.h
    ColumnPtr convertToFullColumnIfAdopted() const override
    {
        if (adoption_)
            return cloneResized(size());
        return this->getPtr();
    }
```
(`ColumnVector` is identical: `ColumnVector.h` 291–295.)

`adoption_` is a `unique_ptr<AdoptionHolder>` that owns the two shared handles
(`AdoptionHolder.h` 20–29). It is non-null **iff** the column was built via `createAdopted`, which is
**iff** the underlying `PODArray` is in adopted mode. And adopted mode is what makes the memory release
correct:

```214:229:/home/ubuntu/ClickHouse/src/Common/PODArray.h
    void dealloc()
    {
        if (isAdopted())
        {
            /// Producer owns this memory; the wrapping column's retain_token RAII handles
            /// release. ...
            return;
        }
        if (c_start == null)
            return;
        unprotect();
        TAllocator::free(c_start - pad_left, allocated_bytes());
    }
```

There are exactly **two** PODArray ownership modes today:
- **adopted** → `dealloc()` is a no-op; the bytes are freed externally by the `RetainToken` deleter
  (`RetainToken.h` 32–45; `TcpStreamSource.cpp` 326–335). `convertToFullColumnIfAdopted` **copies**.
- **normal** → `dealloc()` calls `TAllocator::free(c_start - pad_left, …)`. The buffer **must** have
  been produced by ClickHouse's `Allocator` with `pad_left`/`pad_right`. `convertToFullColumnIfAdopted`
  is a **no-op**.

The prompt wants a *third* mode: a column that **owns** a CH-heap buffer (so the convert is a no-op) but
whose buffer was delivered by the transport, not allocated by `Allocator`, and that frees with a custom
deleter (the recv buffer is `aligned_alloc`'d in `TcpStreamSource.cpp` 289 and freed with `::free` via
the RetainToken). That mode **does not exist**. To get it you must either:

- **(a)** put the recv buffer into normal mode — impossible, because `dealloc()` would call
  `TAllocator::free` on an `aligned_alloc`/page-flipped pointer that was never offset by `pad_left` and
  never came from `Allocator` → heap corruption; **and** `ColumnString` needs the buffer carved into two
  `PaddedPODArray`s with the right pad regions, which the recv buffer is not; or
- **(b)** add a real "owned-external + custom-deleter" path to `PODArrayBase` (a deleter slot, a third
  state in `c_end_of_storage_tagged`, a `dealloc()` arm that invokes it) **and** a flag so
  `convertToFullColumnIfAdopted` returns `getPtr()` for it. `PODArrayBase` is the single most
  layout/perf-sensitive structure in ClickHouse (`ColumnVector<T>` is the most-used column; the header
  even bit-packs the adopted flag to avoid growing the struct — `PODArray.h` 143–147). Adding a
  per-array deleter pointer bloats *every* PODArray instance unless hidden behind the existing
  `adoption_` holder.

The **least invasive feasible** approach is *not* what the prompt literally asks: keep the column in
adopted mode (PODArray `dealloc` no-op + RetainToken frees), and add a boolean to `AdoptionHolder`
("owned_full") so `convertToFullColumnIfAdopted` returns `getPtr()` when the bytes live in
ClickHouse-controlled heap. That is a small, local change — but the column is **still aliasing** a shared
buffer; calling it "a first-class owned column" is a wording choice, and crucially it does **not** make
the column independently mutable (mutators still throw via `assertOwnedForMutation`). So "no-op convert"
is achievable-with-caveats *only by redefining what the no-op means*; the literal "owned, full,
non-adopted, zero-copy column" is not achievable without forking core column/PODArray code.

**The deeper problem:** B6 also **contradicts B3**. If B3 succeeds (page-flipped / DMA'd recv), the bytes
are kernel/`mmap` pages or NIC-DMA `net_iov`s — *foreign, page-granular, must be returned to the kernel
refill ring or `munmap`'d*. Such a column is the **most** adopted thing possible; it can never be a
CH-heap-owned no-op. The only way B6's no-op is literally true is if the bytes were copied into CH heap —
which is the opposite of B3. **You cannot have both on the same column.**

**Verified** (code) + **inference** (the contradiction is a logical consequence of the two mechanisms).
Confidence: high.

---

## 3. Performance feasibility (Q4–Q7) — detailed, with kernel-fact citations

**Baseline (re-confirmed from `dev/hotcold/phase1/REPORT.md` §5–7):**
- in-query loopback recv **6.4–6.9 GB/s**; isolated gtest microbench **7.90 GB/s, 0.127 ns/byte**
  (`gtest_tcp_stream_source.cpp::LoopbackThroughputMicrobench`).
- SHM-copy in-query **~21 GB/s**, cold microbench **29.7 GB/s** (single userspace memcpy).
- End-to-end W=8: ClickBench **+0.6%** median vs SHM-adopt; TPC-H **+7.5%**; ClickBench Q24 (`SELECT *`,
  ~8 GB) **+26.6%**.
- **Mechanism (decisive for everything below):** the TCP cost is the kernel recv copy
  `tcp_recvmsg → skb_copy_datagram_iter → __arch_copy_to_user` (6.63% of the consumer profile), with
  **+4.1% cache-misses and context-switches FLAT** (REPORT §7). The cost is **bandwidth (a copy)**, not
  syscalls.

### Q4. Zero-copy SEND on loopback (`MSG_ZEROCOPY` / `SEND_ZC`)

Today the send copy is `copy_from_user` inside blocking `send(MSG_NOSIGNAL)`:

```472:474:/home/ubuntu/pg_clickhouse/src/shm_producer.c
        CHECK_FOR_INTERRUPTS();
        w = send(p->tcp_conn_fd, ptr, n, MSG_NOSIGNAL);
        if (w > 0) { ptr += w; n -= (size_t) w; continue; }
```

**Kernel fact (verified, kernel.org `networking/msg_zerocopy`):**
> **Loopback.** For TCP and UDP: … all packets generated with `MSG_ZEROCOPY` that are **looped to a
> local socket will incur a deferred copy.** This includes looping onto packet sockets (e.g., tcpdump)
> and tun devices.

and
> As implemented, with page pinning, it replaces per byte copy cost with **page accounting and completion
> notification overhead**. As a result, `MSG_ZEROCOPY` is generally only effective at writes over around
> 10 KB.

`IORING_OP_SEND_ZC` uses the same net-stack zerocopy ubuf/skb-frag machinery and is subject to the same
loopback orphaning (`skb_orphan_frags_rx`), so it inherits the same deferred copy on `lo`.

**Conclusion (Q4):** on loopback, send "zero-copy" **does not eliminate the copy — it relocates it to a
deferred path and adds notification cost** (the doc explicitly notes deferred copies can be *more*
expensive when data has gone cold). It is a **pessimization** at these block sizes on loopback. It only
pays off on a real NIC with scatter-gather. **Beneficial+achievable: only off-host, not on `lo`.**

**Adversarial trap for the agent:** the prompt's proof instrument is "absence of `copy_from_user` in
perf." On loopback `MSG_ZEROCOPY` genuinely removes `copy_from_user` *from the send syscall* while the
deferred copy happens elsewhere (`skb_copy_ubufs`/orphan path). So the agent could show "no
`copy_from_user` on send" and declare victory **while the bytes were still copied**. The instrument is
necessary but **not sufficient**; the prompt's own evidence standard would bless a false win here.

**Verified.** Confidence: high.

### Q5. Zero-copy RECEIVE on loopback (`TCP_ZEROCOPY_RECEIVE` / io_uring zcrx)

This is the cost that actually matters (Q4 above is not the hotspot; this is). It is also the hardest.

**`TCP_ZEROCOPY_RECEIVE` (verified, LWN 752188 / 754681, kernel selftest `tcp_mmap.c`):**
- mmap page-flipping at **page granularity**: "inbound network data must be **both page-aligned and
  page-sized**" or it cannot be mapped; partial/unaligned tail returns `recv_skip_hint` and you fall back
  to `recv()` (a copy).
- alignment "requires cooperation from the network interface; in particular … a network interface that
  is **capable of splitting the packet headers into a different buffer**."

**io_uring zcrx / `IORING_OP_RECV_ZC` (verified, kernel.org `networking/iou-zcrx`, merged Linux 6.15):**
- "Several **NIC HW features are required**": **header/data split** (`ethtool -G eth0 tcp-data-split on`),
  **flow steering**, and **dedicated HW Rx queues** (`ethtool -L eth0 combined 2`), registered via an
  `ifq` bound to `if_nametoindex("eth0")` + a specific `if_rxq`. Requires `CAP_NET_ADMIN` at
  registration. "no data is actually read out of the socket, it has already been copied by the **netdev
  into userspace memory via DMA**."

**Host reality (verified on this machine):**
- `ethtool -g lo` → **"Operation not supported"**: loopback has no ring/queue model at all; it cannot do
  header/data split or expose a HW Rx queue. **zcrx and `TCP_ZEROCOPY_RECEIVE` are impossible on `lo`.**
- The real NIC `ens34` is **ENA** (`driver: ena`); `ethtool -g ens34` reports **`TCP data split: n/a`**.
  ENA does **not** support header/data split on this host. So even moving off loopback to this machine's
  NIC, zero-copy receive **still cannot be set up.**

**Conclusion (Q5):** "no `copy_to_user` on recv" is **not achievable on loopback**, and not achievable on
this host's real NIC either. The honest answer is exactly the one the prompt tries to pre-empt: *not on
loopback — only on a zcrx/header-split-capable NIC, which this host does not have.* The prompt's
instruction to "still implement the mechanism and prove its kernel path" cannot be satisfied on `lo`
(you cannot even register the ifq), so the proof it demands ("the very frames Phase 1 §7 showed must
vanish") is **unprovable here**.

**Verified.** Confidence: high (kernel docs + direct `ethtool` on the host).

### Q6. io_uring in both processes

- **ClickHouse already links io_uring — but as a *file* reader only.** `IOUringReader::submitToRing` does
  `io_uring_prep_read(sqe, fd, …)` over a `LocalFileDescriptor`:

```171:174:/home/ubuntu/ClickHouse/src/Disks/IO/IOUringReader.cpp
        int fd = assert_cast<const LocalFileDescriptor &>(*request.descriptor).fd;
        ...
        io_uring_prep_read(sqe, fd, request.buf, static_cast<unsigned>(request.size - enqueued.bytes_read), request.offset + enqueued.bytes_read);
```
  There is **no socket/recv io_uring path** to reuse; a network io_uring consumer is net-new code.
- **PostgreSQL 18's io_uring is read-only file AIO** (verified: PG18 `io_method=io_uring`, one ring per
  backend created in postmaster, "AIO is currently only for read operations and not for write and WAL").
  The producer's socket *send* would be a separate, hand-rolled ring in the bgworker — feasible (it's a
  normal process) but unrelated to PG's AIO and not "free".
- **Blockers / caveats that make this low-value here:**
  - The Phase-1 profile shows **context-switches are flat** and the cost is the copy, so io_uring's main
    win (fewer syscalls/cs) targets a non-bottleneck. Expect ~0 throughput gain on loopback.
  - The real benefit io_uring *could* unlock is an **async `TcpStreamSource`** that overlaps recv with
    processing, fixing the Phase-1 single-stream `max_threads=1` limitation. But `StorageShm::read`
    returns a **single blocking source per stream**:

```88:92:/home/ubuntu/ClickHouse/src/Storages/SharedMemorySource/Source/StorageShm.cpp
    if (transport_mode == ShmTransportMode::Tcp)
        return Pipe(std::make_shared<TcpStreamSource>(
            std::move(shared_header), tcp_host, tcp_port,
            std::move(full_column_types), std::move(full_column_names),
            std::move(requested_names), stall_ms));
```
    and `TcpStreamSource` is a blocking `ISource` (`generate()` → `recvBlock()` → blocking `recvAll`,
    `TcpStreamSource.cpp` 93–139, 370–379). The harness already hides this by running W (or W/2)
    producers per relation (REPORT §9), so the **async rewrite mostly helps a regime the benchmark avoids**.
  - **Deadlock-safety invariant** (REPORT §9): liveness rests on
    `max_threads = Σ producers ≥ #blocking TCP sources`. An async source changes that invariant; doing it
    wrong reintroduces the deadlock the Phase-1 comment guards against. Achievable, but it is real
    surgery, not a flag.

**Conclusion (Q6):** io_uring is **mechanically achievable** in both processes, but is **low-value on
loopback** (the bottleneck is the copy, not syscalls) and its one genuine upside (async overlap) targets
a non-benchmarked regime and perturbs the deadlock invariant. Confidence: high (file reader is
file-only; flat-cs from REPORT §7; blocking-source from code).

### Q7. "≥ today and better"

- Today's TCP overhead over SHM-adopt is **dominated by the recv copy** (Q5/REPORT §7). The *only* way to
  beat today's TCP materially is to remove that copy. On loopback (and this host's NIC) you **cannot**
  (Q5).
- Branch B's other levers don't move the needle on loopback: send zero-copy is a pessimization (Q4);
  io_uring removes non-bottleneck syscalls (Q6); the "direct-to-ColumnPtr exact-size" rework, *if it
  removed the userspace second copy*, would help — but today's path is **already** zero-copy from recv
  buffer to columns (it adopts; there is no second userspace copy for the bespoke frame). Native, by
  contrast, **adds** a parse copy for String (Q1/Q2). So switching to Native is, for String-heavy cells,
  a *regression* relative to the bespoke adopt-in-place path, not an improvement.
- **Realistic best case on loopback:** *approach* SHM-adopt asymptotically by shaving constant factors
  (fused serialize, better batching, async overlap in single-stream) — i.e. close the +0.6%/+7.5% gap,
  not turn it negative. "Better than today's TCP" is plausible only for the single-stream regime (via
  async overlap) the harness doesn't run; in the W=8 multi-stream regime the transfer is already
  overlapped and there is little left to win without removing the copy.
- **The real-NIC future the prompt invokes (~3 GB/s):** there, zero-copy recv *would* matter — but
  (a) it needs a header/data-split NIC this host lacks, and (b) at ~3 GB/s the network, not the copy, is
  the ceiling, so the win is CPU/cache, not wall-clock throughput. That is a legitimate research goal,
  but it is **unmeasurable on this machine** and must be stated as such.

**Conclusion (Q7):** "≥ today" is achievable (don't regress); **"better than today on loopback" is
infeasible** because the removable cost can't be removed here. Confidence: high.

---

## 4. Contradictions / mis-stated or unprovable success criteria

1. **B3 + the copy-budget table assert a loopback impossibility as a deliverable.** The table row
   "kernel → userspace (recv) | **0 (eliminated)** | TCP_ZEROCOPY_RECEIVE / io_uring RECV_ZC | no
   `copy_to_user`/`skb_copy_datagram_iter` in perf" cannot be made true on `lo` or on this host's ENA NIC
   (Q5). The prose hedges ("residual copy on loopback = measured finding") but the **Definition of Done**
   states "**zero-copy kernel on BOTH sides proven** — no `copy_from_user` on send AND no
   `copy_to_user`/`skb_copy_datagram_iter` on recv (or, where loopback forces a residual copy, that is a
   measured finding…)". The "(or …)" makes the DoD self-contradictory: it simultaneously requires the
   proof and excuses its absence. As written, an agent can satisfy it only by declaring the residual
   copy a "finding" — which means **the headline B3 guarantee is, in practice, known-infeasible up front.**

2. **B6 contradicts B3** (proved in Q3): a page-flipped/DMA'd recv column (B3) is maximally foreign and
   can never be a CH-heap-owned no-op (B6). The two HARD requirements cannot both hold on one column.

3. **B5 ("one allocation per column") contradicts `ColumnString`'s two-array reality** (Q2). For any
   block with a String column, "allocations-per-block == #columns" is false by construction.

4. **B2's proof instrument is unsound on loopback** (Q4): "absence of `copy_from_user`" can be true while
   the data is copied via the deferred path. The prompt's evidence standard (§"BANNED: claiming a copy
   was eliminated without a profile/counter proving it") is satisfiable by a *misleading* profile here —
   the standard needs a stronger instrument (e.g. count `skb_copy_ubufs`/zerocopy "copied" notifications
   via `SO_EE_CODE_ZEROCOPY_COPIED`) or it will certify a non-elimination.

5. **A4 "parity is the floor for every cell"** vs **the prompt's own Branch-A pre-registration** that
   predicts a String-heavy regression. These are inconsistent: a predicted, measured regression on
   `SELECT *`/String cells cannot also be "below the parity floor". Reword A4 to "parity for fixed-width;
   bounded, measured regression allowed for String-heavy cells."

6. **"Native ⊕ zero-copy resolved with evidence"** presumes a resolution that does not exist for String
   (Q1). The task should ask the agent to *choose and document the per-type sacrifice*, not to resolve a
   contradiction.

7. **"io_uring on both sides … reduce per-block syscall count … prove via a context-switch counter
   delta"** assumes syscalls are a cost. Phase-1 measured context-switches **flat** (REPORT §7); the
   predicted delta may be ~0, which the prompt would read as a failure to deliver rather than as evidence
   that the lever is irrelevant on loopback.

---

## 5. Concrete recommendations (specific enough to edit the prompt)

1. **Reword the per-type intentions (Q1).** Replace "resolve the tension" with an explicit per-type
   contract:
   - *Fixed-width:* Native on the wire **and** zero-copy adopt-in-place on recv, via a **custom alias
     reader** (not `NativeReader`). State that `NativeReader` is the *correctness oracle*, not the fast
     path.
   - *String / variable-length:* Native on the wire **with exactly one parse-copy and two allocations
     (chars+offsets)** on recv. Drop the zero-copy claim for these types. (This is resolution (i),
     stated honestly.)
   - Explicitly **forbid** calling resolution (ii) "Native" — it is a bespoke layout and would block
     intention 1 (retire-ability) for String.

2. **Fix B5 wording.** "For fixed-width columns: one allocation of exact size, zero realloc. For String
   columns: two allocations (chars, offsets); to avoid chars realloc, ship the chars-total byte length as
   block/column metadata so the consumer reserves exactly once." Drop "allocations-per-block ==
   #columns".

3. **Specify the column-ownership mechanism for B6 precisely (Q3).** Choose one and write it down:
   - *Minimal:* add an `owned_full` boolean to `AdoptionHolder` and make
     `convertToFullColumnIfAdopted` return `getPtr()` when set (column still aliases a shared CH-heap
     buffer freed by the RetainToken; mutators still throw). State that "no-op convert" means "no
     re-allocation/copy", **not** "independently mutable owned column".
   - *Invasive (only if truly required):* add a third `PODArrayBase` state with a custom deleter slot;
     budget for the perf/layout review of touching the hottest struct in the codebase.
   - Either way, **delete the implication that the column is a stock non-adopted owned column** — that is
     not what either mechanism produces.

4. **Relax/replace the loopback zero-copy-receive requirement (Q5).** Make B3:
   - On loopback: **do not require** `TCP_ZEROCOPY_RECEIVE`/zcrx; require instead that the recv path is
     *single-copy* (one kernel `skb_copy_datagram_iter` straight into the destination column-staging
     buffer, no extra userspace memcpy) and that the destination is reused/bounded.
   - Gate the true zero-copy-receive deliverable behind **"a header/data-split-capable NIC"** and mark it
     **out of scope on this host** (ENA: `TCP data split: n/a`; `lo`: unsupported). If a zcrx demo is
     wanted, it needs different hardware; say so.

5. **Demote B2 (send zero-copy) on loopback to a measured null/negative result (Q4).** Pre-register that
   `MSG_ZEROCOPY`/`SEND_ZC` is expected to *lose* on loopback (deferred copy + notification cost) and
   require the **`SO_EE_CODE_ZEROCOPY_COPIED`** signal (not merely "absence of `copy_from_user`") as the
   instrument that proves whether the copy was actually elided.

6. **Reframe B8 ("≥ today and better").** Floor = "no regression vs today's bespoke TCP in the W=8
   multi-stream regime, within the committed noise band." Upside = "close the +0.6%/+7.5% gap toward
   SHM-adopt; any *beat* of today's TCP must come from the async-overlap of the single-stream regime, and
   be reported separately." Drop the unqualified "better".

7. **Reframe B4 (io_uring).** Keep it as an **async-overlap** experiment (fixing single-stream
   `max_threads=1`), not a syscall-reduction win; pre-register that context-switch deltas may be ~0 and
   that the deadlock invariant `max_threads ≥ #blocking sources` must be re-derived for the async source.

8. **Fix the DoD self-contradiction (§4.1).** Make the two-sided zero-copy proof **conditional on capable
   hardware** and the loopback path explicitly a single-copy target, so "done" is reachable without
   either faking a profile or relying on the escape clause.

---

## 6. Confidence ledger (verified vs inferred)

| Claim | Basis | Confidence |
|---|---|---|
| Fixed-width Native bytes == in-memory PODArray (LE) → adoptable in principle | code: `AdoptionLayer.cpp` 111–114; host is aarch64 LE | Verified |
| `NativeReader` always allocates+copies (no aliasing) | code: `NativeReader.cpp` 222–233, `SerializationNumber.cpp` 227–234 | Verified |
| `String` Native ⇒ parse pass + 2 arrays + growth-doubling reallocs + final shrink-realloc | code: `SerializationString.cpp` 162–214, 238–266 | Verified |
| No-op convert keyed on `adoption_`; only two PODArray modes; normal `dealloc` uses `TAllocator::free` | code: `IColumn.h` 150, `ColumnString.h` 156–160, `PODArray.h` 214–229 | Verified |
| A CH-heap-owned, no-op-convert, zero-copy column needs a new PODArray mode (or a redefinition of "no-op") | inference from the above two | Verified (logic) |
| B6 contradicts B3 (foreign page-flipped pages can't be CH-heap no-op) | inference from zcrx/TCP_ZEROCOPY semantics + column model | Verified (logic) |
| `MSG_ZEROCOPY`/`SEND_ZC` forces a deferred copy on loopback | kernel.org `networking/msg_zerocopy` §Loopback | Verified |
| `TCP_ZEROCOPY_RECEIVE` needs page-aligned payload via NIC header/data split | LWN 752188/754681, selftest `tcp_mmap.c` | Verified |
| io_uring zcrx needs HW Rx queue + header/data split + flow steering; merged 6.15 | kernel.org `networking/iou-zcrx`; Phoronix/LWN | Verified |
| `lo` cannot do ring/header-split; ENA reports `TCP data split: n/a` | `ethtool -g lo` (Operation not supported), `ethtool -i/-g ens34` on host | Verified |
| Host kernel `7.0.0-1006-aws` (has zcrx code, lacks capable NIC) | `uname -r` | Verified |
| CH io_uring is a file reader only; PG18 io_uring is read-only file AIO | code: `IOUringReader.cpp` 171–174; PG18 docs | Verified |
| Cost is the recv copy (bandwidth), context-switches flat | `phase1/REPORT.md` §7 (prior measurement; I did not re-run it) | Inferred-from-prior-evidence |
| Baseline numbers (6.4–6.9 / 7.9 GB/s; +0.6%/+7.5%/+26.6%) | `phase1/REPORT.md` §5–7 (not re-measured in this review) | Inferred-from-prior-evidence |
| Async `TcpStreamSource` perturbs the deadlock invariant | code: `StorageShm.cpp` 88–92 + REPORT §9 | Verified (mechanism), Inferred (risk magnitude) |

**Not independently re-measured in this review:** all wall-clock/throughput numbers (taken from
`phase1/REPORT.md`). Everything attributed to "code" or "kernel doc/host `ethtool`" was checked directly.
