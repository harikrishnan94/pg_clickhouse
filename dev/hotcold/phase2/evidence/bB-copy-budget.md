# Branch B — copy-budget table (measured)

Every byte movement end to end on the `arrow:` zero-copy-adopt path (Branch B), labelled REQUIRED /
ELIMINATED / RESIDUAL, with the mechanism and the instrument that proves it. Evidence = the W=8 sweep
(`evidence/bB-overhead-table.md` / `results/bB-*`), the consumer CPU split (cells.tsv `cons_user_us` /
`cons_sys_us`), the gtests, and verify_offload's leak oracle. Host: loopback / NIC-less (no header/data
split — feasibility review §3), so the kernel recv copy is the irreducible residual (real-NIC north star).

| stage | bytes | copy? | mechanism | how proven |
| --- | --- | --- | --- | --- |
| PG heap → deform → temp buf + Arrow serialize | logical | **1 (required)** | the deform fills cache-resident column buffers; nanoarrow `EncodeSimpleRecordBatch` concatenates those (viewed zero-copy) into the IPC body — one userspace serialize copy, the mirror of bespoke `tcp_serialize_block`'s one scratch copy | producer phase split (SERIALIZE/PUBLISH); B-it1 confirms the per-block copy counter == 1 |
| userspace → kernel (send) | logical | loopback **1 (deferred)** / real-NIC **0** | io_uring `IORING_OP_SEND` today (Branch 0 substrate); `IORING_OP_SEND_ZC` is **B-it3** (pre-registered measured-null) | Branch-0 per-producer counter (io_uring on path); B-it3 will prove the loopback deferred copy via the `SO_EE_CODE_ZEROCOPY_COPIED` errqueue flag (NOT "absence of copy_from_user") |
| wire (loopback) | logical | n/a | — | — |
| kernel → userspace (recv) | logical | loopback **1 (single-copy)** / real-NIC **0** | the Arrow encapsulated **metadata** is recv'd + parsed (NOT a data copy), then the **body** is recv'd ONCE into the to-be-adopted buffer (`tryRecvInto`, no userspace recopy) | single-copy proven: one `allocFrameBuffer` body buffer/message, `recv` straight into it, no second memcpy. The residual kernel recv copy is the measured floor: CB Q24 tcp 1156 − shm-adopt 916 = **+240 ms** (the kernel copy; absent in SHM). NOT closable on this NIC-less host |
| recv buffer → ColumnPtr — **fixed-width** (Int/UInt/Float/Date/DateTime/Decimal32/64) | 0 | **0 (ELIMINATED)** | `ColumnVector/ColumnDecimal::createAdopted(ptr, n, retain, charge)` aliases `buffers[1]` in place — no per-column data copy, no per-column alloc | structural (createAdopted is alias-only); fixed-width adopt vs bespoke at parity (TPC-H Q1/Q6/Q19, CB Q2 ≤2.2%, within band); adopt cons_user ≈ copy ≈ tcp on these cells |
| recv buffer → ColumnPtr — **String** (`LargeBinary`) | 0 | **0 (ELIMINATED)** | `ColumnString::createAdopted(chars=value_data, offsets=&arrow_offsets[1], n, retain, charge)` aliases the values + offsets buffers; `arrow_offsets[0]==0` is CH's `offsets[-1]` sentinel (validated; `array.offset()==0` checked) | **CB Q24 `cons_user` adopt 663 ms vs copy 962 ms = −299 ms** — the copy-decode is measurably eliminated (matches the predicted ~+283 ms); `validateAdoptedOffsets()` passes; verify_offload 137/137 String columns correct |
| recv buffer → ColumnPtr — **Decimal128** | logical | **1 (residual — alignment)** | `FixedSizeBinary(16)` at an 8-byte-padded IPC body offset may not meet `Decimal128`'s 16-byte alignment → `adoptArrowColumnToCH` returns nullptr → per-column copy fallback | the alignment check + copy fallback in `adoptArrowColumnToCH`; absent in the swept datasets (TPC-H = Decimal64, ClickBench = no decimal), so 0 in practice; a producer-64B-alignment follow-up removes it |
| recv buffer → ColumnPtr — **Nullable null_map** | row_count B | **1 small (required, n/a here)** | Arrow validity bitmap → CH 1-byte null_map transform; the nested data column still adopts (`ColumnNullable::convertToFullColumnIfAdopted` recurses, D-HC-0206) | the PG producer emits **non-Nullable**, so this never fires in the offload; the consumer-side capability + the recurse are gtested (`AdoptedConvert.NullableRecursesIntoAdoptedNested`) |
| ColumnPtr lifetime | — | — | one `RetainToken`/block (deleter frees the recv body) shared across the adopted columns; last-drop on chunk consume | verify_offload leak teardown 137/137 (no leaked workers/sockets/fds); in-flight bounded to ~K recv buffers/stream |
| **[residual — NEW, Branch-B finding]** Arrow IPC framing parse | metadata | **parse (not a data copy)** | `arrow::ipc::Message::Open` + `ReadRecordBatch` construct one `arrow::Array` per column per block (flatbuffer walk + object allocs) — work the bespoke descriptor wire avoids | CB Q24 arrow-adopt **+7.4%** vs bespoke-tcp (above band); adopt vs tcp `cons_user` +47 ms / `cons_sys` +118 ms; this is the residual **B-it3** (lean buffer-extraction) targets |

## Reading
The two data-path copies Branch A paid on the consumer (fixed-width memcpy + String chars/offsets rebuild)
are **ELIMINATED** — proven by the −299 ms consumer user CPU on the String-heavy Q24 and by parity on
fixed-width. The **known residuals** (recorded, not buried): (1) the **kernel recv copy** (+240 ms on Q24,
host has no NIC header/data split — review §3, real-NIC north star); (2) **Decimal128** alignment copy
fallback (0 in the swept datasets); (3) the **Nullable null_map** transform (n/a — non-Nullable producer);
and (4) the **NEW** Arrow IPC framing-parse overhead (`ReadRecordBatch`, the +7.4% Q24 residual) — the
honest cost of a standard format, which B-it3 attacks with a lean buffer-extraction. The send-side row is
io_uring `IORING_OP_SEND` today; `IORING_OP_SEND_ZC` (B-it3/4) is a loopback measured-null by design.
