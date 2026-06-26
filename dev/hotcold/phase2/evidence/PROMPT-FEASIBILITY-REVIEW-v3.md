# PROMPT.md feasibility review v3 — adversarial round 3

## Verdict

**Acceptable with one medium consistency fix recommended before unattended execution.** The latest `PROMPT.md` materially fixes every round-2 blocker/recommendation called out in the user brief: it targets Apache Arrow, sequences Branches 0 → A → B, bans the blanket `owned_full` no-op, adds the `ColumnNullable` recurse requirement, validates Arrow offset normalization, scopes producer Arrow IPC work, records the Date/DateTime raw-vs-semantic tradeoff, and correctly treats loopback recv as single-copy rather than zero-copy.

I did **not** find a remaining hard blocker in the latest prompt. The main residual issue is Branch A's performance wording: it simultaneously says "parity with today's bespoke TCP is the floor" and says a String-heavy regression is expected if Branch A uses the copying decode path (`PROMPT.md:295-305`, DoD at `PROMPT.md:660-661`). That can mislead an unattended implementer about whether Branch A is allowed to go green before Branch B recovers the String path.

## Round-2 fixes status table

| Round-2 item | Status | Evidence |
|---|---:|---|
| Drop blanket no-op `convertToFullColumnIfAdopted` / `owned_full` | **FIXED** | Latest prompt says the method "still materializes a mutable owned column where callers mutate" (`PROMPT.md:171-175`) and explicitly bans a blanket no-op (`PROMPT.md:365-381`, DoD `PROMPT.md:666-668`). This matches real caller needs: `Squashing.cpp:337-347` converts before mutating; `HashJoin.cpp:118-126` materializes build-side columns. |
| Add `ColumnNullable::convertToFullColumnIfAdopted` recurse | **FIXED** | Prompt adds it as a mandatory caveat (`PROMPT.md:211-217`), adversarial-review check (`PROMPT.md:568-570`), and DoD item (`PROMPT.md:666-668`). This is real: `ColumnNullable.h:28-54` has no override, while default `IColumn.h:142-150` returns `getPtr()` without recursing. |
| Validate Arrow offset normalization | **FIXED** | Prompt now says Arrow only recommends leading zero and adopter must validate `array.offset()==0 && arrow_offsets[0]==0` (`PROMPT.md:190-198`), repeated in DoD (`PROMPT.md:663-665`). |
| Use `LargeBinary` for CH `String` | **FIXED in latest prompt** | Prompt uses `LargeBinary` as the `String` mapping (`PROMPT.md:119-121`, `PROMPT.md:167-169`, `PROMPT.md:267-268`) and says `LargeUtf8` only for known-UTF-8 columns (`PROMPT.md:190-191`). Stock reader supports `LARGE_STRING`/`LARGE_BINARY` via `arrow::LargeBinaryArray` (`ArrowColumnToCHColumn.cpp:1819-1825`). |
| Drop loopback "close gap to SHM-adopt" over-claim | **FIXED** | Prompt now predicts Branch B parity with today's TCP and says the SHM-adopt gap is not closable on loopback (`PROMPT.md:416-420`). |
| Scope producer Arrow IPC dependency/decision | **FIXED** | Prompt adds a producer decision for nanoarrow IPC vs hand-rolled flatbuffer metadata and warns body-only framing forfeits stock-reader/cross-engine claims (`PROMPT.md:271-276`). Current producer is still bespoke C framing (`shm_producer.c:463-494`, `shm_producer.c:560-655`); search found no Arrow dependency in `pg_clickhouse/src`. |
| Add Date/DateTime/DateTime64 raw-vs-semantic decision | **FIXED** | Prompt explicitly calls out `Date`/`DateTime` width mismatch and `DateTime64` precision limits, requiring a per-type decision (`PROMPT.md:200-206`, deliverables `PROMPT.md:644-647`). Code confirms CH adopt storage is `UInt16`/`UInt32`/`DateTime64` fixed storage (`AdoptionLayer.cpp:336-345`), while the stock Arrow reader has raw `UINT16`/`UINT32` type-hint handling (`ArrowColumnToCHColumn.cpp:1833-1848`). |
| Note Arrow IPC metadata + body two-recv shape | **FIXED** | Branch B now defines single-copy recv as one kernel copy of the record-batch body and explicitly permits a small metadata read first (`PROMPT.md:345-347`). |

## Remaining findings

### Medium — Branch A parity floor conflicts with its allowed copying decode path

`PROMPT.md:295-298` says Branch A's performance floor is parity with today's bespoke TCP, but the same paragraph says that if Branch A uses the copying decode path, a String-heavy regression versus bespoke is expected and recovered in Branch B. The pre-registration paragraph repeats that prediction (`PROMPT.md:300-305`), while the DoD still requires Branch A `≥ parity with bespoke TCP` (`PROMPT.md:660-661`).

That is not a physics blocker: Branch A could still be implemented with a fast enough copying path, or Branch A could be required to include the custom adopter earlier. But as written, it gives two possible success criteria for the same branch. An unattended implementer may either hide a predicted String regression to satisfy the DoD, or prematurely implement Branch B adoption inside Branch A.

### Low — Evidence-standard wording still over-emphasizes `copy_from_user` absence

The send-side details are fixed in Branch B: loopback success is a measured null/negative result via `SO_EE_CODE_ZEROCOPY_COPIED`, not absence of `copy_from_user` (`PROMPT.md:324-332`, `PROMPT.md:410-413`). However the generic evidence-standard section still says "zero-copy send removes `copy_from_user` cycles" and profiles should show "absence of `copy_from_user`" (`PROMPT.md:462-468`). This is not fatal because the later Branch B instructions are explicit, but it is stale enough to invite a weak proof if read out of context.

### Low — Supporting investigation still has stale shorthand outside latest prompt

The latest prompt fixed `LargeBinary` and offset-normalization wording, but the supporting file still uses broader shorthand: `LargeBinary`/`LargeUtf8` together (`FINDINGS-format-and-sequencing.md:51-57`, `:82-98`) and "first offset 0" in the spec summary/confidence ledger (`FINDINGS-format-and-sequencing.md:42-47`, `:130-131`). The same file also contains the corrected validation language (`FINDINGS-format-and-sequencing.md:57-64`), so this is not a prompt blocker. Still, since `PROMPT.md:199` cites it, an implementer could benefit from an explicit "PROMPT supersedes supporting shorthand" note.

## Internal consistency check

The latest prompt no longer contains the stale hard contradictions named in the request:

- No blanket no-op clone remains; `owned_full` appears only as the banned pattern (`PROMPT.md:365-381`).
- The latest prompt does not target ClickHouse Native; Native appears only as existing-format/baseline context.
- `LargeUtf8` is not the default for CH `String`; `LargeBinary` is.
- Arrow leading offset zero is no longer treated as guaranteed in the latest prompt; validation is mandatory.
- Hard zero-copy recv on this loopback/ENA host is no longer a success criterion; Branch B's loopback criterion is single-copy recv (`PROMPT.md:333-349`, DoD `PROMPT.md:670-673`).
- Branch B's copy-budget table and DoD are aligned: loopback send has a deferred copy, loopback recv has one kernel copy, fixed-width/String adoption has zero userspace data copy, Nullable has a small null_map transform, and mutating callers still materialize (`PROMPT.md:391-399`, `PROMPT.md:662-673`).

The only internal inconsistency I would fix before launch is Branch A's "parity floor" versus "expected copying-decode String regression" wording.

## Concrete recommendations

1. **Make Branch A's performance gate unambiguous.** Choose one:
   - Allow Branch A to go green with a bounded, pre-registered String-heavy regression when using the copying decode path, with Branch B required to recover parity via zero-copy adoption; or
   - Require Branch A itself to include enough custom decode/adoption work to meet parity, and remove the "copying decode regression is expected" allowance.

2. **Qualify the generic `copy_from_user` evidence text.** Keep `copy_from_user` absence as a profile observation, but state there too that loopback send-copy elimination is proven by `SO_EE_CODE_ZEROCOPY_COPIED`/errqueue accounting, not by that profile frame alone.

3. **Optional cleanup: update or annotate the supporting investigation.** Align `FINDINGS-format-and-sequencing.md` with the latest prompt: CH `String` defaults to `LargeBinary`, `LargeUtf8` only for known UTF-8, and offset normalization must be validated.

4. **Optional clarity for raw Date/DateTime.** When the raw-uint Arrow path is chosen, specify how the stock-reader oracle compares CH semantic types: e.g. run it with type hints or compare after restoring `Date`/`DateTime` wrappers. The stock reader has relevant type-hint behavior (`ArrowColumnToCHColumn.cpp:1833-1848`), so this is achievable.

## Confidence ledger

| Claim | Basis | Confidence |
|---|---|---:|
| Round-2 prompt fixes are present | Direct read of `PROMPT.md` line ranges cited above | High |
| Blanket no-op would be wrong; prompt now avoids it | `IColumn.h:142-150`, `ColumnVector.h:289-295`, `ColumnString.h:154-160`, `Squashing.cpp:337-347`, `HashJoin.cpp:118-126`, `gtest_column_vector_adopted.cpp:122-158` | High |
| `ColumnNullable` recurse requirement is real | `ColumnNullable.h:28-54` lacks override; default `IColumn.h:150` does not recurse | High |
| Arrow `LargeBinary` consumer support exists | `ArrowColumnToCHColumn.cpp:1819-1825` | High |
| Producer Arrow IPC work is newly scoped and non-trivial | Current C producer bespoke TCP path in `shm_producer.c:463-655`; no Arrow hits in `pg_clickhouse/src` except unrelated text | High |
| Branch A parity ambiguity is real | `PROMPT.md:295-305` vs DoD `PROMPT.md:660-661` | Medium-high |
| Branch 0/A/B are achievable as scoped | Code inspection + prior reviews for kernel/NIC facts; no new runtime measurements in this review | Medium |
| Kernel loopback/ENA zero-copy facts | Relied on round-1/round-2 reviews and latest prompt; not re-run with `ethtool` in this round | Medium |

