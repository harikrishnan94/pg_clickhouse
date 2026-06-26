# Phase 0 — PMU + profile mechanism (adopt vs copy), ClickBench Q24 (SELECT * 105 cols)

Q24 is the heaviest copy cell (8.18 GB/iter materialised). `perf stat -p <CHPID>` and
`perf record -e cpu-clock -g -p <CHPID>` over N=20 query iterations per mode, uncapped, idle host.
Hardware PMU confirmed available on this Graviton guest (the earlier `<not counted>` was `sleep`
being off-CPU). Raw: `evidence/perf-stat-{adopt,copy}.txt`, `evidence/perf-prof-{adopt,copy}*.txt`.

## PMU counters (whole CH server process, 20 iters, ~19.7 s window each)

| metric             | adopt          | copy           | copy/adopt | reading |
|--------------------|---------------:|---------------:|-----------:|---------|
| cycles             | 37,225,284,067 | 63,861,609,846 | 1.72×      | copy does more work |
| instructions       |121,884,645,315 |150,091,253,978 | 1.23×      | +28.2 B insns = the memcpy |
| cache-references   | 46,178,670,248 | 63,970,893,964 | 1.39×      | more memory traffic |
| **cache-misses**   |  **245,765,268** | **1,550,281,734** | **6.31×** | **bandwidth-bound signature** |
| branch-instructions| 24,848,079,265 | 29,034,393,002 | 1.17×      | scales with insns |
| branch-misses      |     88,622,067 |    104,120,909 | 1.17×      | rate FLAT 0.357%→0.359% |
| IPC                |          3.27  |          2.35  | 0.72×      | copy is memory-STALLED |

**Mechanism (matches pre-registration hypothesis #4 exactly):** copy mode is **bandwidth / cache
bound**, not branch/control-flow bound. Cache-misses jump 6.31× (the consumer now streams the
block payload through cache into owned columns); IPC falls 3.27→2.35 (cycles rise 1.72× while
instructions rise only 1.23× — the extra cycles are memory stalls); the branch-miss *rate* is flat
(0.357%→0.359%) — a streaming `memcpy`, not new control flow. No unexpected new hotspot.

## Profile (`perf report --no-children`, copy/adopt-relevant frames)

**COPY** — the copy is a clear hotspot exactly where predicted:
```
16.25% ColumnString::validateAdoptedOffsets   (runs in BOTH modes; pre-convert string validation)
        └─ convertToFullColumnIfAdopted (inlined)
            --4.72%-- cloneResized (inlined)
            --4.43%-- cloneResized (inlined)
            --2.42%-- cloneResized (inlined)
            --1.04%-- cloneResized (inlined)
            --0.68%-- cloneResized (inlined)
 0.04% ColumnString::convertToFullColumnIfAdopted
```
**ADOPT** — `convertToFullColumnIfAdopted`/`cloneResized` is ABSENT as a hotspot (only a 0.02%
incidental `ColumnString::cloneResized` and a 0.08% kernel `__pi_memcpy_generic` from unrelated
query machinery). The zero-copy adoption path carries no materialisation cost.

**Conclusion:** the `memcpy` (cloneResized inside convertToFullColumnIfAdopted) appears where
predicted in copy mode and vanishes in adopt mode; the zero-copy adopt path's cost is genuinely
absent. Pre-registration mechanism hypotheses #1 (new copy hotspot in copy only) and #4
(bandwidth-bound) are both confirmed by independent instruments (profile + PMU).

(`validateAdoptedOffsets` at 16–28% in both modes is the lazy ColumnString offset content
validation — Q24 has many String columns — and is transport-independent; it is larger as a
*fraction* in adopt only because adopt's total is smaller without the copy.)
