# Phase 0 — direct copy-cost analysis (C2 in-query rate, C3 copy-time vs consumer-CPU delta)

Source: capture_copy_cost.sh (warm, uncapped; ratios are cap-independent). copy_us = ShmCopyTimeMicroseconds,
copied_bytes = ShmCopiedBytesLogical, cons_us = UserTime+SystemTime. Rows with blocks=0 = capture did not
offload (focused script sets no per-table parallel_workers) — those cells are covered by the W=8 sweep.

## tpch

| q | copy_bytes | copy_us | rate GB/s | ns/byte | copy_cons_us | adopt_cons_us | dCons_us | copy_us vs dCons |
|--:|-----------:|--------:|----------:|--------:|-------------:|--------------:|---------:|----------------|
| 1 | 3,119,274,704 | 135,114 | 23.1 | 0.0433 | 2,259,664 | 2,150,088 | +109,576 | 1.23x |
| 6 | 1,559,637,352 | 69,851 | 22.3 | 0.0448 | 555,231 | 482,185 | +73,046 | 0.96x |
| 9 | — | — | — | — | — | — | — | (capture no-offload) |
| 14 | 1,384,891,897 | 75,535 | 18.3 | 0.0545 | 532,127 | 562,105 | -29,978 | n/a(Δ~0) |
| 19 | 3,695,413,881 | 170,182 | 21.7 | 0.0461 | 1,317,797 | 1,141,794 | +176,003 | 0.97x |

## clickbench

| q | copy_bytes | copy_us | rate GB/s | ns/byte | copy_cons_us | adopt_cons_us | dCons_us | copy_us vs dCons |
|--:|-----------:|--------:|----------:|--------:|-------------:|--------------:|---------:|----------------|
| 17 | 241,438,666 | 14,468 | 16.7 | 0.0599 | 729,952 | 831,383 | -101,431 | n/a(Δ~0) |
| 19 | 321,438,666 | 15,518 | 20.7 | 0.0483 | 1,181,258 | 1,182,158 | -900 | n/a(Δ~0) |
| 24 | 8,175,107,721 | 464,884 | 17.6 | 0.0569 | 1,149,519 | 645,676 | +503,843 | 0.92x |
| 29 | — | — | — | — | — | — | — | (capture no-offload) |

