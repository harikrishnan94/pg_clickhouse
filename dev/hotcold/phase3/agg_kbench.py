#!/usr/bin/env python3
"""Aggregate a lean P2 K-bench walls.tsv: per (q,KV) median wall across all rounds/runs, rel% vs K=1.
Usage: agg_kbench.py <results/p2_kbench_TAG/walls.tsv>"""
import sys, statistics as st
from collections import defaultdict

rows = [l.rstrip("\n").split("\t") for l in open(sys.argv[1])][1:]
D = defaultdict(list)  # (q,KV) -> [wall_ms]
for q, kv, rnd, w in rows:
    try: D[(q, int(kv))].append(float(w))
    except ValueError: pass

qs = sorted({q for (q, k) in D})
Ks = sorted({k for (q, k) in D})
print(f"# Lean P2 K-bench ({sys.argv[1]}). median wall_ms, rel% vs K=1. n=samples.")
print(f"{'q':9} | " + " | ".join(f"K={k:<2} med   rel%" for k in Ks))
for q in qs:
    base = D.get((q, 1))
    bm = st.median(base) if base else None
    segs = []
    for k in Ks:
        v = D.get((q, k))
        if not v: segs.append(f"K={k}: --        "); continue
        m = st.median(v)
        rel = ((m - bm) / bm * 100.0) if bm else 0.0
        mark = ""
        # crude noise band: 1 stdev of this cell as %
        sd = st.pstdev(v) if len(v) > 1 else 0.0
        band = max(5.0, sd / m * 100.0 if m else 5.0)
        if k != 1 and abs(rel) > band: mark = "*"
        segs.append(f"{m:7.0f} {rel:+5.1f}{mark} (n{len(v)})")
    print(f"{q:9} | " + " | ".join(segs))
print("\n# K>1 FASTER (negative rel beyond noise) = the overlap win; for msg_zerocopy a flat curve is the H4 FAIL.")
