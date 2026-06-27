#!/usr/bin/env python3
"""Aggregate a P2 K-sweep: results/<root>/k<K>/r<round>/tcp/<bench>/cells.tsv. For each (bench,q,K) compute
the across-round median + spread of off_med_ms; report each K vs the K=1 baseline (same binary, run-ahead
varied by GUC). band = max(5%, max across-round sd / median). Usage: agg_ksweep.py <ksweep_root>"""
import sys, os, glob, statistics as st

root = sys.argv[1]

def collect():
    # out[(bench,q,K)] = [off_med per round]
    out = {}
    for cells in glob.glob(os.path.join(root, "k*", "r*", "tcp", "*", "cells.tsv")):
        parts = cells.split(os.sep)
        K = int(parts[-5][1:])      # k<K>
        bench = parts[-2]
        with open(cells) as f:
            lines = [l.rstrip("\n").split("\t") for l in f]
        if len(lines) < 2: continue
        h = {k: i for i, k in enumerate(lines[0])}
        for c in lines[1:]:
            if len(c) <= h["off_med_ms"] or c[h["live"]] != "yes": continue
            try: v = float(c[h["off_med_ms"]])
            except ValueError: continue
            if v > 0: out.setdefault((bench, c[h["q"]], K), []).append(v)
    return out

D = collect()
Ks = sorted({k[2] for k in D})
cells = sorted({(b, q) for (b, q, k) in D}, key=lambda x: (x[0], int(x[1])))
print(f"# P2 K-sweep ({root}). rel% vs K=1 (same binary). band=max(5%, across-round sd/med).")
hdr = f"{'bench':10} {'q':>3} | " + " | ".join(f"K={k:<2}med [rng]      rel%" for k in Ks)
print(hdr)
any_win = {k: 0 for k in Ks}; any_slow = {k: 0 for k in Ks}
for (b, q) in cells:
    base = D.get((b, q, 1))
    if not base: continue
    bm = st.median(base)
    row = f"{b:10} {q:>3} | "
    segs = []
    for k in Ks:
        v = D.get((b, q, k))
        if not v: segs.append(f"K={k}: --              "); continue
        m = st.median(v); rel = (m - bm) / bm * 100.0
        sd = st.pstdev(v) if len(v) > 1 else 0.0
        band = max(5.0, sd / m * 100.0 if m else 5.0)
        tag = "parity" if abs(rel) <= band else ("SLOWER" if rel > 0 else "FASTER")
        if k != 1 and tag == "FASTER": any_win[k] += 1
        if k != 1 and tag == "SLOWER": any_slow[k] += 1
        segs.append(f"{m:6.0f}[{min(v):.0f},{max(v):.0f}] {rel:+5.1f}{('*' if tag!='parity' else ' ')}")
    print(row + " | ".join(segs))
print()
print("## per-K vs K=1 (cells beyond noise):", {k: f"{any_win[k]}faster/{any_slow[k]}slower" for k in Ks if k != 1})
print("## (bare loopback: expect ~all parity = the pre-registered NULL; netem rate: expect K>1 FASTER)")
