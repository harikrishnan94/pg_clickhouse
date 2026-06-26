#!/usr/bin/env python3
"""Aggregate the interleaved A/B rounds: for each (transport,bench,q) compute the across-round median
and spread (min/max/sd) of off_med_ms for C1 vs baseline, then the parity verdict with a band derived
from the OBSERVED between-run variance (not the within-run sd). Usage: agg_interleave.py <interleave_root>"""
import sys, os, glob, statistics as st

root = sys.argv[1]
TREAT = sys.argv[2] if len(sys.argv) > 2 else "c1"   # treatment-side subdir name (c1 | p1)

def collect(side):
    """side in {c1,baseline}; -> {(tr,bench,q): [off_med per round]}"""
    out = {}
    for cells in glob.glob(os.path.join(root, side, "r*", "*", "*", "cells.tsv")):
        parts = cells.split(os.sep)
        tr, bench = parts[-3], parts[-2]
        with open(cells) as f:
            lines = [l.rstrip("\n").split("\t") for l in f]
        if len(lines) < 2:
            continue
        h = {k: i for i, k in enumerate(lines[0])}
        for c in lines[1:]:
            if len(c) <= h["off_med_ms"] or c[h["live"]] != "yes":
                continue
            try:
                v = float(c[h["off_med_ms"]])
            except ValueError:
                continue
            if v > 0:
                out.setdefault((tr, bench, c[h["q"]]), []).append(v)
    return out

C1, BL = collect(TREAT), collect("baseline")
keys = sorted(set(C1) & set(BL), key=lambda k: (k[0], k[1], int(k[2])))
print(f"# Interleaved A/B parity (across-round). band = max(5%, max(sd_c1,sd_bl)/med).")
print(f"{'tr':4} {'bench':10} {'q':>3} | {'bl_med':>7} {'bl_rng':>13} | {'c1_med':>7} {'c1_rng':>13} | {'rel%':>6} {'band%':>6} {'verdict':>8}")
verds = {}
for k in keys:
    tr, bench, q = k
    b, c = BL[k], C1[k]
    bm, cm = st.median(b), st.median(c)
    bsd = st.pstdev(b) if len(b) > 1 else 0.0
    csd = st.pstdev(c) if len(c) > 1 else 0.0
    rel = (cm - bm) / bm * 100.0
    band = max(5.0, (max(bsd, csd) / bm * 100.0) if bm else 5.0)
    verdict = "parity" if abs(rel) <= band else ("SLOWER" if rel > 0 else "faster")
    verds[verdict] = verds.get(verdict, 0) + 1
    print(f"{tr:4} {bench:10} {q:>3} | {bm:>7.0f} [{min(b):>5.0f},{max(b):>5.0f}] | {cm:>7.0f} [{min(c):>5.0f},{max(c):>5.0f}] | {rel:>+5.1f} {band:>5.1f} {verdict:>8}   (n_c1={len(c)},n_bl={len(b)})")
print()
print("## verdicts:", verds)
slower = [k for k in keys if (st.median(C1[k])-st.median(BL[k]))/st.median(BL[k])*100 >
          max(5.0, max(st.pstdev(BL[k]) if len(BL[k])>1 else 0, st.pstdev(C1[k]) if len(C1[k])>1 else 0)/st.median(BL[k])*100)]
print("## SLOWER (regression) cells:", slower if slower else "NONE")
