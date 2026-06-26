#!/usr/bin/env python3
"""Hot-Cold Phase 3 — parity comparator. Compares offload wall (off_med_ms) per (bench,q,W)
between a TREATMENT cells.tsv tree and a BASELINE tree, applying the binding noise rule:
an effect is real only if |rel_diff| > max(5%, 1 stdev/med). Otherwise = parity.

Usage:
  compare_parity.py <treatment_root> <baseline_root> [--label-t C1 --label-b iouring]
Roots contain <transport>/<bench>/cells.tsv. Emits a per-cell table + a verdict summary.
"""
import sys, os, math

HDR = None
def load(root):
    """root/<transport>/<bench>/cells.tsv -> {(transport,bench,q,W): row-dict}"""
    global HDR
    out = {}
    for tr in ("tcp", "arrow"):
        for bench in ("clickbench", "tpch"):
            p = os.path.join(root, tr, bench, "cells.tsv")
            if not os.path.isfile(p):
                continue
            with open(p) as f:
                lines = [l.rstrip("\n") for l in f]
            if not lines:
                continue
            hdr = lines[0].split("\t")
            HDR = hdr
            idx = {k: i for i, k in enumerate(hdr)}
            for l in lines[1:]:
                c = l.split("\t")
                if len(c) < len(hdr):
                    continue
                key = (tr, bench, c[idx["q"]], c[idx["W"]])
                out[key] = {k: c[idx[k]] for k in idx}
    return out

def fnum(x):
    try:
        return float(x)
    except (ValueError, TypeError):
        return None

def main():
    t_root, b_root = sys.argv[1], sys.argv[2]
    lt = sys.argv[sys.argv.index("--label-t")+1] if "--label-t" in sys.argv else "treatment"
    lb = sys.argv[sys.argv.index("--label-b")+1] if "--label-b" in sys.argv else "baseline"
    T, B = load(t_root), load(b_root)
    keys = sorted(set(T) & set(B))
    print(f"# Parity: {lt} (treatment) vs {lb} (baseline). Rule: real iff |rel|>max(5%,1sd).")
    print(f"{'transport':9} {'bench':10} {'q':>3} {'W':>2}  {lb+'_ms':>11} {lt+'_ms':>11} {'rel%':>7} {'noise%':>7} {'verdict':>9}  corr")
    regressions, improvements, parities, skipped = [], [], [], []
    for k in keys:
        tr, bench, q, W = k
        tb, tt = B[k], T[k]
        # only compare cells live in BOTH
        if tb.get("live") != "yes" or tt.get("live") != "yes":
            skipped.append((k, f"live b={tb.get('live')} t={tt.get('live')}"))
            continue
        ob, otv = fnum(tb["off_med_ms"]), fnum(tt["off_med_ms"])
        if not ob or not otv or ob <= 0:
            skipped.append((k, "no off_med")); continue
        rel = (otv - ob) / ob * 100.0
        # noise band: max(5%, 1 stdev of the baseline median expressed as %)
        sdb = fnum(tb.get("off_sd")) or 0.0
        noise = max(5.0, (sdb / ob * 100.0) if ob else 5.0)
        corr_t, corr_b = tt.get("correct"), tb.get("correct")
        corr = f"{corr_b}->{corr_t}"
        if abs(rel) <= noise:
            verdict = "parity"; parities.append(k)
        elif rel > 0:
            verdict = "SLOWER"; regressions.append((k, rel, noise))
        else:
            verdict = "faster"; improvements.append((k, rel, noise))
        print(f"{tr:9} {bench:10} {q:>3} {W:>2}  {ob:>11.1f} {otv:>11.1f} {rel:>+6.1f}% {noise:>6.1f}% {verdict:>9}  {corr}")
    print()
    print(f"## Summary: {len(parities)} parity, {len(improvements)} faster, {len(regressions)} SLOWER, {len(skipped)} skipped")
    if regressions:
        print("### REGRESSIONS (rel > noise, treatment slower) — BLOCK parity claim:")
        for k, rel, noise in regressions:
            print(f"   {k}  {rel:+.1f}% (noise {noise:.1f}%)")
    # correctness drift check
    drift = [k for k in keys if T[k].get("live")=="yes" and B[k].get("live")=="yes"
             and T[k].get("correct") != B[k].get("correct")]
    if drift:
        print("### CORRECTNESS VERDICT CHANGED (investigate — possible new DIFF):")
        for k in drift:
            print(f"   {k}  {B[k].get('correct')} -> {T[k].get('correct')}")
    else:
        print("### correctness verdicts identical across all compared cells (no new DIFF)")

if __name__ == "__main__":
    main()
