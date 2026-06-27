#!/usr/bin/env python3
"""Hot/Cold ClickBench-100M — Unit 1 analysis of results/bench/cells.tsv.

Reports, per (W, fraction), aggregated over eligible queries (losses INCLUDED, no cherry-pick):
  - the overlap verdict counts (HIDDEN vs ADD) and the geomean/median of hidden_frac;
  - geomean/median/min/max of the streaming overhead ratio merge_ch/cold_ch (1.0 = hot fully hidden);
  - the regime split: HIDDEN concentrates where cold_ch >> hot_ch (heavy query x small fraction);
  - read-bytes (C1 cold-filter inflation), spill count (C4), push-down (ShmAdoptedBlocks min).
All times are CH-internal query_duration_ms (the fair merge-EXECUTION metric; bash wall carries the
POC producer-launch overhead). Usage: analyze_bench.py [cells.tsv]
"""
import sys, math, statistics, os

P = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(os.path.abspath(__file__)), "results/bench/cells.tsv")

def geomean(xs):
    xs = [x for x in xs if x and x > 0]
    return math.exp(sum(math.log(x) for x in xs)/len(xs)) if xs else float('nan')

rows = []
with open(P) as f:
    hdr = f.readline().rstrip("\n").split("\t")
    idx = {k: i for i, k in enumerate(hdr)}
    for line in f:
        p = line.rstrip("\n").split("\t")
        if len(p) < len(hdr): continue
        def g(k, cast=float):
            try: return cast(p[idx[k]])
            except Exception: return None
        rows.append(dict(f=p[idx['f']], W=p[idx['W']], q=p[idx['q']],
                         cold=g('pure_ch'), hot=g('hot_ch'), merge=g('merge_ch'),
                         msd=g('merge_ch_sd'), ovr=g('overlap_ch'), hid=g('hidden_frac'),
                         verdict=p[idx['verdict']], rmb=g('read_mb'), spill=g('spill', int) or 0,
                         blk=g('shmblk', int) or 0))

Ws = sorted({r['W'] for r in rows}, key=int)
Fs = sorted({r['f'] for r in rows})
print(f"# cells: {len(rows)}   W: {Ws}   fractions: {Fs}\n")
for W in Ws:
    for f in Fs:
        cell = [r for r in rows if r['W']==W and r['f']==f and r['cold'] and r['merge']]
        if not cell: continue
        ratios = [r['merge']/r['cold'] for r in cell if r['cold']>0]
        hids = [r['hid'] for r in cell if r['hid'] is not None]
        hidden = [r for r in cell if r['verdict']=='HIDDEN']; add=[r for r in cell if r['verdict']=='ADD']
        # regime: cold/hot ratio buckets
        heavy = [r for r in cell if r['hot'] and r['cold']/max(r['hot'],1e-9) >= 4]   # cold >= 4x hot
        heavy_hidden = [r for r in heavy if r['verdict']=='HIDDEN']
        minblk = min((r['blk'] for r in cell if r['blk'] is not None), default=0)
        spills = sum(1 for r in cell if r['spill'])
        print(f"== W={W} f={f}  (n={len(cell)}) ==")
        print(f"  overlap verdict: HIDDEN={len(hidden)}  ADD={len(add)}   (HIDDEN = hot hidden under cold within noise)")
        print(f"  hidden_frac: geomean={geomean([h for h in hids if h>0]):.2f} median={statistics.median(hids):.2f}  (1=fully hidden)")
        print(f"  merge_ch/cold_ch (streaming overhead): geomean={geomean(ratios):.3f} median={statistics.median(ratios):.3f} min={min(ratios):.3f} max={max(ratios):.3f}")
        print(f"  regime cold>=4x hot: {len(heavy)} queries, {len(heavy_hidden)} HIDDEN ({100*len(heavy_hidden)/max(len(heavy),1):.0f}%)")
        print(f"  cold-filter read: median read_mb={statistics.median([r['rmb'] for r in cell if r['rmb']]):.0f}   spill cells={spills}   min ShmAdoptedBlocks={minblk}")
        # worst (most additive) and best (most hidden) examples
        cell_sorted = sorted(cell, key=lambda r: r['merge']/r['cold'])
        best = cell_sorted[0]; worst = cell_sorted[-1]
        print(f"  best  q{best['q']}: cold={best['cold']:.0f} hot={best['hot']:.0f} merge={best['merge']:.0f} ratio={best['merge']/best['cold']:.2f} {best['verdict']}")
        print(f"  worst q{worst['q']}: cold={worst['cold']:.0f} hot={worst['hot']:.0f} merge={worst['merge']:.0f} ratio={worst['merge']/worst['cold']:.2f} {worst['verdict']}")
        print()
