#!/usr/bin/env python3
"""Hot-Cold Phase 1 — 3-way transport overhead table (adopt vs copy vs tcp, W=8).

Reads results/{adopt,copy,tcp}/<bench>/cells.tsv and, per cell eligible+live+correct in ALL THREE
modes, reports wall medians + tcp overhead vs BOTH SHM modes (the Phase-1 AC), consumer cores, and
producer phase split. Same noise rule as Phase 0 (rel diff <= max(5%, off_sd/off_med)).
"""
import csv, os, sys, math

ROOT = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(__file__), "results")
PASS_EXACT = ("exact", "exact(bpchar)", "empty-both")

def load(mode, bench):
    p = os.path.join(ROOT, mode, bench, "cells.tsv")
    if not os.path.exists(p): return {}
    return {r["q"]: r for r in csv.DictReader(open(p), delimiter="\t")}

def f(x):
    try: return float(x)
    except: return float("nan")

def included(r):
    if not r or not r.get("live", "").startswith("yes"): return False
    c = r.get("correct", "")
    return c in PASS_EXACT or c.startswith("approx") or c.startswith("topN")

def cons_cores(r):
    s = f(r["off_med_ms"]) / 1000.0
    return (f(r["cons_user_us"]) + f(r["cons_sys_us"])) / 1e6 / s if s > 0 else float("nan")

def main():
    print("# Hot-Cold Phase 1 — TCP vs SHM-adopt vs SHM-copy overhead (W=8, fresh same-session/binary)\n")
    for bench in ("tpch", "clickbench"):
        A, C, T = load("adopt", bench), load("copy", bench), load("tcp", bench)
        keys = sorted(set(A) & set(C) & set(T), key=lambda q: int(q) if q.isdigit() else 0)
        if not keys: continue
        print(f"## {bench}\n")
        print("| q | adopt_ms | copy_ms | tcp_ms | tcp/adopt% | tcp/copy% | band% | verdict | adopt_cons | tcp_cons | dCons(tcp-adopt) | tcp_recvms | adopt_spd | tcp_spd |")
        print("|--:|--:|--:|--:|--:|--:|--:|--|--:|--:|--:|--:|--:|--:|")
        ta, tc, excl = [], [], []
        for q in keys:
            a, c, t = A[q], C[q], T[q]
            if not (included(a) and included(c) and included(t)):
                excl.append((q, a.get("correct"), c.get("correct"), t.get("correct"))); continue
            am, cm, tm = f(a["off_med_ms"]), f(c["off_med_ms"]), f(t["off_med_ms"])
            ova = (tm - am) / am * 100 if am > 0 else float("nan")
            ovc = (tm - cm) / cm * 100 if cm > 0 else float("nan")
            band = max(0.05, f(a["off_sd"]) / am if am > 0 else 0.05) * 100
            verdict = "within-noise" if abs(ova) <= band else ("SLOWER" if ova > 0 else "faster")
            ta.append(ova); tc.append(ovc)
            print(f"| {q} | {am:.0f} | {cm:.0f} | {tm:.0f} | {ova:+.1f} | {ovc:+.1f} | {band:.1f} | {verdict} "
                  f"| {cons_cores(a):.3f} | {cons_cores(t):.3f} | {cons_cores(t)-cons_cores(a):+.3f} "
                  f"| {f(t['st_cpu']):.1f} | {f(a['speedup']):.2f} | {f(t['speedup']):.2f} |")
        if ta:
            def summ(xs):
                xs = sorted(xs); med = xs[len(xs)//2]
                gm = (math.exp(sum(math.log(max(1e-9, 1 + x/100)) for x in xs)/len(xs)) - 1)*100
                return med, gm, xs[0], xs[-1]
            ma, ga, lo_a, hi_a = summ(ta); mc, gc, lo_c, hi_c = summ(tc)
            print(f"\n**{bench} (n={len(ta)}):** tcp vs adopt median **{ma:+.1f}%** (geomean {ga:+.1f}%, "
                  f"[{lo_a:+.1f},{hi_a:+.1f}]); tcp vs copy median **{mc:+.1f}%** (geomean {gc:+.1f}%, "
                  f"[{lo_c:+.1f},{hi_c:+.1f}]).\n")
        if excl:
            print("_excluded (not comparable in all 3 modes): " +
                  ", ".join(f"Q{q}(a:{ca}/c:{cc}/t:{tt})" for q, ca, cc, tt in excl) + "_\n")

if __name__ == "__main__":
    main()
