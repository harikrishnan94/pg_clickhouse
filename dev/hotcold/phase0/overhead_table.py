#!/usr/bin/env python3
"""Hot-Cold Phase 0 — copy-vs-adopt overhead table (W=8).

Joins results/adopt/<bench>/cells.tsv and results/copy/<bench>/cells.tsv on (bench,q,W) and,
per cell eligible+live+correct in BOTH modes, reports:
  wall    : off_med_ms adopt vs copy -> ovh% = (copy-adopt)/adopt ; vs noise band max(5%, off_sd/off_med)
  consumer: cons_cores = (cons_user_us+cons_sys_us)/1e6 / (off_med_ms/1000) ; dCons = copy-adopt (the copy cost, cores)
  stall   : st_cpu (PUBLISH_STALL producer CPU-ms summed over workers/K) adopt vs copy  [C5: copy <= adopt]
  speedup : vs native, adopt vs copy
Units (verified): off_med_ms/off_min/off_max/off_sd in ms; rd/df/pb/st_cpu in CPU-ms; cons_*_us in us.
Noise band (committed): rel diff <= max(5%, 1 stdev/median). Within band => "within-noise", never a win/regression.
"""
import csv, os, sys, math

ROOT = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(__file__), "results")
PASS_PREFIX = ("approx", "topN")
PASS_EXACT = ("exact", "exact(bpchar)", "empty-both")

def load(mode, bench):
    p = os.path.join(ROOT, mode, bench, "cells.tsv")
    if not os.path.exists(p): return {}
    return {(r["bench"], r["q"], r["W"]): r for r in csv.DictReader(open(p), delimiter="\t")}

def f(x):
    try: return float(x)
    except: return float("nan")

def included(r):
    if not r or not r.get("live", "").startswith("yes"): return False
    c = r.get("correct", "")
    return c in PASS_EXACT or c.startswith(PASS_PREFIX)

def cons_cores(r):
    s = f(r["off_med_ms"]) / 1000.0
    return (f(r["cons_user_us"]) + f(r["cons_sys_us"])) / 1e6 / s if s > 0 else float("nan")

def main():
    print("# Hot-Cold Phase 0 — copy-vs-adopt overhead (W=8, fresh same-session baseline)\n")
    print("Cell shown only if eligible+live+correct in BOTH modes. `ovh%` = wall overhead of copy over "
          "adopt; `band%` = max(5%, adopt off_sd/off_med). `dCons` = copy consumer cores − adopt "
          "consumer cores (the copy cost). `stall_ms` = PUBLISH_STALL producer CPU-ms (C5: copy ≤ adopt).\n")
    for bench in ("tpch", "clickbench"):
        A, C = load("adopt", bench), load("copy", bench)
        keys = sorted(set(A) & set(C), key=lambda k: (int(k[1]) if k[1].isdigit() else 0))
        if not keys: continue
        print(f"## {bench}\n")
        print("| q | adopt_ms | copy_ms | ovh% | band% | verdict | adopt_cons | copy_cons | dCons | adopt_stall | copy_stall | adopt_spd | copy_spd |")
        print("|--:|---------:|--------:|-----:|------:|---------|----------:|---------:|------:|------------:|-----------:|----------:|---------:|")
        ovhs, dcons_l, stall_ok, stall_n, excluded = [], [], 0, 0, []
        for k in keys:
            a, c = A[k], C[k]
            if not (included(a) and included(c)):
                excluded.append((k[1], a.get("correct"), c.get("correct"), a.get("live"), c.get("live")))
                continue
            am, cm = f(a["off_med_ms"]), f(c["off_med_ms"])
            ovh = (cm - am) / am * 100 if am > 0 else float("nan")
            band = max(0.05, f(a["off_sd"]) / am if am > 0 else 0.05) * 100
            verdict = "within-noise" if abs(ovh) <= band else ("SLOWER" if ovh > 0 else "faster")
            ac, cc = cons_cores(a), cons_cores(c)
            ovhs.append(ovh); dcons_l.append(cc - ac)
            ast, cst = f(a["st_cpu"]), f(c["st_cpu"])
            stall_n += 1; stall_ok += 1 if cst <= ast + max(1.0, 0.05 * ast) else 0
            print(f"| {k[1]} | {am:.0f} | {cm:.0f} | {ovh:+.1f} | {band:.1f} | {verdict} "
                  f"| {ac:.3f} | {cc:.3f} | {cc-ac:+.3f} | {ast:.1f} | {cst:.1f} | {f(a['speedup']):.2f} | {f(c['speedup']):.2f} |")
        if ovhs:
            ovhs.sort()
            med = ovhs[len(ovhs)//2]
            gm = (math.exp(sum(math.log(max(1e-9, 1 + o/100)) for o in ovhs) / len(ovhs)) - 1) * 100
            within = sum(1 for k in (set(A) & set(C)) if included(A[k]) and included(C[k]) and
                         abs((f(C[k]["off_med_ms"]) - f(A[k]["off_med_ms"])) / f(A[k]["off_med_ms"]) * 100)
                         <= max(0.05, f(A[k]["off_sd"]) / f(A[k]["off_med_ms"]) if f(A[k]["off_med_ms"])>0 else .05)*100)
            dcons_l.sort(); medd = dcons_l[len(dcons_l)//2]
            print(f"\n**{bench} (n={len(ovhs)} comparable cells):** median wall overhead **{med:+.1f}%** ; "
                  f"geomean {gm:+.1f}% ; range [{ovhs[0]:+.1f}%, {ovhs[-1]:+.1f}%] ; within-noise {within}/{len(ovhs)} ; "
                  f"median dCons {medd:+.3f} cores ; PUBLISH_STALL copy≤adopt {stall_ok}/{stall_n}.\n")
        if excluded:
            print(f"_excluded (not comparable in both modes): " +
                  ", ".join(f"Q{q}(a:{ca}/c:{cc})" for q, ca, cc, _, _ in excluded) + "_\n")

if __name__ == "__main__":
    main()
