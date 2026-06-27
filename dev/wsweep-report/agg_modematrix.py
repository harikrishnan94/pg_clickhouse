#!/usr/bin/env python3
"""Aggregate the transport-mode matrix (modematrix.sh) into a vs-native report WITH phase splits.

Reads results/modematrix/<bench>/matrix.tsv (long format: one row per (q,mode)).
Emits, per bench: (1) per-mode geomean speedup vs optimal-native PG, (2) per-query speedup table,
(3) producer phase split + consumer cost + transport counters per mode (the "where does the time go").
Usage: agg_modematrix.py [dir]
"""
import sys, os, math
from collections import defaultdict

MODES = ["shm_adopt", "shm_copy", "tcp_epoll", "tcp_zerocopy", "tcp_blocking", "arrow"]
LBL = {"shm_adopt": "SHM adopt (zero-copy)", "shm_copy": "SHM copy", "tcp_epoll": "TCP epoll",
       "tcp_zerocopy": "TCP msg_zerocopy", "tcp_blocking": "TCP blocking", "arrow": "Arrow IPC"}

# exact / exact(bpchar) / approx(<=tol) / topN(<=tol) all mean values match (topN = same values,
# ORDER BY ties reshuffled). DIFF / rowcount / empty-* / *-ERR mean a real mismatch.
def is_correct(v): return v.startswith(("exact", "approx", "topN"))
def fnum(s):
    try: return float(s)
    except (ValueError, TypeError): return None
def geomean(xs):
    xs = [x for x in xs if x and x > 0]
    return math.exp(sum(math.log(x) for x in xs)/len(xs)) if xs else None
def median(xs):
    xs = sorted(x for x in xs if x is not None)
    return xs[len(xs)//2] if xs else None

def load(path):
    rows = []
    with open(path) as f:
        hdr = f.readline().rstrip("\n").split("\t")
        for line in f:
            p = line.rstrip("\n").split("\t")
            if len(p) == len(hdr):
                rows.append(dict(zip(hdr, p)))
    return rows

def report(bench, rows):
    o = []
    qs = sorted({r["q"] for r in rows}, key=lambda x: int(x) if x.isdigit() else 999)
    nat = {}
    cell = defaultdict(dict)
    for r in rows:
        q, m = r["q"], r["mode"]
        if r.get("nat_min_ms") not in ("-", "", None):
            nat[q] = fnum(r["nat_min_ms"])
        if m in MODES:
            cell[q][m] = r

    # (1) per-mode speedup summary
    o.append(f"\n## {bench} — speedup vs optimal-native PG (W=8)\n")
    o.append("Speedup = native_med / offload_med (>1 = offload faster).\n")
    o.append("| mode | geomean | median | best | worst | faster>1.05x | slower<0.95x | offloaded | incorrect |")
    o.append("|------|--------:|-------:|-----:|------:|---:|---:|---:|---:|")
    for m in MODES:
        sps, bad = [], 0
        for q in qs:
            r = cell[q].get(m)
            if not r or fnum(r["speedup"]) is None: continue
            if not is_correct(r["correct"]): bad += 1; continue
            sps.append(fnum(r["speedup"]))
        if not sps:
            o.append(f"| {LBL[m]} | – | – | – | – | – | – | 0 | {bad} |"); continue
        g = geomean(sps)
        o.append(f"| {LBL[m]} | **{g:.2f}x** | {median(sps):.2f}x | {max(sps):.2f}x | {min(sps):.2f}x | "
                 f"{sum(1 for x in sps if x>=1.05)} | {sum(1 for x in sps if x<0.95)} | {len(sps)} | {bad} |")

    # (2) per-query speedup
    o.append(f"\n### {bench} — per-query speedup (native ms → speedup per mode)\n")
    o.append("| Q | native ms | " + " | ".join(LBL[m] for m in MODES) + " |")
    o.append("|---|---:|" + "|".join("---:" for _ in MODES) + "|")
    for q in qs:
        nms = nat.get(q); ns = f"{nms:.0f}" if nms else "–"
        c = []
        for m in MODES:
            r = cell[q].get(m)
            if not r or fnum(r["speedup"]) is None: c.append("excl"); continue
            mark = "" if is_correct(r["correct"]) else "⚠"
            c.append(f"{fnum(r['speedup']):.2f}x{mark}")
        o.append(f"| {q} | {ns} | " + " | ".join(c) + " |")

    # (3) phase split + consumer + transport, per mode (median over offloaded+correct queries)
    o.append(f"\n### {bench} — where the time goes, per mode (median over offloaded queries)\n")
    o.append("Producer phases are Σ-across-W-workers wall ms (READ+DEFORM are transport-agnostic; "
             "PUBLISH = ring-memcpy for SHM vs serialize+send for TCP/Arrow; STALL = SHM ring backpressure). "
             "Consumer: total query wall + ShmCopyTime (=memcpy for copy / socket-recv for tcp; 0 for adopt). "
             "Transport counters from the producer 'shm tcp-send' LOG.\n")
    o.append("| mode | prod READ | prod DEFORM | prod PUBLISH | prod STALL | cons total ms | cons copytime ms | "
             "cons user/sys cpu ms | tx method | K | zc_sends | overlap |")
    o.append("|------|--:|--:|--:|--:|--:|--:|--:|:--|--:|--:|--:|")
    def med_of(m, col, scale=1.0):
        vals = []
        for q in qs:
            r = cell[q].get(m)
            if not r or fnum(r["speedup"]) is None or not is_correct(r["correct"]): continue
            v = fnum(r.get(col))
            if v is not None: vals.append(v*scale)
        return median(vals)
    def mode_of(m, col):
        from collections import Counter
        vals = [cell[q][m].get(col) for q in qs if cell[q].get(m) and fnum(cell[q][m]["speedup"]) is not None]
        vals = [v for v in vals if v not in (None, "", "-")]
        return Counter(vals).most_common(1)[0][0] if vals else "–"
    def f(x, d=0): return f"{x:.{d}f}" if x is not None else "–"
    for m in MODES:
        rd = med_of(m, "p_read_wall"); df = med_of(m, "p_deform_wall")
        pb = med_of(m, "p_publish_wall"); st = med_of(m, "p_stall_wall")
        cms = med_of(m, "cons_ms"); cct = med_of(m, "cons_copytime_us", 1e-3)
        cu = med_of(m, "cons_user_us", 1e-3); csy = med_of(m, "cons_sys_us", 1e-3)
        meth = mode_of(m, "tx_method"); K = med_of(m, "tx_K")
        zc = med_of(m, "tx_zc_sends"); ov = med_of(m, "tx_overlap")
        cpu = f"{f(cu)}/{f(csy)}" if cu is not None else "–"
        o.append(f"| {LBL[m]} | {f(rd)} | {f(df)} | {f(pb)} | {f(st,1)} | {f(cms)} | {f(cct)} | {cpu} | "
                 f"{meth} | {f(K)} | {f(zc)} | {f(ov)} |")
    return "\n".join(o)

def main():
    d = sys.argv[1] if len(sys.argv) > 1 else "dev/wsweep-report/results/modematrix"
    print("# Transport-mode matrix vs optimal-native PostgreSQL (W=8)\n")
    print("Best-of-3 (min) wall, ClickBench-style. Each offload mode timed against the SAME same-session "
          "optimal-native baseline (offload off, JIT, parallel costs 0; work_mem=2GB TPC-H / 1GB ClickBench). "
          "⚠ = result mismatch (excluded from geomean). Phase split is from one separate stats-on run (kept "
          "out of the timed min); consumer has no per-phase wall decomposition (only total + ShmCopyTime + cpu).")
    for bench in ("tpch", "clickbench"):
        p = os.path.join(d, bench, "matrix.tsv")
        if not os.path.exists(p): print(f"\n(missing {p})"); continue
        print(report(bench, load(p)))

if __name__ == "__main__":
    main()
