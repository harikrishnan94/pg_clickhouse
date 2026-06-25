#!/usr/bin/env python3
# Assemble the final W-sweep + producer-phase-split REPORT.md from the per-cell TSVs
# emitted by wsweep_split.sh. Pure post-processing -- no measurement, no tuning.
#
#   assemble.py results/tpch/cells.tsv results/clickbench/cells.tsv > REPORT.md
import sys, math, os

def load(path):
    rows = []
    with open(path) as f:
        hdr = f.readline().rstrip('\n').split('\t')
        for ln in f:
            p = ln.rstrip('\n').split('\t')
            if len(p) != len(hdr):
                continue
            rows.append(dict(zip(hdr, p)))
    return rows

def fnum(x, d=0.0):
    try: return float(x)
    except: return d

def geomean(xs):
    xs = [x for x in xs if x > 0]
    if not xs: return 0.0
    return math.exp(sum(math.log(x) for x in xs)/len(xs))

def median(xs):
    xs = sorted(xs)
    n = len(xs)
    if n == 0: return 0.0
    return xs[n//2] if n % 2 else (xs[n//2-1]+xs[n//2])/2

PASS_CORRECT = ('exact', 'exact(bpchar)', 'approx', 'topN', 'empty-both')
# Top-N queries whose plain-ORDER-BY DIFF was adjudicated BENIGN tie-reshuffle by
# dev/clickbench/tiebreak_check.sh: each collapses to `exact` once a deterministic
# total order is imposed (evidence/topn-tiebreak-adjudication.txt). Their sweep DIFF
# is the allowed "top-N tie reshuffle under a deterministic tiebreak" deviation.
TIEBREAK_BENIGN = {('clickbench', q) for q in ('18', '22', '32', '33', '39', '40', '41')}

def correct_pass(c, bench=None, q=None):
    if any(c.startswith(p) for p in PASS_CORRECT):
        return True
    if c.startswith('DIFF') and (bench, q) in TIEBREAK_BENIGN:
        return True
    return False

def is_eligible(r):
    return r.get('W','-') != '-' and fnum(r.get('off_med_ms')) > 0

def cell_included(r):
    # report a speedup only if offloaded, live, and correct within bounded deviation
    return is_eligible(r) and r.get('live','').startswith('yes') and correct_pass(r.get('correct',''), r['bench'], r['q'])

def correct_label(r):
    c = r.get('correct','')
    if c.startswith('DIFF') and (r['bench'], r['q']) in TIEBREAK_BENIGN:
        return 'topN-tiebreak'  # benign tie-reshuffle, adjudicated exact under total order
    return c

def phase_cores(r):
    op = fnum(r['off_prod']); rd=fnum(r['rd_cpu']); df=fnum(r['df_cpu']); pb=fnum(r['pb_cpu']); st=fnum(r['st_cpu'])
    tc = rd+df+pb+st
    if tc <= 0: return (0,0,0)
    return (op*rd/tc, op*df/tc, op*pb/tc)

def wallpct(r):
    rw=fnum(r['rd_w']); dw=fnum(r['df_w']); pw=fnum(r['pb_w'])
    tw = rw+dw+pw
    if tw<=0: return (0,0,0)
    return (100*rw/tw, 100*dw/tw, 100*pw/tw)

def per_cell_table(rows):
    out = []
    out.append("| Q | W | nat_med_ms (min/max,sd) | off_med_ms (min/max,sd) | speedup | off_prod cores [read/deform/publish] (+stall_ms) | within-prod wall% [r:d:p] | off_cons cores | consumer_ms | offload/live/correct |")
    out.append("|--:|--:|---|---|--:|---|---|--:|--:|:--|")
    for r in rows:
        if not is_eligible(r):
            continue
        rdc,dfc,pbc = phase_cores(r)
        rp,dp,pp = wallpct(r)
        spd = fnum(r['speedup'])
        out.append("| %s | %s | %s (%s/%s,%s) | %s (%s/%s,%s) | %.2fx | %.2f [%.2f/%.2f/%.2f] (+%.0fms) | %.0f:%.0f:%.0f | %.2f | %s | %s/%s/%s |" % (
            r['q'], r['W'],
            r['nat_med_ms'], r['nat_min'], r['nat_max'], r['nat_sd'],
            r['off_med_ms'], r['off_min'], r['off_max'], r['off_sd'],
            spd, fnum(r['off_prod']), rdc, dfc, pbc, fnum(r['st_w']),
            rp, dp, pp, fnum(r['off_cons']), r['consumer_ms'],
            'Y' if fnum(r['elig_blocks'])>=1 else 'N', r['live'], correct_label(r)))
    return "\n".join(out)

def aggregate(rows, bench):
    out = []
    out.append("### %s — speedup distribution per W (over ALL eligible+live+correct queries, incl. losses)\n" % bench)
    out.append("| W | n_queries | faster(>1x) | slower(<=1x) | geomean | median | min | max |")
    out.append("|--:|--:|--:|--:|--:|--:|--:|--:|")
    for W in ('1','2','4','8'):
        cells = [r for r in rows if r['W']==W and cell_included(r)]
        spd = [fnum(r['speedup']) for r in cells]
        if not spd:
            out.append("| %s | 0 | - | - | - | - | - | - |" % W); continue
        faster = sum(1 for s in spd if s>1.0)
        out.append("| %s | %d | %d | %d | %.2fx | %.2fx | %.2fx | %.2fx |" % (
            W, len(spd), faster, len(spd)-faster, geomean(spd), median(spd), min(spd), max(spd)))
    return "\n".join(out)

def excluded_table(rows):
    out = []
    out.append("| benchmark | Q | reason |")
    out.append("|---|--:|---|")
    seen = set()
    for r in rows:
        b=r['bench']; q=r['q']
        reason = None
        if r.get('W','-') == '-':
            reason = "offload oracle did not fire: " + r.get('correct','').replace('oracle:','')
        elif not correct_pass(r.get('correct',''), b, q):
            reason = "correctness FAIL: %s (offloaded but result deviates beyond bounded fidelity)" % r['correct']
        elif not r.get('live','').startswith('yes'):
            reason = "liveness: %s" % r['live']
        if reason and (b,q) not in seen:
            seen.add((b,q))
            out.append("| %s | %s | %s |" % (b,q,reason))
    return "\n".join(out) if len(out)>2 else (None)

def what_split_shows(rows, bench):
    # classify each included query at the highest W available: producer- vs consumer-bound, dominant phase
    out = []
    byq = {}
    for r in rows:
        if cell_included(r):
            byq.setdefault(r['q'], []).append(r)
    out.append("| Q | W* | off_prod | off_cons | bound | dominant producer phase (cores) | speedup@W* |")
    out.append("|--:|--:|--:|--:|:--|:--|--:|")
    for q in sorted(byq, key=lambda x:int(x) if x.isdigit() else 0):
        cells = byq[q]
        r = sorted(cells, key=lambda c:int(c['W']))[-1]  # highest W
        op=fnum(r['off_prod']); oc=fnum(r['off_cons'])
        rdc,dfc,pbc = phase_cores(r)
        bound = "producer" if op > oc*1.2 else ("consumer" if oc > op*1.2 else "balanced")
        ph = max((('read',rdc),('deform',dfc),('publish',pbc)), key=lambda t:t[1])
        out.append("| %s | %s | %.2f | %.2f | %s | %s (%.2f) | %.2fx |" % (
            q, r['W'], op, oc, bound, ph[0], ph[1], fnum(r['speedup'])))
    return "\n".join(out)

def convergence_appendix(rows):
    out = []
    out.append("For a representative sample of offloaded cells, the in-code reader CPU vs /proc off_prod (G3),")
    out.append("and the CH query_log consumer CPU vs /proc off_cons (G4). Noise band := max(5%, 1 stdev).\n")
    out.append("| benchmark | Q | W | off_prod(/proc) | in-code reader cores (G3) | residual(non-reader PG) | off_cons(/proc) | qlog cons cores (G4) |")
    out.append("|---|--:|--:|--:|--:|--:|--:|--:|")
    sample = [r for r in rows if cell_included(r)]
    # take a spread: every query's W=8 cell if present, capped
    pick = [r for r in sample if r['W']=='8'][:24]
    for r in pick:
        iw = fnum(r['inst_wall_ms']); pcs = fnum(r['prod_cpu_sum'])
        incode = pcs/iw if iw>0 else 0
        op = fnum(r['off_prod']); oc = fnum(r['off_cons'])
        resid = op - incode
        cms = fnum(r['consumer_ms']); uu=fnum(r['cons_user_us']); su=fnum(r['cons_sys_us'])
        qcons = (uu+su)/1000.0/cms if cms>0 else 0
        out.append("| %s | %s | %s | %.2f | %.2f | %.2f | %.2f | %.2f |" % (
            r['bench'], r['q'], r['W'], op, incode, resid, oc, qcons))
    return "\n".join(out)

def main():
    paths = sys.argv[1:]
    allrows = []
    benches = []
    for p in paths:
        if not os.path.exists(p): continue
        rows = load(p)
        b = rows[0]['bench'] if rows else os.path.basename(os.path.dirname(p))
        benches.append((b, rows))
        allrows += rows

    print("# W-sweep + producer-phase-split — RESULTS\n")
    print("Optimally-tuned native PostgreSQL vs SHM-offload wall time, swept over W={1,2,4,8} cores under a")
    print("shared cgroup-v2 cpu.max cap (both the PG postmaster tree AND the live ClickHouse server in ONE")
    print("cgroup). Warm median of N=5 runs/cell. For every offloaded cell, the producer is decomposed into")
    print("READ / DEFORM / PUBLISH cores (+PUBLISH_STALL ms) plus the ClickHouse CONSUMER. See")
    print("00-PRE-REGISTRATION.md for the frozen protocol and 90-CONVERGENCE for the G1-G4 evidence.\n")
    print("Speedup = native_median / offload_median (>1 => offload faster). Producer and consumer run")
    print("CONCURRENTLY (streaming pipeline): cores are additive, wall-times are NOT.\n")

    for b, rows in benches:
        print("\n## %s — per-(query, W)\n" % b.upper())
        print(per_cell_table(rows))
        print()
        print(aggregate(rows, b.upper()))
        print()

    print("\n## Aggregate: what the split shows (highest measured W per query)\n")
    for b, rows in benches:
        print("\n### %s\n" % b.upper())
        print(what_split_shows(rows, b))

    print("\n## Excluded queries (root-caused)\n")
    et = excluded_table(allrows)
    print(et if et else "_none_")

    print("\n## Convergence appendix (G3/G4 per-cell; G1/G2 in evidence/G1-G2-convergence-q6.txt)\n")
    print(convergence_appendix(allrows))

if __name__ == '__main__':
    main()
