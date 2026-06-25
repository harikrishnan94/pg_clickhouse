#!/usr/bin/env python3
# Tolerant result comparator for the correctness oracle.
# Usage: cmp.py NATIVE_FILE OFFLOAD_FILE
# Inputs are psql -tA -F'|' result dumps (no header, '|'-separated fields).
# Prints ONE verdict token on stdout:
#   exact            : byte-identical as a multiset of rtrimmed rows
#   exact(bpchar)    : identical only after stripping trailing blanks (F5)
#   approx(<=R)      : numeric value-equal within relative tol R (avg->Float64 F3, decimal scale F1)
#   topN(<=R)        : same as approx but row order differed pre-sort (top-N tie reshuffle)
#   DIFF(n)          : n genuinely differing rows  -> correctness FAIL
#   rowcount(a!=b)   : different row counts         -> correctness FAIL
#   empty-both/one   : one or both empty
# Bounded-deviation tolerance: 1e-12 relative (well above Float64 eps ~2.2e-16, below a
# one-cent error on a billion-scale SF10 sum). Decimal display-scale (0.50 vs 0.5) and
# bpchar trailing blanks compare equal and never consume the tolerance.
import sys

TOL = 1e-12

def load(p):
    with open(p, 'r', errors='replace') as f:
        return [ln.rstrip('\n') for ln in f if ln.strip() != '']

def fields(line):
    return line.split('|')

def num(s):
    try:
        return float(s)
    except ValueError:
        return None

def rtrim_row(line):
    return '|'.join(c.rstrip() for c in fields(line))

def main():
    a = load(sys.argv[1]); b = load(sys.argv[2])
    if not a and not b:
        print("empty-both"); return
    if not a or not b:
        print("empty-one"); return
    # raw multiset equality
    if sorted(a) == sorted(b):
        print("exact"); return
    # bpchar: equal after rtrim of each field
    ar = sorted(rtrim_row(x) for x in a)
    br = sorted(rtrim_row(x) for x in b)
    if ar == br:
        print("exact(bpchar)"); return
    if len(a) != len(b):
        print(f"rowcount({len(a)}!={len(b)})"); return
    # numeric-tolerant, row-aligned after sort (set compare); track order-sensitivity
    order_differed = (a != b) and (sorted(a) != a or sorted(b) != b)
    sa = sorted(a); sb = sorted(b)
    maxrel = 0.0
    diffs = 0
    for ra, rb in zip(sa, sb):
        fa = fields(ra); fb = fields(rb)
        if len(fa) != len(fb):
            diffs += 1; continue
        rowbad = False
        for x, y in zip(fa, fb):
            if x.rstrip() == y.rstrip():
                continue
            nx, ny = num(x), num(y)
            if nx is None or ny is None:
                rowbad = True; break
            denom = max(abs(nx), abs(ny), 1e-300)
            rel = abs(nx - ny) / denom
            if rel > TOL:
                rowbad = True; maxrel = max(maxrel, rel); break
            maxrel = max(maxrel, rel)
        if rowbad:
            diffs += 1
    if diffs == 0:
        tag = "topN" if order_differed else "approx"
        print(f"{tag}(<={maxrel:.1e})")
    else:
        print(f"DIFF({diffs})")

if __name__ == '__main__':
    main()
