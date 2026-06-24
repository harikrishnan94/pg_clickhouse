#!/usr/bin/env python3
"""Numeric-aware fidelity comparator for native (offload OFF) vs offload (ON)
ClickBench result sets.

Both inputs are pipe-separated psql -tA output (one row per line, NULL -> the
token 'NULL' via -P null=NULL). The comparator:

  * pairs rows by a canonical numeric-aware sort over ALL columns (group-key
    columns -- exact strings/ints, emitted first in every ClickBench query --
    dominate the sort, so paired rows share their group key even when an
    aggregate column differs slightly);
  * classifies the deviation and quantifies it:
      exact        - byte-identical after bpchar trailing-blank normalization
      float(<=tol) - only float/numeric columns differ, max rel err <= TOL
                     (the accepted avg()->Float64 round-off deviation)
      approx       - an INTEGER-valued column differs (the count(DISTINCT)
                     exact-vs-approximate axis); reports the integer error
      DIFF         - row-count mismatch or a string/group-key column differs
                     (a real value error OR a LIMIT/top-N tie-boundary reshuffle
                     -- flagged for manual classification, never hidden)
  * prints one machine-readable line:
        class|rows_off|rows_on|max_abs_err|max_rel_err|n_int_diff|n_struct_diff|detail
    and (on stderr, if --verbose) the worst-offending column + sample rows.

Usage: cmp_results.py OFF.out ON.out [TOL] [--verbose]
"""
import sys

def isnum(s):
    if s == '' or s == 'NULL':
        return False
    try:
        float(s)
        return True
    except ValueError:
        return False

def isint(s):
    # integer-valued numeric (count/count-distinct/sum columns): no '.', no exponent
    if not isnum(s):
        return False
    return ('.' not in s) and ('e' not in s) and ('E' not in s)

def load(path):
    rows = []
    err = None
    with open(path, encoding='utf-8', errors='replace') as f:
        for line in f:
            line = line.rstrip('\n')
            if line == '':
                continue
            low = line.lower()
            if ('error' in low or 'fatal' in low or 'server closed' in low
                    or 'terminated' in low) and '|' not in line:
                err = line
                continue
            rows.append(line.split('|'))
    return rows, err

def norm(s):
    # bpchar trailing-blank normalization (HitColor CHAR(1) etc.)
    return s.rstrip(' ')

def main():
    args = [a for a in sys.argv[1:] if a != '--verbose']
    verbose = '--verbose' in sys.argv
    off_path, on_path = args[0], args[1]
    tol = float(args[2]) if len(args) > 2 else 1e-6

    A, errA = load(off_path)
    B, errB = load(on_path)

    if errA or errB:
        tag = []
        if errA:
            tag.append('OFF_ERR')
        if errB:
            tag.append('ON_ERR')
        print('%s|%d|%d|||||%s' % ('/'.join(tag), len(A), len(B),
                                   (errB or errA)[:120]))
        return

    if not A and not B:
        print('exact|0|0|0|0|0|0|both-empty')
        return

    ncols = max((len(r) for r in (A + B)), default=0)

    # Per-column type: numeric if every non-empty value (both sides) parses as a
    # number; integer if additionally no value has a decimal point/exponent.
    numeric = [True] * ncols
    integer = [True] * ncols
    for r in (A + B):
        for i in range(ncols):
            v = r[i] if i < len(r) else ''
            if v in ('', 'NULL'):
                continue
            if not isnum(v):
                numeric[i] = False
                integer[i] = False
            elif not isint(v):
                integer[i] = False

    def key(r):
        k = []
        for i in range(ncols):
            v = r[i] if i < len(r) else ''
            if numeric[i] and v not in ('', 'NULL'):
                k.append((1, float(v)))
            else:
                k.append((0, norm(v)))
        return k

    As = sorted(A, key=key)
    Bs = sorted(B, key=key)

    max_abs = 0.0
    max_rel = 0.0
    max_abs_col = -1
    max_rel_col = -1
    n_int_diff = 0          # integer-column value differences (count-distinct axis)
    max_int_abs = 0
    max_int_rel = 0.0
    n_struct_diff = 0       # string/group-key mismatches or unpaired rows
    worst_int = None
    worst_struct = None

    if len(As) != len(Bs):
        n_struct_diff = abs(len(As) - len(Bs))

    for a, b in zip(As, Bs):
        for i in range(ncols):
            x = a[i] if i < len(a) else ''
            y = b[i] if i < len(b) else ''
            if x == y:
                continue
            if numeric[i] and isnum(x) and isnum(y):
                fx, fy = float(x), float(y)
                d = abs(fx - fy)
                m = max(abs(fx), abs(fy))
                rel = d / m if m else 0.0
                if integer[i]:
                    # integer column mismatch => count(distinct)-style approximation
                    if d > 0:
                        n_int_diff += 1
                        if d > max_int_abs:
                            max_int_abs = int(d)
                        if rel > max_int_rel:
                            max_int_rel = rel
                            worst_int = (i, x, y)
                else:
                    if d > max_abs:
                        max_abs = d
                        max_abs_col = i
                    if rel > max_rel:
                        max_rel = rel
                        max_rel_col = i
            else:
                # string/group-key value differs after bpchar norm => structural
                if norm(x) != norm(y):
                    n_struct_diff += 1
                    if worst_struct is None:
                        worst_struct = (i, x, y)

    # classify
    detail = ''
    if n_struct_diff > 0:
        cls = 'DIFF'
        if worst_struct:
            detail = 'col%d off=%r on=%r' % worst_struct
        elif len(As) != len(Bs):
            detail = 'rowcount %d/%d' % (len(As), len(Bs))
    elif n_int_diff > 0:
        cls = 'approx'
        detail = ('int col%d off=%s on=%s relerr=%.4g' %
                  (worst_int[0], worst_int[1], worst_int[2], max_int_rel)
                  if worst_int else 'int-diff')
        # fold integer error into the abs/rel report too
        max_abs = max(max_abs, float(max_int_abs))
        max_rel = max(max_rel, max_int_rel)
    elif max_abs == 0.0 and max_rel == 0.0:
        cls = 'exact'
    else:
        # any non-zero float/numeric difference (the avg()->Float64 axis): always
        # surfaced with its measured error -- the ledger judges the bound, not this.
        cls = 'float' if max_rel <= tol else 'float!'
        detail = 'col%d rel=%.4g abs=%.6g' % (max_rel_col, max_rel, max_abs)

    print('%s|%d|%d|%.6g|%.6g|%d|%d|%s' % (
        cls, len(A), len(B), max_abs, max_rel, n_int_diff, n_struct_diff, detail))

    if verbose and (cls in ('DIFF', 'approx', 'float')):
        sys.stderr.write('  worst_int=%s worst_struct=%s max_abs_col=%d max_rel_col=%d\n'
                         % (worst_int, worst_struct, max_abs_col, max_rel_col))
        sys.stderr.write('  sample OFF: %s\n' % ('  '.join('|'.join(r) for r in As[:3])))
        sys.stderr.write('  sample ON : %s\n' % ('  '.join('|'.join(r) for r in Bs[:3])))

if __name__ == '__main__':
    main()
