#!/usr/bin/env python3
"""Hot/Cold ClickBench-100M — merge / pure-CH SQL generator (Unit 0).

Given a captured deparser-faithful CH template (templates/q<q>.ch.sql) whose source is a single
`(SELECT * FROM streamed_table('<ring>', '<projected schema>'))`, emit the SAME body with the source
replaced by either:
  - pure-CH  : (SELECT <projcols> FROM clickbench.hits_dt64)           [the 100M ground truth]
  - merge(f) : ((SELECT <projcols> FROM streamed_table('pgch_hot_p<f>', '<FULL 105 wire schema>'))
               UNION ALL
               (SELECT <projcols, cold-cast> FROM clickbench.hits_100m WHERE tuple < B(f)))

Column projection is per-query minimal (the cold MergeTree arm reads only referenced columns -> fair
vs pure-CH). The hot arm declares the FULL 105-col wire schema (the ring layout) and projects the
subset. Timestamp columns are DateTime64(6,'UTC') on the hot/wire side; the cold arm casts hits_100m
DateTime->DateTime64 so the UNION column types line up (union output names come from the hot arm).

Usage:
  mk_merge_sql.py ddl                      -> print the hits_dt64 CREATE VIEW DDL
  mk_merge_sql.py <template> pure          -> print pure-CH SQL
  mk_merge_sql.py <template> p01|p05|p10   -> print merge SQL for that fraction
"""
import sys, os, re

HERE = os.path.dirname(os.path.abspath(__file__))

def load_colmap():
    cols = []  # (pos, Camel, lower, basetype, dt64)
    with open(os.path.join(HERE, "colmap.tsv")) as f:
        for line in f:
            p = line.rstrip("\n").split("\t")
            if len(p) < 5: continue
            cols.append((int(p[0]), p[1], p[2], p[3], p[4] == "1"))
    cols.sort()
    return cols

def wiretype(basetype, dt64):
    # single quotes doubled because the schema is itself a single-quoted CH string literal
    return "DateTime64(6, ''UTC'')" if dt64 else basetype

def full_hot_schema(cols):
    return ", ".join(f"{lo} {wiretype(bt, d)}" for (_, _, lo, bt, d) in cols)

def load_boundaries():
    env = {}
    with open(os.path.join(HERE, "boundaries.env")) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line: continue
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip().strip("'")
    return env

def cold_pred(env, f):  # f in {01,05,10}
    et, wid, uid, cid, ed = (env[f"B{f}_EventTime"], env[f"B{f}_WatchID"],
                             env[f"B{f}_UserID"], env[f"B{f}_CounterID"], env[f"B{f}_EventDate"])
    return (f"(EventTime, WatchID, UserID, CounterID, EventDate) < "
            f"(toDateTime('{et}'), {wid}, {uid}, {cid}, toDate('{ed}'))")

ST_RE = re.compile(r"streamed_table\('[^']*',\s*'((?:[^']|'')*)'\)")

def projected_cols(schema_str):
    """Split a 'col Type, col Type, ...' schema at TOP-LEVEL commas (commas inside
    DateTime64(6, 'UTC') are at paren depth > 0) and take each part's first token."""
    parts, cur, depth = [], "", 0
    for ch in schema_str:
        if ch == "(":
            depth += 1; cur += ch
        elif ch == ")":
            depth -= 1; cur += ch
        elif ch == "," and depth == 0:
            parts.append(cur); cur = ""
        else:
            cur += ch
    if cur.strip():
        parts.append(cur)
    return [p.split()[0] for p in parts if p.strip()]

# Cold-arm timestamp normalization (A1 fix, see METHODOLOGY-LOG L0040 / ADVERSARIAL-REVIEW).
# CH server TZ is Asia/Kolkata; hits_100m.EventTime is a DateTime INSTANT. The hot arm streams the
# PG naive wall-clock (e.g. '2013-08-01 01:01:07') as DateTime64(6,'UTC') — matching native-PG
# ClickBench semantics. toDateTime64(EventTime,'UTC') would instead RELABEL the instant (−5:30),
# disagreeing with the hot arm and native PG. toDateTime64(toString(EventTime),'UTC') reproduces the
# server-TZ wall-clock digits as a UTC DateTime64 — identical to the streamed hot value.
def dt64_cast(camel):
    return f"toDateTime64(toString({camel}), 6, 'UTC')"

def hits_dt64_ddl(cols):
    proj = []
    for (_, camel, lo, bt, d) in cols:
        proj.append(f"{dt64_cast(camel)} AS {lo}" if d else f"{camel} AS {lo}")
    return ("CREATE OR REPLACE VIEW clickbench.hits_dt64 AS\nSELECT " +
            ",\n       ".join(proj) + "\nFROM clickbench.hits_100m")

def main():
    cols = load_colmap()
    by_lower = {lo: (camel, d) for (_, camel, lo, bt, d) in cols}
    if sys.argv[1] == "ddl":
        print(hits_dt64_ddl(cols)); return
    tmpl_path, mode = sys.argv[1], sys.argv[2]
    body = open(tmpl_path).read().strip()
    m = ST_RE.search(body)
    if not m:
        sys.stderr.write(f"no streamed_table in {tmpl_path}\n"); sys.exit(2)
    pcols = projected_cols(m.group(1))            # lowercase projected names
    if mode == "pure":
        repl = "(SELECT " + ", ".join(pcols) + " FROM clickbench.hits_dt64)"
    else:
        # modes: "p01"/"p05"/"p10" (merge); "p01:hot" (hot arm only, overlap counterfactual);
        #        "p01:parP" (P-way PARALLEL hot: P disjoint sub-tables/rings unioned — iter-2 optimization);
        #        "p01:parP:hot" (parallel hot arm only).
        parts = mode.split(":")
        ftag = parts[0]                           # p01
        f = ftag[1:]                              # 01
        par = next((int(p[3:]) for p in parts if p.startswith("par")), 1)
        hotonly = "hot" in parts
        env = load_boundaries()
        sch = full_hot_schema(cols)
        if par > 1:
            arms = [f"SELECT {', '.join(pcols)} FROM streamed_table('pgch_hot_{ftag}_{w}', '{sch}')" for w in range(par)]
            hot = "(" + " UNION ALL ".join(f"({a})" for a in arms) + ")"
        else:
            hot = f"(SELECT {', '.join(pcols)} FROM streamed_table('pgch_hot_{ftag}', '{sch}'))"
        if hotonly:
            repl = f"({hot})"
        else:
            cold_sel = []
            for c in pcols:
                camel, d = by_lower[c]
                cold_sel.append(dt64_cast(camel) if d else camel)
            cold = ("SELECT " + ", ".join(cold_sel) +
                    f" FROM clickbench.hits_100m WHERE {cold_pred(env, f)}")
            repl = f"(({hot}) UNION ALL ({cold}))"
    print(body[:m.start()] + repl + body[m.end():])

if __name__ == "__main__":
    main()
