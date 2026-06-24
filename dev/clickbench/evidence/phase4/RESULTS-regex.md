# Phase 4 — Q29 REGEXP_REPLACE PG-vs-CH newline semantics

Root cause: PostgreSQL regex default is dotall (`.` matches newline; newline-
sensitive matching OFF), ClickHouse RE2 default is NOT (`.` excludes newline).
For Q29's pattern '^https?://(?:www\.)?([^/]+)/.*$', a Referer with an embedded
newline (\r\n\t) matches in PG (extracts host 'gadgets.irr.ru') but FAILS in RE2
(returns the string unchanged), so those rows land in different GROUP BY groups ->
per-group count drift ~1e-4 (e.g. gadgets.irr.ru pg=223329 vs ch=223307, 22 rows).

Evidence:
- PG: regexp_replace(E'aXb\nc/d','^(a[^/]+)/.*$','\1') matched across the newline.
- CH default replaceRegexpOne(newline-referer, pat) = original; with (?s) = 'gadgets.irr.ru'.
- CH with (?s) maps exactly 223329 rows to gadgets.irr.ru == PG's count.
- The 22 divergent rows are all Referers with embedded \r\n\t (malformed/concatenated URLs).

Fix: deparse regexp_replace's pattern as concat('(?s)', pattern) (RE2 dotall) in the
no-flags path (src/deparse.c CF_REPLACE_REGEX), aligning RE2 with PG's default. RE2
^/$ already match PG's default (string anchors, not multiline), so only dotall needed.

Result: Q29 native vs offload (deterministic tiebreak) = exact|15|15|0|0|0|0
(count column now bit-exact; avg(length) Float64 within 1e-6). Dispatched SQL:
replaceRegexpOne(referer, concat('(?s)', '^https?://...'), '\1').
