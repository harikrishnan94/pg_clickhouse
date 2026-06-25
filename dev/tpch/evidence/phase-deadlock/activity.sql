SELECT query_id, round(elapsed,1) AS el, substring(query,1,70) AS q
FROM system.processes
WHERE query NOT LIKE '%system.proc%'
FORMAT TSV
