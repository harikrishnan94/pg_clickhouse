EXPLAIN (ANALYZE, COSTS)
SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits;
