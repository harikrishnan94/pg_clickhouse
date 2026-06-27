EXPLAIN (ANALYZE, COSTS)
SELECT sum(l_quantity), sum(l_extendedprice), sum(l_discount), sum(l_tax), count(*) FROM lineitem;
