SET search_path=pg;
SET max_parallel_workers_per_gather=8;
SET statement_timeout='300s';
select 100.00 * sum(case when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount) else 0 end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from lineitem, part
where l_partkey = p_partkey and l_shipdate >= date '1995-09-01' and l_shipdate < date(date '1995-09-01' + interval '1 month');
