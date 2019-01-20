EXPLAIN (ANALYZE true,VERBOSE true,COSTS true,BUFFERS true,TIMING false,FORMAT JSON)
select
	sum(l_extendedprice) / 7.0 as avg_yearly
from
	store.lineitem,
	store.part,
	(SELECT l_partkey AS agg_partkey, 0.2 * avg(l_quantity) AS avg_quantity FROM store.lineitem GROUP BY l_partkey) part_agg
where p_partkey = l_partkey
	and agg_partkey = l_partkey
	and p_brand = 'Brand#13'
	and p_container = 'WRAP PKG'
	and l_quantity < avg_quantity;