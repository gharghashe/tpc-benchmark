EXPLAIN (ANALYZE true,VERBOSE true,COSTS true,BUFFERS true,TIMING false,FORMAT JSON)
select
	100.00 * sum(case
		when p_type like 'BRUSHED%'
			then l_extendedprice * (1 - l_discount)
		else 0
	end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
	store.lineitem,
	store.part
where l_partkey = p_partkey
	and l_shipdate >= date '1994-03-01'
	and l_shipdate < date '1994-03-01' + interval '1' month;