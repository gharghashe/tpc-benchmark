EXPLAIN (ANALYZE true,VERBOSE true,COSTS true,BUFFERS true,TIMING false,FORMAT JSON)
select
	n_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue
from
	store.customer,
	store.orders,
	store.lineitem,
	store.supplier,
	store.nation,
	store.region
where c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and l_suppkey = s_suppkey
	and c_nationkey = s_nationkey
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = 'AFRICA'
	and o_orderdate >= date '1996-01-01'
	and o_orderdate < date '1996-01-01' + interval '1' year
group by
	n_name
order by
	revenue desc;