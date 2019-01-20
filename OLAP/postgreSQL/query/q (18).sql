EXPLAIN (ANALYZE true,VERBOSE true,COSTS true,BUFFERS true,TIMING false,FORMAT JSON)
select
	c_name,
	c_custkey,
	o_orderkey,
	o_orderdate,
	o_totalprice,
	sum(l_quantity)
from
	store.customer,
	store.orders,
	store.lineitem
where o_orderkey in (
		select
			l_orderkey
		from
			store.lineitem
		group by
			l_orderkey having
				sum(l_quantity) > 309
	)
	and c_custkey = o_custkey
	and o_orderkey = l_orderkey
group by
	c_name,
	c_custkey,
	o_orderkey,
	o_orderdate,
	o_totalprice
order by
	o_totalprice desc,
	o_orderdate
LIMIT 100;