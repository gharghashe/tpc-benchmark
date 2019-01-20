create view store.revenue0 (supplier_no, total_revenue) as
	select
		l_suppkey,
		sum(l_extendedprice * (1 - l_discount))
	from
		store.lineitem
	where l_shipdate >= date '1997-05-01'
		and l_shipdate < date '1997-05-01' + interval '3' month
	group by
		l_suppkey;

EXPLAIN (ANALYZE true,VERBOSE true,COSTS true,BUFFERS true,TIMING false,FORMAT JSON)
select
	s_suppkey,
	s_name,
	s_address,
	s_phone,
	total_revenue
from
	store.supplier,
	store.revenue0
where s_suppkey = supplier_no
	and total_revenue = (
		select
			max(total_revenue)
		from
			store.revenue0
	)
order by
	s_suppkey;

drop view revenue0;