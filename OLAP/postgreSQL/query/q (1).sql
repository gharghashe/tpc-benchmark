EXPLAIN (ANALYZE true,VERBOSE true,COSTS true,BUFFERS true,TIMING false,FORMAT JSON)
select
	l.l_returnflag,
	l.l_linestatus,
	sum(l.l_quantity) as sum_qty,
	sum(l.l_extendedprice) as sum_base_price,
	sum(l.l_extendedprice * (1 - l.l_discount)) as sum_disc_price,
	sum(l.l_extendedprice * (1 - l.l_discount) * (1 + l.l_tax)) as sum_charge,
	avg(l.l_quantity) as avg_qty,
	avg(l.l_extendedprice) as avg_price,
	avg(l.l_discount) as avg_disc,
	count(*) as count_order
from
	store.lineitem l
where
	l.l_shipdate <= date '1998-12-01' - interval '69' day
group by
	l.l_returnflag,
	l.l_linestatus
order by
	l.l_returnflag,
	l.l_linestatus;