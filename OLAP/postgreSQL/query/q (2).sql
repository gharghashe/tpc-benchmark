EXPLAIN (ANALYZE true,VERBOSE true,COSTS true,BUFFERS true,TIMING false,FORMAT JSON)
select
	s_acctbal,
	s_name,
	n_name,
	p_partkey,
	p_mfgr,
	s_address,
	s_phone,
	s_comment
from
	store.part,
	store.supplier,
	store.partsupp,
	store.nation,
	store.region
where p_partkey = ps_partkey
	and s_suppkey = ps_suppkey
	and p_size = 7
	and p_type like '%BURNISHED'
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = 'AFRICA'
	and ps_supplycost = (
		select
			min(ps_supplycost)
		from
			store.partsupp,
			store.supplier,
			store.nation,
			store.region
		where p_partkey = ps_partkey
			and s_suppkey = ps_suppkey
			and s_nationkey = n_nationkey
			and n_regionkey = r_regionkey
			and r_name = 'AFRICA'
	)
order by
	s_acctbal desc,
	n_name,
	s_name,
	p_partkey
LIMIT 100;