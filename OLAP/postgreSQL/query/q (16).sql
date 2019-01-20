EXPLAIN (ANALYZE true,VERBOSE true,COSTS true,BUFFERS true,TIMING false,FORMAT JSON)
select
	p_brand,
	p_type,
	p_size,
	count(distinct ps_suppkey) as supplier_cnt
from
	store.partsupp,
	store.part
where p_partkey = ps_partkey
	and p_brand <> 'Brand#31'
	and p_type not like 'LARGE PLATED%'
	and p_size in (43, 20, 12, 5, 41, 6, 21, 40)
	and ps_suppkey not in (
		select
			s_suppkey
		from
			store.supplier
		where s_comment like '%Customer%Complaints%'
	)
group by
	p_brand,
	p_type,
	p_size
order by
	supplier_cnt desc,
	p_brand,
	p_type,
	p_size;