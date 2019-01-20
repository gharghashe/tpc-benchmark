EXPLAIN (ANALYZE true,VERBOSE true,COSTS true,BUFFERS true,TIMING false,FORMAT JSON)
select
	cntrycode,
	count(*) as numcust,
	sum(c_acctbal) as totacctbal
from
	(
		select
			substring(c_phone from 1 for 2) as cntrycode,
			c_acctbal
		from
			store.customer
		where substring(c_phone from 1 for 2) in
				('28', '43', '22', '39', '31', '30', '41')
			and c_acctbal > (
				select
					avg(c_acctbal)
				from
					store.customer
				where c_acctbal > 0.00
					and substring(c_phone from 1 for 2) in
						('28', '43', '22', '39', '31', '30', '41')
			)
			and not exists (
				select
					*
				from
					store.orders
				where o_custkey = c_custkey
			)
	) as custsale
group by
	cntrycode
order by
	cntrycode;