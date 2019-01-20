l = lineitem.filter("l_shipdate >= '1994-01-01' " and "l_shipdate < '1995-01-01' ").groupBy('L_PARTKEY',
                                                                                            'L_SUPPKEY').agg(
    func.sum(lineitem.L_QUANTITY * 0.5).alias("SUM_QUANTITY"))
n = nation.filter("n_name like '%INDIA%' ")
n_s = supplier.select('S_SUPPKEY', 'S_NAME', 'S_NATIONKEY', 'S_ADDRESS').join(n, supplier.S_NATIONKEY == n.N_NATIONKEY)
p = part.filter(part.P_NAME.startswith("green")).select('P_PARTKEY')
pps = p.join(partsupp, part.P_PARTKEY == partsupp.PS_PARTKEY)

temp = pps.join(l, l.L_SUPPKEY == pps.PS_SUPPKEY).filter(pps.PS_AVAILQTY > l.SUM_QUANTITY).select(pps.PS_SUPPKEY)
result = temp.join(n_s, n_s.S_SUPPKEY == temp.PS_SUPPKEY).select(n_s.S_NAME, n_s.S_ADDRESS).sort(n_s.S_NAME)
result.show()
