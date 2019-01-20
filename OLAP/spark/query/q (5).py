r = region.filter("r_name = 'AFRICA'")
nr = r.join(nation,r.R_REGIONKEY == nation.N_REGIONKEY)
nrs = nr.join(supplier,nr.N_NATIONKEY == supplier.S_NATIONKEY)
nrsl = nrs.join(lineitem,nrs.S_SUPPKEY == lineitem.L_SUPPKEY).select('N_NAME','L_EXTENDEDPRICE','L_DISCOUNT','L_ORDERKEY')

o = orders.filter("o_orderdate >= '1995-01-01' ").filter("o_orderdate < '1996-01-01'")
co = customer.join(o,customer.C_CUSTKEY == o.O_CUSTKEY).select('O_ORDERKEY')
temp = nrsl.join(co,nrsl.L_ORDERKEY == co.O_ORDERKEY)
result = temp.groupBy('N_NAME').agg(func.sum(temp.L_EXTENDEDPRICE *(1 -temp.L_DISCOUNT)).alias("revenue")).sort(func.desc("revenue"))

result.show()


