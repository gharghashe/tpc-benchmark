l = lineitem.filter("l_returnflag == 'R' ")
o = orders.filter("o_orderdate >= '1994-11-01' ").filter("o_orderdate < '1995-02-01'")
oc = o.join(customer, o.O_CUSTKEY == customer.C_CUSTKEY)
ocn = oc.join(nation, oc.C_NATIONKEY == nation.N_NATIONKEY)

temp = ocn.join(l, l.L_ORDERKEY == ocn.O_ORDERKEY)
temp2 = temp.select('C_CUSTKEY', 'C_NAME', (temp.L_EXTENDEDPRICE * (1 - temp.L_DISCOUNT)).alias("VOLUME"), 'C_ACCTBAL',
                    'N_NAME', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT', 'C_PHONE')
result = temp2.groupBy('C_CUSTKEY', 'C_NAME', 'C_ACCTBAL', 'N_NAME', 'C_ADDRESS', 'C_COMMENT', 'C_PHONE').agg(
    func.sum("VOLUME").alias("REVENUE")).limit(10)
result.show()
