s = supplier.select('S_SUPPKEY', 'S_NATIONKEY', 'S_NAME')
n = nation.filter("n_name like '%INDIA%' ")
l = lineitem.select('L_SUPPKEY', 'L_ORDERKEY', 'L_RECEIPTDATE', 'L_COMMITDATE')
l2 = l.filter('l_receiptdate > l_commitdate')
o = orders.select('O_ORDERKEY', 'O_ORDERSTATUS').filter("O_ORDERSTATUS = 'F' ")

temp1 = l.groupBy(l.L_ORDERKEY).agg(func.countDistinct(l.L_SUPPKEY).alias("SUPPKEY_COUNT"),
                                 func.max(l.L_SUPPKEY).alias("SUPPKEY_MAX")).select(l.L_ORDERKEY.alias("KEY"),
                                                                                    "SUPPKEY_COUNT", "SUPPKEY_MAX")

temp2 = l2.groupBy(l.L_ORDERKEY).agg(func.countDistinct(l.L_SUPPKEY).alias("SUPPKEY_COUNT"),
                                  func.max(l.L_SUPPKEY).alias("SUPPKEY_MAX")).select(l.L_ORDERKEY.alias("KEY"),
                                                                                     "SUPPKEY_COUNT", "SUPPKEY_MAX")

ns = n.join(s, s.S_NATIONKEY == n.N_NATIONKEY)
nsl = ns.join(l2, ns.S_SUPPKEY == l2.L_SUPPKEY)
tnslo = nsl.join(o, o.O_ORDERKEY == nsl.L_ORDERKEY)
temp = tnslo.join(temp1, temp1.KEY == tnslo.L_ORDERKEY).filter(
    (temp1.SUPPKEY_COUNT > 1) | (temp1.SUPPKEY_COUNT == 1) & (tnslo.L_SUPPKEY == temp1.SUPPKEY_MAX)).select(tnslo.S_NAME,
                                                                                                   tnslo.L_ORDERKEY,
                                                                                                   tnslo.L_SUPPKEY)

result = temp.join(temp2, temp.L_ORDERKEY == temp2.KEY, "left_outer").select(temp.S_NAME, temp.L_ORDERKEY, temp.L_SUPPKEY,
                                                                       temp2.SUPPKEY_COUNT, temp2.SUPPKEY_MAX).filter(
    (temp2.SUPPKEY_COUNT == 1) & (temp.L_SUPPKEY == temp2.SUPPKEY_MAX)).groupBy(temp.S_NAME).agg(
    func.count(temp.L_SUPPKEY).alias("NUM_WAIT")).sort(func.desc("NUM_WAIT"), temp.S_NAME)
result.show()
