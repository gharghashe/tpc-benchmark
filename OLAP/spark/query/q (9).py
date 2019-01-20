p = part.filter("p_name like '%blue%'")
lp = p.join(lineitem, p.P_PARTKEY == lineitem.L_PARTKEY)
ns = nation.join(supplier, nation.N_NATIONKEY == supplier.S_NATIONKEY)
lps = lp.join(ns, lp.L_SUPPKEY == ns.S_SUPPKEY)
lpsps = lps.join(partsupp, lps.L_SUPPKEY == partsupp.PS_SUPPKEY)
temp = lpsps.join(orders, lpsps.L_ORDERKEY == orders.O_ORDERKEY)
profit = temp.select(temp.N_NAME, func.year(temp.O_ORDERDATE).alias("O_YEAR"),
                     (temp.L_EXTENDEDPRICE * (1 - temp.L_DISCOUNT) - temp.PS_SUPPLYCOST * temp.L_QUANTITY).alias(
                         "AMOUNT"))
result = profit.groupBy(profit.N_NAME, profit.O_YEAR).agg(func.sum(profit.AMOUNT).alias("SUM_PROFIT")).orderBy(
    temp.N_NAME, func.desc("O_YEAR"))
result.show()
