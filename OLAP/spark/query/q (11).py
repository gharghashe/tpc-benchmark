n = nation.filter("n_name like '%ARGENTINA%' ")
ns = n.join(supplier, nation.N_NATIONKEY == supplier.S_NATIONKEY).select('S_SUPPKEY')
nsps = ns.join(partsupp, ns.S_SUPPKEY == partsupp.PS_SUPPKEY).select('PS_PARTKEY', (
        partsupp.PS_SUPPLYCOST * partsupp.PS_AVAILQTY).alias("VALUE"))

sum = nsps.agg(func.sum("VALUE").alias("TOTAL_VALUE"))
temp = nsps.groupBy('PS_PARTKEY').agg(func.sum("VALUE").alias("PART_VALUE"))
result = temp.crossJoin(sum).filter(temp.PART_VALUE > (sum.TOTAL_VALUE * 0.0001)).sort(func.desc("PART_VALUE"))
result.show()
