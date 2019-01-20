sizes = [43, 20, 12, 5, 41, 6, 21, 40]
p = part.filter("p_brand != 'Brand#31' " and "p_type not like 'LARGE PLATED%'" and part.P_SIZE.isin(sizes)).select(
    'P_PARTKEY', 'P_BRAND', 'P_TYPE', 'P_SIZE')
sps = supplier.filter("s_comment like '%Customer%Complaints%' ").join(partsupp,
                                                                      supplier.S_SUPPKEY == partsupp.PS_SUPPKEY)
result = sps.join(p, sps.PS_PARTKEY == p.P_PARTKEY).groupBy('P_BRAND', 'P_TYPE', 'P_SIZE').agg(
    func.count('PS_SUPPKEY').alias("SUPPLIER_COUNT")).sort(func.desc("SUPPLIER_COUNT"), 'P_BRAND', 'P_TYPE', 'P_SIZE')
result.show()