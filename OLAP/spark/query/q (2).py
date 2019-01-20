r = region.filter("r_name = 'AFRICA' ")
nr = nation.join(r, nation.N_REGIONKEY == r.R_REGIONKEY)
nrs1 = nr.join(supplier, supplier.S_NATIONKEY == nr.N_NATIONKEY)
min_supp = nrs1.join(partsupp, partsupp.PS_SUPPKEY == nrs1.S_SUPPKEY).join(part,
                                                                           part.P_PARTKEY == partsupp.PS_PARTKEY).agg(
    func.min('ps_supplycost')).collect()

min = min_supp[0].asDict()["min(ps_supplycost)"]
ps = partsupp.filter(partsupp.PS_SUPPLYCOST == min)
nrs2 = nr.join(supplier, nr.N_NATIONKEY == supplier.S_NATIONKEY)
nrspps = nrs2.join(ps, ps.PS_SUPPKEY == nrs2.S_SUPPKEY)

temp = part.filter('p_size = 7').filter("p_type like '%BURNISHED'")
result = nrspps.join(temp, temp.P_PARTKEY == nrspps.PS_PARTKEY).select('s_acctbresult', 's_name', 'n_name', 'p_partkey',
                                                                       'p_mfgr', 's_address', 's_phone',
                                                                       's_comment').orderBy(func.desc("s_acctbresult"),
                                                                                            'n_name', 's_name',
                                                                                            'p_partkey').limit(100)
result.show()
