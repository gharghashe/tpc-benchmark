def isAmericaCountry(x, y):
    if "AMERICA" in x:
        return float(y)
    else:
        return float(0)


funcIsAmericaCountry = func.udf(isAmericaCountry, FloatType())

r = region.filter("r_name = 'AMERICA' ")
o = orders.filter("o_orderdate >= '1995-01-01' ").filter("o_orderdate < '1996-12-31'")
p = part.filter("p_type == 'ECONOMY ANODIZED STEEL'")

ns = nation.join(supplier, supplier.S_NATIONKEY == nation.N_NATIONKEY)
lp = lineitem.join(p, lineitem.L_PARTKEY == p.P_PARTKEY)
lpns = lp.join(ns, lp.L_SUPPKEY == ns.S_SUPPKEY).select('L_PARTKEY', 'L_SUPPKEY', 'L_ORDERKEY',
                                                        (l_p.L_EXTENDEDPRICE * (1 - l_p.L_DISCOUNT)).alias("VOLUME"))
nr = nation.join(r, nation.N_REGIONKEY == r.R_REGIONKEY).select('N_NATIONKEY', 'N_NAME')
nrc = nr.join(customer, nr.N_NATIONKEY == customer.C_NATIONKEY).select('C_CUSTKEY', 'N_NAME')
nrco = nrc.join(o, nrc.C_CUSTKEY == o.O_CUSTKEY).select('O_ORDERKEY', 'O_ORDERDATE', 'N_NAME')

temp = nrco.join(lpns, nrco.O_ORDERKEY == lpns.L_ORDERKEY)
result1 = temp.select(func.year("O_ORDERDATE").alias("o_year"), 'VOLUME',
                      funcIsAmericaCountry(temp.N_NAME, temp.VOLUME).alias("AMERICA_VOLUME"))
result2 = result1.groupBy('o_year').agg(func.sum(result1.VOLUME).alias("TOTAL_VOLUME")).orderBy('o_year').select(
    result1.o_year.alias("o_year2"), 'TOTAL_VOLUME')
result3 = result1.groupBy('o_year').agg(func.sum(result1.AMERICA_VOLUME).alias("TOTAL_AMERICA")).orderBy(
    'o_year').select(result1.o_year.alias("o_year3"), 'TOTAL_AMERICA')

final_result = result3.join(result2, result2.o_year2 == result3.o_year3).dropDuplicates().select(
    result3.o_year3.alias("o_year"), (result3.TOTAL_AMERICA / result2.TOTAL_VOLUME).alias("mkt_share"))
final_result.show()
