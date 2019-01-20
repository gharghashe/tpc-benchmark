l = lineitem.select('L_PARTKEY', 'L_QUANTITY', 'L_EXTENDEDPRICE')
p = part.filter("p_brand like '%Brand#13%' " and "p_container like '%WRAP PKG%' ")
pl = p.join(l, p.P_PARTKEY == l.L_PARTKEY, "left_outer")

temp = pl.groupBy("P_PARTKEY").agg(func.avg(pl.L_QUANTITY * 0.2).alias("AVG_QUANTITY")).select(
    pl.P_PARTKEY.alias("KEY"), 'AVG_QUANTITY')
temp2 = temp.join(pl, temp.KEY == pl.P_PARTKEY).select('AVG_QUANTITY', 'KEY', 'L_QUANTITY', 'L_EXTENDEDPRICE')

result = p_temp.join(temp2, temp2.KEY == p_temp.P_PARTKEY).filter(temp2.L_QUANTITY < temp2.AVG_QUANTITY).agg(
    (func.sum(temp2.L_EXTENDEDPRICE) / 7.0).alias("AVG_YEARLY"))
result.show()
