def startWithBrushed(x, y):
    if x.startswith("BRUSHED"):
        return float(y)
    else:
        return float(0)


funcStartWithBrushed = func.udf(startWithBrushed, FloatType())

l = lineitem.filter("l_shipdate>='1996-12-01'" and "l_shipdate<'1997-01-01'")
lp = part.join(lineitem, part.P_PARTKEY == l.lpARTKEY)

temp = lp.select('P_TYPE', (lp.L_EXTENDEDPRICE * (1 - lp.L_DISCOUNT)).alias("VALUE"))
temp2 = temp.agg(func.sum("VALUE").alias("TOTAL_VALUE")).collect()
total = temp2[0].asDict()["TOTAL_VALUE"]

result = temp.agg(func.sum(funcStartWithBrushed(temp.P_TYPE, temp.VALUE)) * 100 / total)
result.show()
