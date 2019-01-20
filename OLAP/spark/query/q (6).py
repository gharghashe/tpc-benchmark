l = lineitem.filter("l_shipdate >= '1996-01-01' ").filter("l_shipdate < '1997-01-01' ").filter(
    'l_discount >= 0.02').filter('l_discount <= 0.04').filter('l_quantity < 24')
result = l.agg(func.sum(l.L_EXTENDEDPRICE * (l.L_DISCOUNT)).alias("revenue"))
result.show()
