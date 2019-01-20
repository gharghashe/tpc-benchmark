c = customer.filter("c_mktsegment = 'MACHINERY'")
l = lineitem.filter("l_shipdate > '1995-03-25' ")
o = orders.filter("o_orderdate < '1995-03-25' ")

lo = l.join(o, l.L_ORDERKEY == o.O_ORDERKEY)
co = c.join(lo, c.C_CUSTKEY == lo.O_CUSTKEY)

temp = co.select(co.O_ORDERKEY, co.O_ORDERDATE, co.O_SHIPPRIORITY).join(l, co.O_ORDERKEY == l.L_ORDERKEY).select(
    co.O_ORDERKEY, co.O_ORDERDATE, co.O_SHIPPRIORITY, l.L_ORDERKEY, co.O_ORDERKEY, co.O_ORDERDATE,
    l.L_EXTENDEDPRICE, l.L_DISCOUNT)
result = temp.groupBy('l_orderkey', 'o_orderdate', 'o_shippriority').agg(
    func.sum(temp.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)).alias("revenue")).sort(temp.O_ORDERDATE, func.desc("revenue"))
result.show()
