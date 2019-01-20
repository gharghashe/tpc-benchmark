o = orders.filter("o_orderdate >= '1996-11-01' ").filter("o_orderdate < '1997-02-01'")
l = lineitem.filter('l_commitdate < l_receiptdate')
result = l.join(o, l.L_ORDERKEY == o.O_ORDERKEY).groupBy('o_orderpriority').agg(
    func.count('*').alias("order_count")).select(o.O_ORDERPRIORITY, 'order_count').orderBy('o_orderpriority')
result.show()
