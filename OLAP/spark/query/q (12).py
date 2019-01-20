def isHighPriority(x):
    if "2-HIGH" in x or "1-URGENT" in x:
        return 1
    else:
        return 0


def isLowPriority(x):
    if "2-HIGH" in x or "1-URGENT" in x:
        return 0
    else:
        return 1


funcIsHighPriority = func.udf(isHighPriority, IntegerType())
funcIsLowPriority = func.udf(isLowPriority, IntegerType())

l = lineitem.filter((
                                "l_shipmode = 'MAIL' " or "l_shipmode = 'SHIP' ") and 'l_commitdate < l_receiptdate' and 'l_shipdate < l_commitdate' and "l_receiptdate >= '1997-01-01' " and " l_receiptdate <= '1998-01-01' ")
temp = l.join(orders, l.L_ORDERKEY == orders.O_ORDERKEY).select('L_SHIPMODE', 'O_ORDERPRIORITY')
result = temp.groupBy('L_SHIPMODE').agg(
    func.sum(funcIsHighPriority(temp.O_ORDERPRIORITY).alias("SUM_HIGHORDERPRIORITY")),
    func.sum(funcIsLowPriority(temp.O_ORDERPRIORITY).alias("SUM_LOWORDERPRIORITY"))).sort("L_SHIPMODE")

result.show()
