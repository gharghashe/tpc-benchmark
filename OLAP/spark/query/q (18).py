l = lineitem.groupBy('L_ORDERKEY').agg(func.sum('L_QUANTITY').alias("SUM_QUANTITY")).filter(
    'sum_quantity > 309').select(lineitem.L_ORDERKEY.alias("KEY"), 'SUM_QUANTITY')
lo = l.join(orders, l.KEY == orders.O_ORDERKEY)
temp = lo.join(lineitem, lineitem.L_ORDERKEY == lo.KEY)
result = temp.join(customer, customer.C_CUSTKEY == temp.O_CUSTKEY).select('L_QUANTITY', 'C_NAME', 'C_CUSTKEY',
                                                                          'O_ORDERKEY', 'O_ORDERDATE',
                                                                          'O_TOTALPRICE').groupBy('C_NAME', 'C_CUSTKEY',
                                                                                                  'O_ORDERKEY',
                                                                                                  'O_ORDERDATE',
                                                                                                  'O_TOTALPRICE').agg(
    func.sum('L_QUANTITY')).sort(func.desc("O_TOTALPRICE"), 'O_ORDERDATE').limit(100)
result.show()
