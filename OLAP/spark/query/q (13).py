o = orders.filter(" o_comment not like '%pending%packages%' ")
co = customer.join(o, customer.C_CUSTKEY == o.O_CUSTKEY, "left_outer")
coo = co.select('O_CUSTKEY', co.O_ORDERKEY.alias("O_ORDERKEY2"))

temp = co.groupBy('O_ORDERKEY').agg(func.count('O_ORDERKEY').alias("C_COUNT"))
result = temp.join(coo, coo.O_ORDERKEY2 == temp.O_ORDERKEY).select('C_COUNT', 'O_CUSTKEY', 'O_ORDERKEY').groupBy(
    'C_COUNT').agg(func.count('O_CUSTKEY').alias("CUSTDIST")).sort(func.desc('CUSTDIST'), func.desc('C_COUNT'))
result.show()
