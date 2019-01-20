n = nation.filter("n_name  like 'ETHIOPIA%' OR  n_name  like 'JAPAN%' ")
l = lineitem.filter("l_shipdate >= '1995-01-01' AND l_shipdate < '1996-12-31'")
ns = n.join(supplier, n.N_NATIONKEY == supplier.S_NATIONKEY).join(l,
                                                                  supplier.S_SUPPKEY == l.L_SUPPKEY).withColumnRenamed(
    "n_name", "supp_nation").select('supp_nation', 'l_orderkey', 'l_extendedprice', 'l_discount', 'l_shipdate')
temp = n.join(customer, n.N_NATIONKEY == customer.C_NATIONKEY).join(orders,
                                                                    customer.C_CUSTKEY == orders.O_CUSTKEY).withColumnRenamed(
    "n_name", "cust_nation").select('cust_nation', 'o_orderkey').join(ns, orders.O_ORDERKEY == ns.l_orderkey).filter(
    ("supp_nation like 'JAPAN%'" and "cust_nation  like 'ETHIOPIA%'") or (
            "supp_nation  like 'ETHIOPIA%'" and "cust_nation  like 'JAPAN%'"))
result = temp.select('supp_nation', 'cust_nation', func.year("l_shipdate").alias("l_year"), 'l_discount',
                     'l_extendedprice').groupBy('supp_nation', 'cust_nation', 'l_year').agg(
    func.sum(temp.l_extendedprice * (1 - temp.l_discount)).alias("revenue")).orderBy('supp_nation', 'cust_nation',
                                                                                     'l_year')
result.show()
