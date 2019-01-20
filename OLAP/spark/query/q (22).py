codes = ["28", "43", "22", "39", "31", "30", "41"]
c1 = customer.select('C_ACCTBAL', 'C_CUSTKEY', customer.C_PHONE.substr(1, 2).alias("COUNTRY_CODE"))
c2 = c1.filter(c1.COUNTRY_CODE.isin(codes))
avg = c2.filter('c_acctbal > 0.0 ').agg(func.avg(c2.C_ACCTBAL).alias("AVG_ACCTBAL"))
temp = orders.select('O_CUSTKEY').join(c2, orders.O_CUSTKEY == c2.C_CUSTKEY, "right_outer")
result = temp.join(avg).filter(temp.C_ACCTBAL > avg.AVG_ACCTBAL).groupBy(temp.COUNTRY_CODE).agg(
    func.count(temp.C_ACCTBAL), func.sum(temp.C_ACCTBAL)).sort(temp.COUNTRY_CODE)
result.show()
