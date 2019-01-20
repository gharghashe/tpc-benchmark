l = lineitem.filter("l_shipdate >= '1997-05-01'" and "l_shipdate < '1997-08-01'").select('L_SUPPKEY', (
        lineitem.L_EXTENDEDPRICE * (1 - lineitem.L_DISCOUNT)).alias("VALUE"))
total = l.groupBy("L_SUPPKEY").agg(func.sum("VALUE").alias("TOTAL"))
temp = total.agg(func.max("TOTAL").alias("MAX_TOTAL")).collect()
max = temp[0].asDict()["MAX_TOTAL"]
result = total.filter(total.TOTAL == max).join(supplier, supplier.S_SUPPKEY == total.L_SUPPKEY).select('S_SUPPKEY',
                                                                                                       'S_NAME',
                                                                                                       'S_ADDRESS',
                                                                                                       'S_PHONE',
                                                                                                       'TOTAL').sort(
    'S_SUPPKEY')
result.show()