result = lineitem.filter("l_shipdate <= '1998-12-01' ").groupBy('l_returnflag', 'l_linestatus').agg(
    func.sum('l_quantity').alias('sum_qty'),
    func.sum(lineitem.L_EXTENDEDPRICE * (1 - lineitem.L_DISCOUNT)).alias('sum_disc_price'),
    func.sum('l_extendedprice').alias('sum_base_price'),
    func.sum(lineitem.L_EXTENDEDPRICE * (1 - lineitem.L_DISCOUNT) * (1 + lineitem.L_TAX)).alias('sum_charge'),
    func.avg('l_quantity').alias('avg_qty'), func.avg('l_extendedprice').alias('avg_price'),
    func.avg('l_discount').alias('avg_disc')).sort('l_returnflag', 'l_linestatus')
result.show()