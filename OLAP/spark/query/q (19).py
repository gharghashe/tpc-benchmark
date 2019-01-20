c1 = ["SM CASE", "SM BOX", "SM PACK", "SM PKG"]
c2 = ["MED BAG", "MED BOX", "MED PKG", "MED PACK"]
c3 = ["LG CASE", "LG BOX", "LG PACK", "LG PKG"]

temp = part.join(lineitem, part.P_PARTKEY == lineitem.L_PARTKEY).filter(
    ("l_shipmode like '%AIR%' " or "l_shipmode like '%AIR REG%' ") and "l_shipinstruct like '%DELIVER IN PERSON%'")

result = temp.filter(((temp.P_BRAND.like("%Brand#54%")) & (temp.P_CONTAINER.isin(c1)) & (temp.L_QUANTITY >= 8) & (
            temp.L_QUANTITY <= 18) & (temp.P_SIZE >= 1) & (temp.P_SIZE <= 10)) | (
                                 (temp.P_BRAND.like("%Brand#22%")) & (temp.P_CONTAINER.isin(c2)) & (
                                     temp.L_QUANTITY >= 3) & (temp.L_QUANTITY <= 23) & (temp.P_SIZE >= 1) & (
                                             temp.P_SIZE <= 5)) | (
                                 (temp.P_BRAND.like("%Brand#51%")) & (temp.P_CONTAINER.isin(c3)) & (
                                     temp.L_QUANTITY >= 22) & (temp.L_QUANTITY <= 32) & (temp.P_SIZE >= 1) & (
                                             temp.P_SIZE <= 15))).select(
    (temp.L_EXTENDEDPRICE * (1 - temp.L_DISCOUNT)).alias("VOLUME")).agg(func.sum("VOLUME"))
result.show()
