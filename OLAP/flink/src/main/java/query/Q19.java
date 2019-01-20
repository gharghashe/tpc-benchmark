package query;

import main.BaseQuery;
import main.QueryImplementation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroOutputFormat;
import org.apache.flink.table.api.Table;
import result.R19;
import table.Lineitem;
import table.Part;


public class Q19 extends BaseQuery {

    @Override
    public String getQueryName() {
        return "q19";
    }

    @Override
    public Integer approxTime() {
        return null;
    }

    @Override
    public QueryImplementation queryImplementation() {
        return () -> {
            Table lineitem = Lineitem.getTable(executionEnvironment, batchTableEnvironment);
            Table part = Part.getTable(executionEnvironment, batchTableEnvironment);

            Table temp = part.join(lineitem).where("P_PARTKEY == L_PARTKEY")
                    .filter(" ( L_SHIPMODE.like('%AIR%') || L_SHIPMODE.like('%AIR REG%') ) && (L_SHIPINSTRUCT.like('%DELIVER IN PERSON%'))");

            Table result = temp.filter(
                    "((P_BRAND.like('%Brand#51%')) && P_CONTAINER.in('SM CASE','SM BOX','SM PACK','SM PKG')  && (L_QUANTITY >= 8) && (L_QUANTITY <= 18) &&  (P_SIZE >= 1) && (P_SIZE <= 10)) " +
                            "|| ((P_BRAND.like('%Brand#22%')) && P_CONTAINER.in('MED BAG','MED BOX','MED PKG','MED PACK')  && (L_QUANTITY >= 3) && (L_QUANTITY <= 23) &&  (P_SIZE >= 1) && (P_SIZE <= 5))  " +
                            "|| ((P_BRAND.like('%Brand#51%')) && P_CONTAINER.in('LG CASE','LG BOX','LG PACK','LG PKG')  && (L_QUANTITY >= 22) && (L_QUANTITY <= 32) &&  (P_SIZE >= 1) && (P_SIZE <= 15))")
                    .select("(L_EXTENDEDPRICE*(1-L_DISCOUNT)) as VOLUME").select("VOLUME.sum as REVENUE");

            DataSet<R19> dataSet = batchTableEnvironment.toDataSet(result, R19.class);

            dataSet.output(new AvroOutputFormat<>(new Path("hdfs://namenode:8020/flink/result/r19.res"), R19.class));
        };
    }
}
