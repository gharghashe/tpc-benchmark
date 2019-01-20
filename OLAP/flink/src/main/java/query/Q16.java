package query;

import main.BaseQuery;
import main.QueryImplementation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroOutputFormat;
import org.apache.flink.table.api.Table;
import result.R16;
import table.Part;
import table.PartSupp;
import table.Supplier;


public class Q16 extends BaseQuery {

    @Override
    public String getQueryName() {
        return "q16";
    }

    @Override
    public Integer approxTime() {
        return null;
    }

    @Override
    public QueryImplementation queryImplementation() {
        return () -> {
            Table supplier = Supplier.getTable(executionEnvironment, batchTableEnvironment);
            Table partsupp = PartSupp.getTable(executionEnvironment, batchTableEnvironment);
            Table part = Part.getTable(executionEnvironment, batchTableEnvironment);
            String size = "(43, 20, 12, 5, 41, 6, 21, 40)";

            Table p = part.filter("P_BRAND != 'Brand#31'&& !P_TYPE.like('LARGE PLATED%') && P_SIZE.in" + size)
                    .select("P_PARTKEY,P_BRAND,P_TYPE,P_SIZE");
            Table temp = supplier.filter("S_COMMENT.like('%Customer%Complaints%')")
                    .join(partsupp).where("S_SUPPKEY == PS_SUPPKEY");

            Table result = temp.join(p).where("PS_PARTKEY == P_PARTKEY")
                    .groupBy("P_BRAND,P_TYPE,P_SIZE")
                    .select(" PS_SUPPKEY.count as SUPPLIER_COUNT,P_BRAND,P_TYPE,P_SIZE")
                    .orderBy("SUPPLIER_COUNT.desc,P_BRAND,P_TYPE,P_SIZE");

            DataSet<R16> dataSet = batchTableEnvironment.toDataSet(result, R16.class);

            dataSet.output(new AvroOutputFormat<>(new Path("hdfs://namenode:8020/flink/result/r16.res"), R16.class));
        };
    }
}
