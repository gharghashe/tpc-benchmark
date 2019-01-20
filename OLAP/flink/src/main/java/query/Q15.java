package query;

import main.BaseQuery;
import main.QueryImplementation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroOutputFormat;
import org.apache.flink.table.api.Table;
import result.R15;
import table.Lineitem;
import table.Supplier;


public class Q15 extends BaseQuery {

    @Override
    public String getQueryName() {
        return "q15";
    }

    @Override
    public Integer approxTime() {
        return null;
    }

    @Override
    public QueryImplementation queryImplementation() {
        return () -> {
            Table lineitem = Lineitem.getTable(executionEnvironment, batchTableEnvironment);
            Table supplier = Supplier.getTable(executionEnvironment, batchTableEnvironment);

            Table l = lineitem.filter("L_SHIPDATE >= '1997-05-01' && L_SHIPDATE < '1997-08-01'")
                    .select("L_SUPPKEY,(L_EXTENDEDPRICE*(1-L_DISCOUNT)) as VALUE");

            Table revenue = l.groupBy("L_SUPPKEY")
                    .select("VALUE.sum as TOTAL,L_SUPPKEY");
            Float max = batchTableEnvironment.toDataSet(revenue.select("TOTAL.max as MAX_TOTAL"), Float.class).collect().get(0);

            Table result = revenue.filter("TOTAL == " + max)
                    .join(supplier).where("S_SUPPKEY == L_SUPPKEY")
                    .select("S_SUPPKEY,S_NAME,S_ADDRESS,S_PHONE,TOTAL")
                    .orderBy("S_SUPPKEY");

            DataSet<R15> dataSet = batchTableEnvironment.toDataSet(result, R15.class);

            dataSet.output(new AvroOutputFormat<>(new Path("hdfs://namenode:8020/flink/result/r15.res"), R15.class));
        };

    }
}
