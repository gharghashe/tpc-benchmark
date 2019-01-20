package query;

import main.BaseQuery;
import main.QueryImplementation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroOutputFormat;
import org.apache.flink.table.api.Table;
import result.R6;
import table.Lineitem;


public class Q6 extends BaseQuery {

    @Override
    public String getQueryName() {
        return "q6";
    }

    @Override
    public Integer approxTime() {
        return 3700000;
    }

    @Override
    public QueryImplementation queryImplementation() {
        return () -> {
            Table lineitem = Lineitem.getTable(executionEnvironment, batchTableEnvironment);

            Table result = lineitem
                    .filter("L_SHIPDATE >= '1996-01-01' ")
                    .filter("L_SHIPDATE < '1997-01-01'")
                    .filter("L_DISCOUNT >= 0.05")
                    .filter("L_DISCOUNT <= 0.07")
                    .filter("L_QUANTITY < 24")
                    .select("(L_EXTENDEDPRICE*(1-L_DISCOUNT)).sum as REVENUE");

            DataSet<R6> dataSet = batchTableEnvironment.toDataSet(result, R6.class);

            dataSet.output(new AvroOutputFormat<>(new Path("hdfs://namenode:8020/flink/result/r6.res"), R6.class));
        };
    }
}
