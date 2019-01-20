package query;

import main.BaseQuery;
import main.QueryImplementation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroOutputFormat;
import org.apache.flink.table.api.Table;
import result.R4;
import table.Lineitem;
import table.Orders;


public class Q4 extends BaseQuery {

    @Override
    public String getQueryName() {
        return "q4";
    }

    @Override
    public Integer approxTime() {
        return 5500000;
    }

    @Override
    public QueryImplementation queryImplementation() {
        return () -> {
            Table lineitem = Lineitem.getTable(executionEnvironment, batchTableEnvironment);
            Table orders = Orders.getTable(executionEnvironment, batchTableEnvironment);

            Table result = lineitem.filter("L_COMMITDATE < L_RECEIPTDATE")
                    .join(orders.filter("O_ORDERDATE >= '1995-01-01' ")
                            .filter("O_ORDERDATE < '1995-04-01' "))
                    .where("L_ORDERKEY == O_ORDERKEY")
                    .groupBy("O_ORDERPRIORITY")
                    .select("O_ORDERPRIORITY,O_ORDERPRIORITY.count as ORDER_COUNT")
                    .orderBy("O_ORDERPRIORITY");

            DataSet<R4> dataSet = batchTableEnvironment.toDataSet(result, R4.class);

            dataSet.output(new AvroOutputFormat<>(new Path("hdfs://namenode:8020/flink/result/r4.res"), R4.class));
        };
    }
}
