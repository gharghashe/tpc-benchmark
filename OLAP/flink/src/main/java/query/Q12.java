package query;

import function.IsHighPriority;
import function.IsLowPriority;
import function.StartWithBrushed;
import main.BaseQuery;
import main.QueryImplementation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroOutputFormat;
import org.apache.flink.table.api.Table;
import result.R12;
import table.Lineitem;
import table.Orders;


public class Q12 extends BaseQuery {

    @Override
    public String getQueryName() {
        return "q12";
    }

    @Override
    public Integer approxTime() {
        return null;
    }

    @Override
    public QueryImplementation queryImplementation() {
        return () -> {
            executionEnvironment.getConfig().getRestartStrategy();
            batchTableEnvironment.registerFunction("IsHighPriority", new IsHighPriority());
            batchTableEnvironment.registerFunction("IsLowPriority", new IsLowPriority());

            Table lineitem = Lineitem.getTable(executionEnvironment, batchTableEnvironment);
            Table orders = Orders.getTable(executionEnvironment, batchTableEnvironment);

            Table temp = lineitem.filter("(L_SHIPMODE = 'MAIL' || L_SHIPMODE == 'SHIP') && L_COMMITDATE < L_RECEIPTDATE && " +
                    "L_SHIPDATE < L_COMMITDATE && L_RECEIPTDATE >= '1997-01-01' && L_RECEIPTDATE <= '1998-01-01' ");
            Table temp2 = temp.join(orders)
                    .where("L_ORDERKEY == O_ORDERKEY")
                    .select("L_SHIPMODE,O_ORDERPRIORITY");
            Table result = temp2.groupBy("L_SHIPMODE")
                    .select("L_SHIPMODE,IsHighPriority(O_ORDERPRIORITY).sum as HIGH_LINE_COUNT,IsLowPriority(O_ORDERPRIORITY).sum as LOW_LINE_COUNT")
                    .orderBy("L_SHIPMODE");

            DataSet<R12> dataSet = batchTableEnvironment.toDataSet(result, R12.class);
            dataSet.output(new AvroOutputFormat<>(new Path("hdfs://namenode:8020/flink/result/r12.res"), R12.class));
        };
    }
}
