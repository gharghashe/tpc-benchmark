package query;

import main.BaseQuery;
import main.QueryImplementation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroOutputFormat;
import org.apache.flink.table.api.Table;
import result.R13;
import table.Customer;
import table.Orders;


public class Q13 extends BaseQuery {

    @Override
    public String getQueryName() {
        return "q13";
    }

    @Override
    public Integer approxTime() {
        return null;
    }

    @Override
    public QueryImplementation queryImplementation() {
        return () -> {
            Table orders = Orders.getTable(executionEnvironment, batchTableEnvironment);
            Table customer = Customer.getTable(executionEnvironment, batchTableEnvironment);

            Table o = orders.filter("!O_COMMENT.like('%pending%packages%')");
            Table co = customer.leftOuterJoin(o).where("C_CUSTKEY == O_CUSTKEY");

            Table temp1 = co.groupBy("O_ORDERKEY")
                    .select("O_ORDERKEY.count as C_COUNT,O_ORDERKEY");

            Table coo = co.select("O_CUSTKEY,O_ORDERKEY as O_ORDERKEY2");

            Table result = temp1.join(coo).where("O_ORDERKEY2 == O_ORDERKEY")
                    .select("C_COUNT,O_CUSTKEY,O_ORDERKEY")
                    .groupBy("C_COUNT")
                    .select("O_CUSTKEY.count as CUSTDIST,C_COUNT")
                    .orderBy("CUSTDIST.desc,C_COUNT.desc");

            DataSet<R13> dataSet = batchTableEnvironment.toDataSet(result, R13.class);
            dataSet.output(new AvroOutputFormat<>(new Path("hdfs://namenode:8020/flink/result/r13.res"), R13.class));
        };
    }
}
