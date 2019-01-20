package query;

import main.BaseQuery;
import main.QueryImplementation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroOutputFormat;
import org.apache.flink.table.api.Table;
import result.R22;
import table.Customer;
import table.Orders;


public class Q22 extends BaseQuery {

    @Override
    public String getQueryName() {
        return "q22";
    }

    @Override
    public Integer approxTime() {
        return null;
    }

    @Override
    public QueryImplementation queryImplementation() {
        return () -> {
            Table customer = Customer.getTable(executionEnvironment, batchTableEnvironment);
            Table orders = Orders.getTable(executionEnvironment, batchTableEnvironment);
            String codes = "('28', '43', '22', '39', '31', '30', '41')";

            Table c = customer.select("C_ACCTBAL,C_CUSTKEY,C_PHONE.substring(1,2) as COUNTRY_CODE ");
            Table c2 = c.filter("COUNTRY_CODE.in" + codes);

            Table avg = c2.filter("C_ACCTBAL > 0.0 ")
                    .select("C_ACCTBAL.avg as AVG_ACCTBAL");

            Table temp = orders.select("O_CUSTKEY")
                    .rightOuterJoin(c2, "O_CUSTKEY == C_CUSTKEY");

            Table result = temp.join(avg)
                    .filter("C_ACCTBAL > AVG_ACCTBAL")
                    .groupBy("COUNTRY_CODE")
                    .select("C_ACCTBAL.count as ACCTBAL_COUNT,C_ACCTBAL.sum as ACCTBAL_SUM,COUNTRY_CODE")
                    .orderBy("COUNTRY_CODE");

            DataSet<R22> dataSet = batchTableEnvironment.toDataSet(result, R22.class);

            dataSet.output(new AvroOutputFormat<>(new Path("hdfs://namenode:8020/flink/result/r22.res"), R22.class));
        };
    }
}
