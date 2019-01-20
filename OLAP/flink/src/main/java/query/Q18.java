package query;

import main.BaseQuery;
import main.QueryImplementation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroOutputFormat;
import org.apache.flink.table.api.Table;
import result.R18;
import table.Customer;
import table.Lineitem;
import table.Orders;


public class Q18 extends BaseQuery {

    @Override
    public String getQueryName() {
        return "q18";
    }

    @Override
    public Integer approxTime() {
        return null;
    }

    @Override
    public QueryImplementation queryImplementation() {
        return () -> {
            Table lineitem = Lineitem.getTable(executionEnvironment, batchTableEnvironment);
            Table customer = Customer.getTable(executionEnvironment, batchTableEnvironment);
            Table orders = Orders.getTable(executionEnvironment, batchTableEnvironment);

            Table lo = lineitem.groupBy("L_ORDERKEY")
                    .select("L_QUANTITY.sum as SUM_QUANTITY,L_ORDERKEY as KEY")
                    .filter("SUM_QUANTITY > 309")
                    .join(orders)
                    .where("KEY == O_ORDERKEY");


            Table temp = lo.join(lineitem).where("L_ORDERKEY == KEY");

            Table result = temp.join(customer).where("C_CUSTKEY == O_CUSTKEY")
                    .select("L_QUANTITY,C_NAME,C_CUSTKEY,O_ORDERKEY,O_ORDERDATE,O_TOTALPRICE,O_CUSTKEY")
                    .groupBy("C_NAME,C_CUSTKEY,O_ORDERKEY,O_ORDERDATE,O_TOTALPRICE")
                    .select("L_QUANTITY.sum as L_QUANTITY_SUM,O_ORDERDATE,O_TOTALPRICE,O_ORDERKEY,C_CUSTKEY,C_NAME")
                    .orderBy("O_TOTALPRICE.desc,O_ORDERDATE")
                    .fetch(100);

            DataSet<R18> dataSet = batchTableEnvironment.toDataSet(result, R18.class);

            dataSet.output(new AvroOutputFormat<>(new Path("hdfs://namenode:8020/flink/result/r18.res"), R18.class));
        };
    }
}
