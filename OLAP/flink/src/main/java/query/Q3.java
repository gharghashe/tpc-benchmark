package query;

import main.BaseQuery;
import main.QueryImplementation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroOutputFormat;
import org.apache.flink.table.api.Table;
import result.R3;
import table.Customer;
import table.Lineitem;
import table.Orders;


public class Q3 extends BaseQuery {

    @Override
    public String getQueryName() {
        return "q3";
    }

    @Override
    public Integer approxTime() {
        return 12000000;
    }

    @Override
    public QueryImplementation queryImplementation() {
        return () -> {
            Table lineitem = Lineitem.getTable(executionEnvironment, batchTableEnvironment);
            Table orders = Orders.getTable(executionEnvironment, batchTableEnvironment);
            Table customer = Customer.getTable(executionEnvironment, batchTableEnvironment);

            Table l = lineitem.filter("L_SHIPDATE > '1995-03-25'");
            Table o = orders.filter("O_ORDERDATE < '1995-03-25' ");

            Table ocl = customer.filter("C_MKTSEGMENT == 'AUTOMOBILE'")
                    .join(l.join(o).where("L_ORDERKEY == O_ORDERKEY"))
                    .where("C_CUSTKEY == O_CUSTKEY");

            Table temp = ocl.select("O_ORDERKEY,O_ORDERDATE,O_SHIPPRIORITY")
                    .join(l)
                    .where("O_ORDERKEY == L_ORDERKEY")
                    .select("O_ORDERKEY,O_ORDERDATE,O_SHIPPRIORITY,L_ORDERKEY,L_EXTENDEDPRICE,L_DISCOUNT");

            Table result = temp.groupBy("L_ORDERKEY,O_ORDERDATE,O_SHIPPRIORITY").select("(L_EXTENDEDPRICE*(1-L_DISCOUNT)).sum as REVENUE,O_ORDERDATE")
                    .orderBy("O_ORDERDATE,REVENUE.desc").fetch(10);

            DataSet<R3> dataSet = batchTableEnvironment.toDataSet(result, R3.class);

            dataSet.output(new AvroOutputFormat<>(new Path("hdfs://namenode:8020/flink/result/r3.res"), R3.class));
        };
    }
}
