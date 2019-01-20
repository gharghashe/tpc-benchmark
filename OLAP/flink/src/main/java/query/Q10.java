package query;

import main.BaseQuery;
import main.QueryImplementation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroOutputFormat;
import org.apache.flink.table.api.Table;
import result.R10;
import table.Customer;
import table.Lineitem;
import table.Nation;
import table.Orders;


public class Q10 extends BaseQuery {

    @Override
    public String getQueryName() {
        return "q10";
    }

    @Override
    public Integer approxTime() {
        return null;
    }

    @Override
    public QueryImplementation queryImplementation() {
        return () -> {
            Table lineitem = Lineitem.getTable(executionEnvironment, batchTableEnvironment);
            Table orders = Orders.getTable(executionEnvironment, batchTableEnvironment);
            Table nation = Nation.getTable(executionEnvironment, batchTableEnvironment);
            Table customer = Customer.getTable(executionEnvironment, batchTableEnvironment);

            Table l = lineitem.filter("L_RETURNFLAG == 'R'");
            Table temp = orders.filter("O_ORDERDATE >= '1994-11-01' ")
                    .filter("O_ORDERDATE < '1995-02-01' ")
                    .join(customer).where("C_CUSTKEY == O_CUSTKEY")
                    .join(nation).where("C_NATIONKEY == N_NATIONKEY")
                    .join(l).where("L_ORDERKEY == O_ORDERKEY");
            Table result = temp.select("C_CUSTKEY,C_NAME,(L_EXTENDEDPRICE*(1-L_DISCOUNT)) as VOLUME,C_ACCTBAL,N_NAME,C_ADDRESS,C_PHONE,C_COMMENT")
                    .groupBy("C_CUSTKEY,C_NAME,C_ACCTBAL,N_NAME,C_ADDRESS,C_COMMENT,C_PHONE")
                    .select("VOLUME.sum as REVENUE,C_CUSTKEY,C_NAME,C_ACCTBAL,N_NAME,C_ADDRESS,C_COMMENT,C_PHONE");

            DataSet<R10> dataSet = batchTableEnvironment.toDataSet(result, R10.class);
            dataSet.output(new AvroOutputFormat<>(new Path("hdfs://namenode:8020/flink/result/r10.res"), R10.class));
        };
    }
}
