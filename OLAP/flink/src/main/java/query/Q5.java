package query;

import main.BaseQuery;
import main.QueryImplementation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroOutputFormat;
import org.apache.flink.table.api.Table;
import result.R5;
import table.*;


public class Q5 extends BaseQuery {

    @Override
    public String getQueryName() {
        return "q5";
    }

    @Override
    public Integer approxTime() {
        return 5700000;
    }

    @Override
    public QueryImplementation queryImplementation() {
        return () -> {
            Table nation = Nation.getTable(executionEnvironment, batchTableEnvironment);
            Table region = Region.getTable(executionEnvironment, batchTableEnvironment);
            Table supplier = Supplier.getTable(executionEnvironment, batchTableEnvironment);
            Table customer = Customer.getTable(executionEnvironment, batchTableEnvironment);
            Table lineitem = Lineitem.getTable(executionEnvironment, batchTableEnvironment);
            Table orders = Orders.getTable(executionEnvironment, batchTableEnvironment);

            Table o = orders.filter("O_ORDERDATE >= '1996-11-01' ").filter("O_ORDERDATE < '1997-02-01' ");
            Table oc = customer.join(o).where("C_CUSTKEY == O_CUSTKEY").select("O_ORDERKEY");
            Table nr = region.filter("R_NAME == 'MIDDLE EAST' ").join(nation).where("R_REGIONKEY == N_REGIONKEY");

            Table temp1 = nr.join(supplier).where("N_NATIONKEY == S_NATIONKEY")
                    .join(lineitem).where("S_SUPPKEY == L_SUPPKEY")
                    .select("N_NAME,L_EXTENDEDPRICE,L_DISCOUNT,L_ORDERKEY");
            Table result = temp1.join(oc).where("L_ORDERKEY == O_ORDERKEY")
                    .groupBy("N_NAME")
                    .select("(L_EXTENDEDPRICE*(1-L_DISCOUNT)).sum as REVENUE")
                    .orderBy("REVENUE.desc");

            DataSet<R5> dataSet = batchTableEnvironment.toDataSet(result, R5.class);

            dataSet.output(new AvroOutputFormat<>(new Path("hdfs://namenode:8020/flink/result/r5.res"), R5.class));
        };
    }
}
