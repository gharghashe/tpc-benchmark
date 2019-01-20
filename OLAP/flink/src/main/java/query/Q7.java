package query;

import main.BaseQuery;
import main.QueryImplementation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroOutputFormat;
import org.apache.flink.table.api.Table;
import result.R7;
import table.*;


public class Q7 extends BaseQuery {

    @Override
    public String getQueryName() {
        return "q7";
    }

    @Override
    public Integer approxTime() {
        return 5000000;
    }

    @Override
    public QueryImplementation queryImplementation() {
        return () -> {
            Table lineitem = Lineitem.getTable(executionEnvironment, batchTableEnvironment);
            Table orders = Orders.getTable(executionEnvironment, batchTableEnvironment);
            Table nation = Nation.getTable(executionEnvironment, batchTableEnvironment);
            Table supplier = Supplier.getTable(executionEnvironment, batchTableEnvironment);
            Table customer = Customer.getTable(executionEnvironment, batchTableEnvironment);

            Table l = lineitem.filter("L_SHIPDATE >= '1995-01-01' ").filter("L_SHIPDATE < '1996-12-31'");
            Table n = nation.filter("N_NAME.like('ETHIOPIA%') || N_NAME.like('JAPAN%') ");

            Table temp = n.join(supplier).where("N_NATIONKEY == S_NATIONKEY")
                    .join(l).where("S_SUPPKEY == L_SUPPKEY")
                    .select("N_NAME as SUPP_NATION,L_ORDERKEY,L_EXTENDEDPRICE,L_DISCOUNT,L_SHIPDATE");

            Table temp2 = n.join(customer).where("N_NATIONKEY == C_NATIONKEY")
                    .join(orders).where("C_CUSTKEY == O_CUSTKEY")
                    .select("N_NAME as CUST_NATION,O_ORDERKEY")
                    .join(temp).where("O_ORDERKEY == L_ORDERKEY")
                    .distinct();

            Table result = temp2.filter("(SUPP_NATION.like('JAPAN%') && CUST_NATION.like('ETHIOPIA%')) || (SUPP_NATION.like('ETHIOPIA%') && CUST_NATION.like('JAPAN'))")
                    .select("SUPP_NATION,CUST_NATION,L_SHIPDATE,L_DISCOUNT,L_EXTENDEDPRICE")
                    .groupBy("SUPP_NATION,CUST_NATION,L_SHIPDATE")
                    .select("SUPP_NATION,CUST_NATION,L_SHIPDATE,(L_EXTENDEDPRICE*(1-L_DISCOUNT)).sum as REVENUE")
                    .orderBy("SUPP_NATION,CUST_NATION,L_SHIPDATE");

            DataSet<R7> dataSet = batchTableEnvironment.toDataSet(result, R7.class);

            dataSet.output(new AvroOutputFormat<>(new Path("hdfs://namenode:8020/flink/result/r7.res"), R7.class));
        };
    }
}
