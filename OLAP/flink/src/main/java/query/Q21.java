package query;

import main.BaseQuery;
import main.QueryImplementation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroOutputFormat;
import org.apache.flink.table.api.Table;
import result.R21;
import table.Lineitem;
import table.Nation;
import table.Orders;
import table.Supplier;


public class Q21 extends BaseQuery {

    @Override
    public String getQueryName() {
        return "q20";
    }

    @Override
    public Integer approxTime() {
        return null;
    }

    @Override
    public QueryImplementation queryImplementation() {
        return () -> {
            Table lineitem = Lineitem.getTable(executionEnvironment, batchTableEnvironment);
            Table nation = Nation.getTable(executionEnvironment, batchTableEnvironment);
            Table supplier = Supplier.getTable(executionEnvironment, batchTableEnvironment);
            Table orders = Orders.getTable(executionEnvironment, batchTableEnvironment);

            Table s = supplier.select("S_SUPPKEY,S_NATIONKEY,S_NAME");
            Table l = lineitem.select("L_SUPPKEY,L_ORDERKEY,L_RECEIPTDATE,L_COMMITDATE");
            Table l2 = l.filter("L_RECEIPTDATE > L_COMMITDATE");
            Table o = orders.select("O_ORDERKEY,O_ORDERSTATUS")
                    .filter("O_ORDERSTATUS = 'F' ");

            Table temp1 = l.groupBy("L_ORDERKEY")
                    .select("L_ORDERKEY,L_SUPPKEY.count as SUPPKEY_COUNT,L_SUPPKEY.max as SUPPKEY_MAX ")
                    .select("L_ORDERKEY as KEY,SUPPKEY_COUNT,SUPPKEY_MAX");

            Table temp2 = l.groupBy("L_ORDERKEY")
                    .select("L_ORDERKEY,L_SUPPKEY.count as SUPPKEY_COUNT,L_SUPPKEY.max as SUPPKEY_MAX ")
                    .select("L_ORDERKEY as KEY,SUPPKEY_COUNT,SUPPKEY_MAX");


            Table temp = nation.filter("N_NAME.like('%INDIA%') ")
                    .join(s).where("S_NATIONKEY == N_NATIONKEY")
                    .join(l2)
                    .where("S_SUPPKEY == L_SUPPKEY")
                    .join(o)
                    .where("O_ORDERKEY == L_ORDERKEY")
                    .join(temp1)
                    .where("KEY == L_ORDERKEY")
                    .filter(" ( SUPPKEY_COUNT>1 || SUPPKEY_COUNT == 1 ) && (L_SUPPKEY == SUPPKEY_MAX) ")
                    .select("S_NAME,L_ORDERKEY,L_SUPPKEY");

            Table result = temp.leftOuterJoin(temp2)
                    .where("L_ORDERKEY == KEY")
                    .select("S_NAME, L_ORDERKEY, L_SUPPKEY,SUPPKEY_COUNT,SUPPKEY_MAX")
                    .filter("(SUPPKEY_COUNT == 1) && (L_SUPPKEY == SUPPKEY_MAX)")
                    .groupBy("S_NAME")
                    .select("S_NAME,L_SUPPKEY.count as NUM_WAIT")
                    .orderBy("NUM_WAIT.desc,S_NAME");


            DataSet<R21> dataSet = batchTableEnvironment.toDataSet(result, R21.class);

            dataSet.output(new AvroOutputFormat<>(new Path("hdfs://namenode:8020/flink/result/r21.res"), R21.class));
        };
    }
}
