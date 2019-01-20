package query;

import main.BaseQuery;
import main.QueryImplementation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroOutputFormat;
import org.apache.flink.table.api.Table;
import result.R20;
import table.*;


public class Q20 extends BaseQuery {

    @Override
    public String getQueryName() {
        return "q19";
    }

    @Override
    public Integer approxTime() {
        return null;
    }

    @Override
    public QueryImplementation queryImplementation() {
        return () -> {
            Table lineitem = Lineitem.getTable(executionEnvironment, batchTableEnvironment);
            Table part = Part.getTable(executionEnvironment, batchTableEnvironment);
            Table nation = Nation.getTable(executionEnvironment, batchTableEnvironment);
            Table supplier = Supplier.getTable(executionEnvironment, batchTableEnvironment);
            Table partsupp = PartSupp.getTable(executionEnvironment, batchTableEnvironment);

            Table l = lineitem.filter("L_SHIPDATE >= '1994-01-01' && L_SHIPDATE < '1995-01-01' ")
                    .groupBy("L_PARTKEY,L_SUPPKEY")
                    .select("(L_QUANTITY*0.5).sum as SUM_QUANTITY,L_SUPPKEY,L_PARTKEY");

            Table n = nation.filter("N_NAME.like('%INDIA%')");
            Table ns = supplier.select("S_SUPPKEY,S_NAME,S_NATIONKEY,S_ADDRESS")
                    .join(n)
                    .where("S_NATIONKEY == N_NATIONKEY");


            Table temp = part.filter("P_NAME.like('forest%')")
                    .select("P_PARTKEY")
                    .join(partsupp)
                    .where("P_PARTKEY == PS_PARTKEY")
                    .join(l)
                    .where("L_SUPPKEY == PS_SUPPKEY && L_PARTKEY == PS_PARTKEY")
                    .filter("PS_AVAILQTY > SUM_QUANTITY")
                    .select("PS_SUPPKEY");

            Table result = temp.join(ns)
                    .where("S_SUPPKEY == PS_SUPPKEY")
                    .select("S_NAME,S_ADDRESS")
                    .orderBy("S_NAME");

            DataSet<R20> dataSet = batchTableEnvironment.toDataSet(result, R20.class);

            dataSet.output(new AvroOutputFormat<>(new Path("hdfs://namenode:8020/flink/result/r20.res"), R20.class));
        };
    }
}
