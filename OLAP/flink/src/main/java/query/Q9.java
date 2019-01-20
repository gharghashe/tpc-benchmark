package query;

import main.BaseQuery;
import main.QueryImplementation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroOutputFormat;
import org.apache.flink.table.api.Table;
import result.R9;
import table.*;


public class Q9 extends BaseQuery {

    @Override
    public String getQueryName() {
        return "q9";
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
            Table supplier = Supplier.getTable(executionEnvironment, batchTableEnvironment);
            Table part = Part.getTable(executionEnvironment, batchTableEnvironment);
            Table partsupp = PartSupp.getTable(executionEnvironment, batchTableEnvironment);

            Table lp = lineitem.join(part.filter("P_NAME.like('%blue%')")).where("P_PARTKEY == L_PARTKEY");
            Table ns = nation.join(supplier).where("N_NATIONKEY == S_NATIONKEY");
            Table temp = lp.join(ns).where("L_SUPPKEY == S_SUPPKEY")
                    .join(partsupp)
                    .where("L_SUPPKEY == PS_SUPPKEY")
                    .join(orders)
                    .where("L_ORDERKEY == O_ORDERKEY");
            Table profit = temp.select("N_NAME,O_ORDERDATE as O_YEAR,(L_EXTENDEDPRICE * (1 - L_DISCOUNT) - PS_SUPPLYCOST * L_QUANTITY) as AMOUNT");
            Table result = profit.groupBy("N_NAME,O_YEAR")
                    .select("AMOUNT.sum as SUM_PROFIT,O_YEAR,N_NAME")
                    .orderBy("N_NAME,O_YEAR.desc");

            DataSet<R9> dataSet = batchTableEnvironment.toDataSet(result, R9.class);

            dataSet.output(new AvroOutputFormat<>(new Path("hdfs://namenode:8020/flink/result/r9.res"), R9.class));
        };
    }
}
