package query;

import main.BaseQuery;
import main.QueryImplementation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroOutputFormat;
import org.apache.flink.table.api.Table;
import result.R11;
import table.Nation;
import table.PartSupp;
import table.Supplier;


public class Q11 extends BaseQuery {

    @Override
    public String getQueryName() {
        return "q12";
    }

    @Override
    public Integer approxTime() {
        return null;
    }

    @Override
    public QueryImplementation queryImplementation() {
        return () -> {
            Table nation = Nation.getTable(executionEnvironment, batchTableEnvironment);
            Table supplier = Supplier.getTable(executionEnvironment, batchTableEnvironment);
            Table partsupp = PartSupp.getTable(executionEnvironment, batchTableEnvironment);

            Table temp = nation.filter("N_NAME.like('%ARGENTINA%')")
                    .join(supplier)
                    .where("N_NATIONKEY == S_NATIONKEY")
                    .select("S_SUPPKEY")
                    .join(partsupp).where("S_SUPPKEY == PS_SUPPKEY")
                    .select("PS_PARTKEY,(PS_SUPPLYCOST*PS_AVAILQTY) as VALUE");
            Table sum = temp.select("VALUE.sum as TOTAL_VALUE");
            Table temp2 = temp.groupBy("PS_PARTKEY")
                    .select("VALUE.sum as PART_VALUE,PS_PARTKEY");
            Table result = temp2.join(sum).filter("PART_VALUE > (TOTAL_VALUE*0.0001)").orderBy("PART_VALUE.desc")
                    .select("PS_PARTKEY,PART_VALUE");

            DataSet<R11> dataSet = batchTableEnvironment.toDataSet(result, R11.class);

            dataSet.output(new AvroOutputFormat<>(new Path("hdfs://namenode:8020/flink/result/r11.res"), R11.class));
        };
    }
}
