package query;

import function.IsAmericaCountry;
import function.StartWithBrushed;
import main.BaseQuery;
import main.QueryImplementation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroOutputFormat;
import org.apache.flink.table.api.Table;
import result.R14;
import table.Lineitem;
import table.Part;


public class Q14 extends BaseQuery {

    @Override
    public String getQueryName() {
        return "q14";
    }

    @Override
    public Integer approxTime() {
        return null;
    }

    @Override
    public QueryImplementation queryImplementation() {
        return () -> {
            executionEnvironment.getConfig().getRestartStrategy();
            batchTableEnvironment.registerFunction("StartWithBrushed", new StartWithBrushed());

            Table lineitem = Lineitem.getTable(executionEnvironment, batchTableEnvironment);
            Table part = Part.getTable(executionEnvironment, batchTableEnvironment);

            Table l = lineitem.filter("L_SHIPDATE >='1996-12-01'  && L_SHIPDATE < '1997-01-01'");
            Table res1 = part.join(l).where("P_PARTKEY == L_PARTKEY")
                    .select("P_TYPE,(L_EXTENDEDPRICE*(1-L_DISCOUNT)) as VALUE");

            Float total = batchTableEnvironment.toDataSet(res1.select("VALUE.sum as TOTAL_VALUE"), Float.class).collect().get(0);
            Table result = res1.select("(StartWithBrushed(P_TYPE,VALUE)).sum*100/" + total + " as BRUSHED_REVENUE");

            DataSet<R14> dataSet = batchTableEnvironment.toDataSet(result, R14.class);

            dataSet.output(new AvroOutputFormat<>(new Path("hdfs://namenode:8020/flink/result/r14.res"), R14.class));
        };
    }
}
