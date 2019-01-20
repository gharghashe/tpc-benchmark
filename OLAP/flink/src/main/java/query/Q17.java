package query;

import main.BaseQuery;
import main.QueryImplementation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroOutputFormat;
import org.apache.flink.table.api.Table;
import result.R17;
import table.Lineitem;
import table.Part;


public class Q17 extends BaseQuery {

    @Override
    public String getQueryName() {
        return "q17";
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

            Table l = lineitem.select("L_PARTKEY,L_QUANTITY,L_EXTENDEDPRICE");
            Table p = part.filter("P_BRAND.like('%Brand#13%') && P_CONTAINER.like('%WRAP PKG%') ");

            Table pl = p.leftOuterJoin(l).where("P_PARTKEY == L_PARTKEY");

            Table temp = pl.groupBy("P_PARTKEY")
                    .select("(L_QUANTITY*0.2).avg as AVG_QUANTITY,P_PARTKEY as KEY");
            temp = temp.join(pl).where("KEY == P_PARTKEY")
                    .select("AVG_QUANTITY,KEY,L_QUANTITY,L_EXTENDEDPRICE");

            Table result = p.join(temp).where("KEY == P_PARTKEY")
                    .filter("L_QUANTITY < AVG_QUANTITY")
                    .select("(L_EXTENDEDPRICE/0.7) as AVG_YEARLY");

            DataSet<R17> dataSet = batchTableEnvironment.toDataSet(result, R17.class);

            dataSet.output(new AvroOutputFormat<>(new Path("hdfs://namenode:8020/flink/result/r17.res"), R17.class));
        };
    }
}
