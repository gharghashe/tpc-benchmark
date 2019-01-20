package query;

import main.BaseQuery;
import main.QueryImplementation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroOutputFormat;
import org.apache.flink.table.api.Table;
import result.R1;
import table.Lineitem;

public class Q1 extends BaseQuery {

    @Override
    public String getQueryName() {
        return "q1";
    }

    @Override
    public Integer approxTime() {
        return 4100000;
    }

    @Override
    public QueryImplementation queryImplementation() {
        return () -> {
            Table lineitem = Lineitem.getTable(executionEnvironment, batchTableEnvironment);

            Table result = lineitem.filter("L_SHIPDATE <= '1998-12-01'")
                    .groupBy("L_RETURNFLAG,L_LINESTATUS")
                    .select("L_RETURNFLAG,L_LINESTATUS,L_QUANTITY.sum as SUM_QTY," +
                            "(L_EXTENDEDPRICE*(1-L_DISCOUNT)).sum as SUM_DISC_PRICE," +
                    "L_EXTENDEDPRICE.sum as SUM_BASE_PRICE,(L_EXTENDEDPRICE*(1-L_DISCOUNT)*(1+L_TAX)).sum as SUM_CHARGE," +
                            "L_QUANTITY.avg as AVG_QTY,L_EXTENDEDPRICE.avg as AVG_PRICE,L_DISCOUNT.avg as AVG_DISC")
                    .orderBy("L_RETURNFLAG,L_LINESTATUS");

            DataSet<R1> dataSet = batchTableEnvironment.toDataSet(result, R1.class);

            dataSet.output(new AvroOutputFormat<>(new Path("hdfs://namenode:8020/flink/result/r1.res"), R1.class));
        };
    }
}
