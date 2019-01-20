package query;

import main.BaseQuery;
import main.QueryImplementation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroOutputFormat;
import org.apache.flink.table.api.Table;
import result.R2;
import table.*;


public class Q2 extends BaseQuery {

    @Override
    public String getQueryName() {
        return "q2";
    }

    @Override
    public Integer approxTime() {
        return 200000;
    }

    @Override
    public QueryImplementation queryImplementation() {
        return () -> {
            Table nation = Nation.getTable(executionEnvironment, batchTableEnvironment);
            Table region = Region.getTable(executionEnvironment, batchTableEnvironment);
            Table supplier = Supplier.getTable(executionEnvironment, batchTableEnvironment);
            Table partsupp = PartSupp.getTable(executionEnvironment, batchTableEnvironment);
            Table part = Part.getTable(executionEnvironment, batchTableEnvironment);

            Table r = region.filter("R_NAME == 'AFRICA' ");
            Table p = part.filter("P_SIZE == 7")
                    .filter("P_TYPE.like('%BURNISHED%')");

            Table nr = nation.join(r).where("N_REGIONKEY == R_REGIONKEY");
            Float min_supp = batchTableEnvironment.toDataSet(partsupp.select("(PS_SUPPLYCOST).min as min_supp_cost"), Float.class).collect().get(0);
            Table ps = partsupp.filter("PS_SUPPLYCOST == " + min_supp);

            Table temp = nr.join(supplier)
                    .where("N_NATIONKEY == S_NATIONKEY")
                    .join(ps)
                    .where("PS_SUPPKEY == S_SUPPKEY");

            Table result = temp.join(p)
                    .where("P_PARTKEY == PS_PARTKEY")
                    .select("S_ACCTBAL,S_NAME,N_NAME,P_PARTKEY,P_MFGR,S_ADDRESS,S_PHONE,S_COMMENT")
                    .orderBy("S_ACCTBAL.desc,N_NAME,S_NAME,P_PARTKEY")
                    .fetch(100);

            DataSet<R2> dataSet = batchTableEnvironment.toDataSet(result, R2.class);

            dataSet.output(new AvroOutputFormat<>(new Path("hdfs://namenode:8020/flink/result/r2.res"), R2.class));
        };
    }
}
