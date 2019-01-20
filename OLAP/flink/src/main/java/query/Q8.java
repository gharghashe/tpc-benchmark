package query;

import function.IsAmericaCountry;
import main.BaseQuery;
import main.QueryImplementation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroOutputFormat;
import org.apache.flink.table.api.Table;
import result.R8;
import table.*;


public class Q8 extends BaseQuery {

    @Override
    public String getQueryName() {
        return "q8";
    }

    @Override
    public Integer approxTime() {
        return null;
    }

    @Override
    public QueryImplementation queryImplementation() {
        return () -> {
            executionEnvironment.getConfig().getRestartStrategy();
            batchTableEnvironment.registerFunction("IsAmericaCountry", new IsAmericaCountry());

            Table lineitem = Lineitem.getTable(executionEnvironment, batchTableEnvironment);
            Table orders = Orders.getTable(executionEnvironment, batchTableEnvironment);
            Table nation = Nation.getTable(executionEnvironment, batchTableEnvironment);
            Table region = Region.getTable(executionEnvironment, batchTableEnvironment);
            Table supplier = Supplier.getTable(executionEnvironment, batchTableEnvironment);
            Table customer = Customer.getTable(executionEnvironment, batchTableEnvironment);
            Table part = Part.getTable(executionEnvironment, batchTableEnvironment);

            Table o = orders.filter("O_ORDERDATE >= '1995-01-01' ").filter("O_ORDERDATE < '1996-12-31' ");

            Table nr = nation.join(region.filter("R_NAME == 'AMERICA' "))
                    .where("N_REGIONKEY == R_REGIONKEY")
                    .select("N_NATIONKEY,N_NAME");
            Table nrc = nr.join(customer).where("N_NATIONKEY == C_NATIONKEY")
                    .select("C_CUSTKEY,N_NAME");

            Table temp1 = lineitem.join(part.filter("P_TYPE == 'ECONOMY ANODIZED STEEL' ")).where("L_PARTKEY == P_PARTKEY");
            Table temp2 = temp1.join(nation.join(supplier).where("N_NATIONKEY == S_NATIONKEY"))
                    .where("L_SUPPKEY == S_SUPPKEY")
                    .select("L_PARTKEY,L_SUPPKEY,L_ORDERKEY,(L_EXTENDEDPRICE*(1-L_DISCOUNT)) as VOLUME");
            Table temp3 = nrc.join(o).where("C_CUSTKEY == O_CUSTKEY")
                    .select("O_ORDERKEY,O_ORDERDATE,N_NAME");

            Table lastResult = temp3.join(temp2).where("O_ORDERKEY == L_ORDERKEY");

            Table result2 = lastResult.select("O_ORDERDATE as O_YEAR ,VOLUME,IsAmericaCountry(N_NAME,VOLUME) as AMERICA_VOLUME");

            Table year2 = result2.groupBy("O_YEAR")
                    .select("O_YEAR as O_YEAR2,VOLUME.sum as TOTAL_VOLUME")
                    .orderBy("O_YEAR2");
            Table year3 = result2.groupBy("O_YEAR")
                    .select("O_YEAR as O_YEAR3,AMERICA_VOLUME.sum as TOTAL_AMERICA")
                    .orderBy("O_YEAR3");

            Table result = year3.join(year2)
                    .where("O_YEAR2 == O_YEAR3")
                    .distinct()
                    .select("O_YEAR3 as O_YEAR,(TOTAL_AMERICA/TOTAL_VOLUME) as MKT_SHARE");

            DataSet<R8> dataSet = batchTableEnvironment.toDataSet(result, R8.class);

            dataSet.output(new AvroOutputFormat<>(new Path("hdfs://namenode:8020/flink/result/r8.res"), R8.class));
        };
    }
}
