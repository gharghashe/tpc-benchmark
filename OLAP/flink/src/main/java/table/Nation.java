package table;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroInputFormat;
import org.apache.flink.table.api.Table;

public class Nation {

    public Integer N_NATIONKEY;
    public String N_NAME;
    public Integer N_REGIONKEY;
    public String N_COMMENT;


    public Integer getN_NATIONKEY() {
        return N_NATIONKEY;
    }

    public void setN_NATIONKEY(Integer n_NATIONKEY) {
        N_NATIONKEY = n_NATIONKEY;
    }

    public String getN_NAME() {
        return N_NAME;
    }

    public void setN_NAME(String n_NAME) {
        N_NAME = n_NAME;
    }

    public Integer getN_REGIONKEY() {
        return N_REGIONKEY;
    }

    public void setN_REGIONKEY(Integer n_REGIONKEY) {
        N_REGIONKEY = n_REGIONKEY;
    }

    public String getN_COMMENT() {
        return N_COMMENT;
    }

    public void setN_COMMENT(String n_COMMENT) {
        N_COMMENT = n_COMMENT;
    }

    public Nation() {
    }

    public static Table getTable(org.apache.flink.api.java.ExecutionEnvironment env, org.apache.flink.table.api.java.BatchTableEnvironment tEnv) {
        Path path = new Path("hdfs://namenode:8020/nation.avro");
        AvroInputFormat<Nation> format = new AvroInputFormat<Nation>(path, Nation.class);
        DataSet<Nation> nationDataSet = env.createInput(format);
        Table nation = tEnv.fromDataSet(nationDataSet);
        return nation;
    }
}
