package main;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import java.util.Date;

public abstract class BaseQuery {

    protected ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
    protected BatchTableEnvironment batchTableEnvironment = TableEnvironment.getTableEnvironment(executionEnvironment);

    public abstract String getQueryName();

    public abstract Integer approxTime();

    public abstract QueryImplementation queryImplementation();

    public void startQueryExec() throws Exception {
        try {
            Master.resultLog.println("start exec " + getQueryName() + " at " + new Date().toString());
            queryImplementation().run();
            executionEnvironment.execute(getQueryName());
            Master.resultLog.println("end initial " + getQueryName() + " at " + new Date().toString());
        } catch (Exception e) {
            e.printStackTrace();
            Master.errorLog.println("exception occur in " + getQueryName() + " at " + new Date().toString());
        }
    }

}
