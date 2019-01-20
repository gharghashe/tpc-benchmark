package function;

import org.apache.flink.table.functions.ScalarFunction;

public class IsLowPriority extends ScalarFunction {

    public int eval(String x) {
        if (x.contains("2-HIGH") || x.contains("1-URGENT"))
            return 0;
        return 1;
    }
}
