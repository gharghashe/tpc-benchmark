package function;

import org.apache.flink.table.functions.ScalarFunction;

public class IsAmericaCountry extends ScalarFunction {

    public float eval(String x, Float y) {
        if (x.contains("AMERICA"))
            return y;
        return 0;
    }

}