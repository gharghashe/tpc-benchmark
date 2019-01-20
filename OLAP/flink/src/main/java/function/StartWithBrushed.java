package function;


import org.apache.flink.table.functions.ScalarFunction;

public class StartWithBrushed extends ScalarFunction {
    public float eval(String x, Float y) {
        if (x.startsWith("BRUSHED"))
            return y;
        return 0;
    }
}
