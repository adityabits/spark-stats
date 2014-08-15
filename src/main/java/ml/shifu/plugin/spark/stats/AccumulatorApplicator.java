package ml.shifu.plugin.spark.stats;

import org.apache.spark.Accumulable;
import org.apache.spark.api.java.function.VoidFunction;

public class AccumulatorApplicator implements VoidFunction<String> {

    Accumulable<ColumnStateArray, String> accum;
    
    AccumulatorApplicator(Accumulable<ColumnStateArray, String> accum) {
        this.accum= accum;
    }
    
    public void call(String line) throws Exception {
        accum.add(line);
    }

}
