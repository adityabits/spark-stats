package ml.shifu.plugin.spark.stats;

import java.util.List;

import ml.shifu.core.util.Params;
import ml.shifu.plugin.spark.stats.interfaces.ColumnStateArray;
import ml.shifu.plugin.spark.stats.interfaces.SparkStatsCalculator;

import org.apache.spark.Accumulable;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.dmg.pmml.DataField;
import org.dmg.pmml.ModelStats;
import org.dmg.pmml.PMML;
import org.dmg.pmml.UnivariateStats;

public class SimpleUnivariateStatsCalculator implements
        SparkStatsCalculator {

    public ModelStats calculate(JavaSparkContext jsc, JavaRDD<String> data, PMML pmml, Params bindingParams) {
        List<DataField> dataFields= pmml.getDataDictionary().getDataFields();
        Accumulable<ColumnStateArray, String> accum= jsc.accumulable(new SimpleUnivariateColumnStateArray(dataFields, bindingParams), new SparkAccumulableWrapper());
        //long time1= System.currentTimeMillis();
        System.out.println("--------Accumulating-------------");
        data.foreach(new AccumulatorApplicator(accum));
        ColumnStateArray colStateArray= accum.value();
        //long time2= System.currentTimeMillis();
        System.out.println("--------Populating PMML-------------");
        return  colStateArray.getModelStats();
        //long time3= System.currentTimeMillis();
        //System.out.println("" + (time2-time1) + ", " + (time3-time2));
    }
}
