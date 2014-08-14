package ml.shifu.plugin.spark;

import java.util.List;

import ml.shifu.core.util.Params;

import org.apache.spark.Accumulable;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.dmg.pmml.DataField;
import org.dmg.pmml.PMML;
import org.dmg.pmml.UnivariateStats;

public class SparkSimpleUnivariateStatsCalculator implements
        SparkUnivariateStatsCalculator {

    public UnivariateStats calculate(JavaSparkContext jsc, JavaRDD<String> data, PMML pmml, Params bindingParams) {
        UnivariateStats univariateStats= new UnivariateStats();
        List<DataField> dataFields= pmml.getDataDictionary().getDataFields();
        Accumulable<ColumnStateArray, String> accum= jsc.accumulable(new ColumnStateArray(dataFields, bindingParams), new SparkAccumulableWrapper());
        data.foreach(new AccumulatorApplicator(accum));
        ColumnStateArray colStateArray= accum.value();
        colStateArray.populatePMML(univariateStats);
        return univariateStats;
    }

}
