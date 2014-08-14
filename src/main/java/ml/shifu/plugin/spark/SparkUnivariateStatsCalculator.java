package ml.shifu.plugin.spark;

import ml.shifu.core.util.Params;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.dmg.pmml.PMML;
import org.dmg.pmml.UnivariateStats;

public interface SparkUnivariateStatsCalculator {

    UnivariateStats calculate(JavaSparkContext jsc, JavaRDD<String> data, PMML pmml, Params params);
}
