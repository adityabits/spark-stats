package ml.shifu.plugin.spark.stats.interfaces;

import ml.shifu.core.util.Params;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.dmg.pmml.ModelStats;
import org.dmg.pmml.PMML;

public interface SparkStatsCalculator {

    ModelStats calculate(JavaSparkContext jsc, JavaRDD<String> data, PMML pmml, Params params);
}
