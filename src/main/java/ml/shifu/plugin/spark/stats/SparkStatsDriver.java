/*
 * Class called by spark-submit script. Needs a main method.
 * Arguments:
 *  1. HDFS Uri
 *  2. HDFS path to Input file
 *  3. HDFS path to PMML XML
 *  4. HDFS path to Request json
 */
package ml.shifu.plugin.spark.stats;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import ml.shifu.core.request.Binding;
import ml.shifu.core.request.Request;
import ml.shifu.core.util.JSONUtils;
import ml.shifu.core.util.PMMLUtils;
import ml.shifu.core.util.Params;
import ml.shifu.core.util.RequestUtils;
import ml.shifu.plugin.spark.stats.CombinedUtils;
import ml.shifu.plugin.spark.stats.interfaces.SparkStatsCalculator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.dmg.pmml.Model;
import org.dmg.pmml.ModelStats;
import org.dmg.pmml.PMML;

public class SparkStatsDriver {

    public static void main(String[] args) throws IOException, URISyntaxException {
        String hdfsUri= args[0];
        String pathHdfsInput= args[1];
        String pathHdfsPmml= args[2];
        String pathHdfsRequest= args[3];
        
        FileSystem hdfs = FileSystem.get(new URI(hdfsUri), new Configuration());

        PMML pmml = CombinedUtils.loadPMML(pathHdfsPmml, hdfs);

        Request req = JSONUtils.readValue(hdfs.open(new Path(pathHdfsRequest)),
                Request.class);

        Params params = req.getProcessor().getParams();

        // TODO: Convert pathHDFSTmp to full hdfs path
        String pathHDFSTmp = (String) params.get("pathHDFSTmp",
                "ml/shifu/plugin/spark/tmp");
        
        String appName = (String) params.get("SparkAppName", "spark-stats");
        SparkConf conf = new SparkConf().setAppName(appName);
        conf.set("spark.serializer",
                "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        Binding binding = RequestUtils.getUniqueBinding(req, "UnivariateStatsCalculator");
        Params bindingParams= binding.getParams();
        
        // TODO: use DI
        SparkStatsCalculator sparkCalculator= new BinomialStatsCalculator();
        // create RDD
        JavaRDD<String> data= jsc.textFile(pathHdfsInput);
        
        ModelStats modelStats= sparkCalculator.calculate(jsc, data, pmml, bindingParams);
        
        // store univariateStats in pmml and save in pathPMML
        Model model = PMMLUtils.getModelByName(pmml, (String) bindingParams.get("modelName"));
        model.setModelStats(modelStats);
        
        // save PMML to HDFS tmp
        CombinedUtils.savePMML(pmml, pathHdfsPmml, hdfs);
    }

}
