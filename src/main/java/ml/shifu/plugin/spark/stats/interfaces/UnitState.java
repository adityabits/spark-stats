package ml.shifu.plugin.spark.stats.interfaces;

import ml.shifu.core.util.Params;

import org.dmg.pmml.UnivariateStats;

public interface UnitState extends java.io.Serializable {

    UnitState getNewBlank();
    void merge(UnitState state) throws Exception;
    void addData(Object value);
    public void populateUnivariateStats(UnivariateStats univariateStats, Params params);

}
