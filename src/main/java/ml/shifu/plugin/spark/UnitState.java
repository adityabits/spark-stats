package ml.shifu.plugin.spark;

import org.dmg.pmml.UnivariateStats;

public interface UnitState {

    UnitState getNewBlank();
    void merge(UnitState state) throws Exception;
    void addData(String strValue);

}
