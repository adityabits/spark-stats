package ml.shifu.plugin.spark.stats.columnstates;

import java.util.ArrayList;
import java.util.List;

import ml.shifu.core.util.Params;
import ml.shifu.plugin.spark.stats.interfaces.ColumnState;
import ml.shifu.plugin.spark.stats.interfaces.UnitState;
import ml.shifu.plugin.spark.stats.unitstates.BasicNumericInfoUnitState;
import ml.shifu.plugin.spark.stats.unitstates.FrequencyUnitState;
import ml.shifu.plugin.spark.stats.unitstates.RSampleUnitState;


public class SimpleUnivariateContState extends ColumnState {
    
    public SimpleUnivariateContState(String name, Params parameters) {
        params= parameters;
        //this.field= field;
        //this.field= null;
        int sampleSize= Integer.parseInt(params.get("sampleSize", "100000").toString());
        states= new ArrayList<UnitState>();
        states.add(new BasicNumericInfoUnitState());
        states.add(new FrequencyUnitState());
        states.add(new RSampleUnitState(sampleSize));
        fieldName= name;
    }
    
    public ColumnState getNewBlank() {
        return new SimpleUnivariateContState(fieldName, params);
    }
    
    public void checkClass(ColumnState colState) throws Exception {
        if(!(colState instanceof SimpleUnivariateContState))
            throw new Exception("Expected UnivariateContState in merge, got " + colState.getClass().toString());
    }

}
