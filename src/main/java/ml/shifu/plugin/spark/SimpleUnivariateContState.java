package ml.shifu.plugin.spark;

import java.util.List;

import org.dmg.pmml.DataField;
import org.dmg.pmml.UnivariateStats;

import ml.shifu.core.util.Params;
import ml.shifu.plugin.spark.stats.unitstates.BasicNumericInfoUnitState;
import ml.shifu.plugin.spark.stats.unitstates.FrequencyUnitState;
import ml.shifu.plugin.spark.stats.unitstates.RSampleUnitState;


public class SimpleUnivariateContState implements ColumnState {
    BasicNumericInfoUnitState numState;
    FrequencyUnitState freqState;
    RSampleUnitState sampleState;
    private Params params;
    private DataField field;
    
    SimpleUnivariateContState(DataField field, Params params) {
        this.params= params;
        this.field= field;
        int sampleSize= Integer.parseInt(params.get("sampleSize", "100000").toString());
        this.numState= new BasicNumericInfoUnitState();
        this.freqState= new FrequencyUnitState();
        this.sampleState= new RSampleUnitState(sampleSize);
    }
    
    /*
     * Returns a new blank ColumnState which is an object of type UnivariateContState
     */
    public ColumnState getNewBlank() {
        return new SimpleUnivariateContState(this.field, this.params);
    }
    
    public FrequencyUnitState getFreqState() {
        return this.freqState;
    }
    public BasicNumericInfoUnitState getNumericInfoState() {
        return this.numState;
    }
    public RSampleUnitState getSampleState() {
        return this.sampleState;
    }
    
    public void merge(ColumnState colState) throws Exception {
        // TODO: check if datafields match
        if(!(colState instanceof SimpleUnivariateContState))
            throw new Exception("Expected UnivariateContState in merge, got " + colState.getClass().toString());
        
        SimpleUnivariateContState newColState= (SimpleUnivariateContState) colState;
        this.freqState.merge(newColState.getFreqState());
        this.numState.merge(newColState.getNumericInfoState());
        this.sampleState.merge(newColState.getSampleState());
    }

    public void addData(String strValue) {
        this.freqState.addData(strValue);
        this.numState.addData(strValue);
        this.sampleState.addData(strValue);
    }

    public UnivariateStats populatePMML() {
        // TODO Auto-generated method stub
        return null;
    }
}
