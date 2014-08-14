package ml.shifu.plugin.spark;

import java.util.ArrayList;
import java.util.List;

import ml.shifu.core.util.Params;
import ml.shifu.plugin.spark.stats.unitstates.FrequencyUnitState;
import ml.shifu.plugin.spark.stats.unitstates.HistogramUnitState;

import org.dmg.pmml.DataField;
import org.dmg.pmml.UnivariateStats;

public class SimpleUnivariateOrdinalState implements ColumnState {
    private FrequencyUnitState freqState;
    private DataField field;
    private Params params;
    
    SimpleUnivariateOrdinalState(DataField field, Params params) {
        this.freqState= new FrequencyUnitState();
        this.field= field;
        this.params= params;
    }

    
    public FrequencyUnitState getFreqState() {
        return this.freqState;
    }

    public ColumnState getNewBlank() {
        return new SimpleUnivariateOrdinalState(this.field, this.params);
    }

    public void merge(ColumnState colState) throws Exception {
        if(!(colState instanceof SimpleUnivariateOrdinalState))
            throw new Exception("Expected UnivariateOrdinalState in merge, got " + colState.getClass().toString());
        SimpleUnivariateOrdinalState newColState= (SimpleUnivariateOrdinalState) colState;
        this.freqState.merge(newColState.getFreqState());
    }

    public void addData(String strValue) {
        this.freqState.addData(strValue);
    }

    public UnivariateStats populatePMML() {
        UnivariateStats univariateStats= new UnivariateStats();
        
        return univariateStats;
    }

}
