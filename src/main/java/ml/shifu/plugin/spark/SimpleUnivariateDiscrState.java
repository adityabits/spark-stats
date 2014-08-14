package ml.shifu.plugin.spark;

import java.util.ArrayList;

import org.dmg.pmml.DataField;
import org.dmg.pmml.UnivariateStats;

import ml.shifu.core.util.Params;
import ml.shifu.plugin.spark.stats.unitstates.FrequencyUnitState;
import ml.shifu.plugin.spark.stats.unitstates.HistogramUnitState;

public class SimpleUnivariateDiscrState implements ColumnState {
    private FrequencyUnitState freqState;
    private HistogramUnitState histState;
    private DataField field;
    private Params params;
    
    SimpleUnivariateDiscrState(DataField field, Params params) {
        this.freqState= new FrequencyUnitState();
        this.histState= new HistogramUnitState();
        this.field= field;
        this.params= params;
    }
    
    public ColumnState getNewBlank() {
        return new SimpleUnivariateDiscrState(this.field, this.params);
    }

    public void merge(ColumnState colState) throws Exception {
        if(!(colState instanceof SimpleUnivariateDiscrState))
            throw new Exception("Expected UnivariateDiscrState in merge, got " + colState.getClass().toString());
        SimpleUnivariateDiscrState newColState= (SimpleUnivariateDiscrState) colState;
        this.freqState.merge(newColState.getFreqState());
        this.histState.merge(newColState.getHistState());
    }

    private FrequencyUnitState getFreqState() {
        return this.freqState;
    }

    private HistogramUnitState getHistState() {
        return this.histState;
    }

    public void addData(String strValue) {
        this.freqState.addData(strValue);
        this.histState.addData(strValue);
    }

    public UnivariateStats populatePMML() {
        UnivariateStats univariateStats= new UnivariateStats();

        return univariateStats;
    }

}
