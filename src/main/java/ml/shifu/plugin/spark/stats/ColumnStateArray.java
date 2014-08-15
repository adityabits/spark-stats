/*
 * This class holds an array of states for every column in the table.
 */
package ml.shifu.plugin.spark.stats;

import java.util.ArrayList;
import java.util.List;

import ml.shifu.core.util.Params;
import ml.shifu.plugin.spark.stats.columnstates.SimpleUnivariateContState;
import ml.shifu.plugin.spark.stats.columnstates.SimpleUnivariateDiscrState;
import ml.shifu.plugin.spark.stats.columnstates.SimpleUnivariateOrdinalState;
import ml.shifu.plugin.spark.stats.interfaces.ColumnState;

import org.dmg.pmml.DataField;
import org.dmg.pmml.ModelStats;
import org.dmg.pmml.OpType;

import com.google.common.base.Splitter;

public class ColumnStateArray implements java.io.Serializable {

    private List<ColumnState> stateArray;
    private String delimiter;
    
    public ColumnStateArray(ColumnStateArray initValue) {
        // copy ColumnStates from initValue
        this.stateArray= new ArrayList<ColumnState>();
        for(ColumnState s: initValue.getStateArray()) {
            this.stateArray.add(s.getNewBlank());
        }
        this.delimiter= initValue.delimiter;
    }

    public ColumnStateArray(List<DataField> dataFields, Params params) {
        
        // Only for Simple Univariate (for now)
        this.stateArray= new ArrayList<ColumnState>();
        for(DataField field: dataFields) {
            if(field.getOptype().equals(OpType.CATEGORICAL))
                this.stateArray.add(new SimpleUnivariateDiscrState(field.getName().getValue(), params));
            else if(field.getOptype().equals(OpType.CONTINUOUS))
                this.stateArray.add(new SimpleUnivariateContState(field.getName().getValue(), params));
            else if(field.getOptype().equals(OpType.ORDINAL))
                this.stateArray.add(new SimpleUnivariateOrdinalState(field.getName().getValue(), params));
        }
        this.delimiter= params.get("delimiter", ",").toString();
    }

    public ColumnStateArray merge(ColumnStateArray stateArray2) throws Exception {
        System.out.println("Merging data");
        if(stateArray2.getStateArray().size() != this.stateArray.size())
            throw new Exception("Sizes of state arrays don't match");
        int index= 0;
        for(ColumnState colState: stateArray2.getStateArray()) {
            this.stateArray.get(index).merge(colState);
            index++;
        }
        return this;
    }

    public void addData(String line) {
        int index= 0;
        for(String strValue: Splitter.on(delimiter).split(line)) {
            this.stateArray.get(index).addData(strValue);
            index++;
        }
        return;
    }
    
    public List<ColumnState> getStateArray() {
        return stateArray;
    }

    public ModelStats getModelStats() {
        ModelStats modelStats= new ModelStats();
        for(ColumnState state: this.stateArray) {
            modelStats.withUnivariateStats(state.getUnivariateStats());
        }
        return modelStats;
    }

}
