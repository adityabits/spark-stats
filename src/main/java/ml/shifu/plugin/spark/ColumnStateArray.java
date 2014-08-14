/*
 * This class holds an array of states for every column in the table.
 */
package ml.shifu.plugin.spark;

import java.util.ArrayList;
import java.util.List;

import ml.shifu.core.util.Params;

import org.dmg.pmml.DataField;
import org.dmg.pmml.ModelStats;
import org.dmg.pmml.OpType;
import org.dmg.pmml.UnivariateStats;

import com.google.common.base.Splitter;

public class ColumnStateArray {

    private List<ColumnState> stateArray;
    //private ArrayList<DataField> dataFields;
    private String delimiter;
    
    public ColumnStateArray(ColumnStateArray initValue) {
        // copy ColumnStates from initValue
        this.stateArray= new ArrayList<ColumnState>();
        for(ColumnState s: initValue.getStateArray()) {
            this.stateArray.add(s.getNewBlank());
        }
    }

    public ColumnStateArray(List<DataField> dataFields, Params params) {
        // Only for Simple Univariate (for now)
        for(DataField field: dataFields) {
            if(field.getOptype().equals(OpType.CATEGORICAL))
                this.stateArray.add(new SimpleUnivariateDiscrState(field, params));
            else if(field.getOptype().equals(OpType.CONTINUOUS))
                this.stateArray.add(new SimpleUnivariateContState(field, params));
        }
        this.delimiter= params.get("delimiter", ",").toString();
    }

    public ColumnStateArray merge(ColumnStateArray stateArray2) throws Exception {
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

    public ModelStats populatePMML() {
        ModelStats modelStats= new ModelStats();
        for(ColumnState state: this.stateArray) {
            modelStats.withUnivariateStats(state.populatePMML());
        }
        return modelStats;
    }

}
