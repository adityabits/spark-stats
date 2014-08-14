/*
 * This class holds an array of states for every column in the table.
 */
package ml.shifu.plugin.spark;

import java.util.ArrayList;

import ml.shifu.core.util.Params;

import org.dmg.pmml.DataField;
import org.dmg.pmml.OpType;

public class SimpleUnivariateColumnStateArray {

    private ArrayList<ColumnState> stateArray;
    // private ArrayList<DataField> dataFields;
    // private String delimiter;
    
    public ColumnStateArray(ColumnStateArray initValue) {
        // copy ColumnStates from initValue
        this.stateArray= new ArrayList<ColumnState>();
        for(ColumnState s: initValue.getStateArray()) {
            this.stateArray.add(s.getNewBlank());
        }
    }

    public ColumnStateArray(ArrayList<DataField> dataFields, Params param) {
        // Only for Simple Univariate (for now)
        for(DataField field: dataFields) {
            if(field.getOptype().equals(OpType.CATEGORICAL))
                this.stateArray.add(new SimpleUnivariateDiscrState());
            else if(field.getOptype().equals(OpType.CONTINUOUS))
                this.stateArray.add(new SimpleUnivariateContState());
        }
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

    public void addData(String row) {
        // TODO Auto-generated method stub        
    }
    
    public ArrayList<ColumnState> getStateArray() {
        return stateArray;
    }

}
