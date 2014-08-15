/*
 * This class holds a collection of unit states to be maintained for a particular column in the table.
 */
package ml.shifu.plugin.spark.stats.interfaces;

import java.util.List;

import ml.shifu.core.util.Params;

import org.dmg.pmml.UnivariateStats;


public abstract class ColumnState implements java.io.Serializable {
    protected List<UnitState> states;
    protected Params params;
    protected String fieldName;

    abstract public ColumnState getNewBlank();
    
    public void merge(ColumnState colState) throws Exception {
        checkClass(colState);
        int index= 0;
        for(UnitState state: colState.getStates()) {
            this.states.get(index).merge(state);
            index++;
        }
    }
        
    abstract public void checkClass(ColumnState colState) throws Exception;
    
    public void addData(String strValue) {
        for(UnitState state: this.states) 
            state.addData(strValue);
    }

    public List<UnitState> getStates() {
        return this.states;
    }

    public UnivariateStats getUnivariateStats() {
        UnivariateStats univariateStats= new UnivariateStats();
        for(UnitState state:this.states)
            state.populateUnivariateStats(univariateStats, this.params);
        return univariateStats;
    }
    
}
