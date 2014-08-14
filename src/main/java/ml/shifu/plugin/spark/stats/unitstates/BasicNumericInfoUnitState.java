package ml.shifu.plugin.spark.stats.unitstates;

import org.dmg.pmml.Counts;
import org.dmg.pmml.UnivariateStats;

import ml.shifu.plugin.spark.UnitState;

public class BasicNumericInfoUnitState implements UnitState {
    private Double min;
    private Double max;
    private Integer n;
    private Double sumSqr;
    private Double sum;
    
    public BasicNumericInfoUnitState() {
        this.min= Double.POSITIVE_INFINITY;
        this.max= Double.NEGATIVE_INFINITY;
        this.n= 0;
        this.sum= 0.0;
        this.sumSqr= 0.0;
        
    }
    public UnitState getNewBlank() {
        return new BasicNumericInfoUnitState();
    }

    public void merge(UnitState state) throws Exception {
        if(!(state instanceof BasicNumericInfoUnitState))
            throw new Exception("Expected BasicNumericInfoUnitState, got " + state.getClass().toString());
        BasicNumericInfoUnitState newState= (BasicNumericInfoUnitState) state;
        this.max= Math.max(this.max, newState.getMax());
        this.min= Math.min(this.min, newState.getMin());
        this.n= this.n + newState.getN();
        this.sum= this.sum + newState.getSum();
        this.sumSqr= this.sumSqr + newState.getSumSqr();
        
    }

    public void addData(String strValue) {
        Double value= Double.parseDouble(strValue);
        this.n++;
        this.sum+= value;
        this.sumSqr+= Math.pow(value, 2);
        this.max= Math.max(this.max, value);
        this.min= Math.min(this.min, value);
    }

    private Double getMin() {
        return this.min;
    }
    private Double getSumSqr() {
        return this.sumSqr;
    }
    private Double getSum() {
        return this.sum;
    }
    private Integer getN() {
        return this.n;
    }
    private Double getMax() {
        return this.max;
    }
    public Counts populatePMML() {
        
        return null;
        
    }


}
