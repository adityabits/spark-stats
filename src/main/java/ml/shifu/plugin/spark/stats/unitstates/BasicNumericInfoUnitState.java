package ml.shifu.plugin.spark.stats.unitstates;

import org.dmg.pmml.ContStats;
import org.dmg.pmml.Counts;
import org.dmg.pmml.NumericInfo;
import org.dmg.pmml.UnivariateStats;

import ml.shifu.core.util.CommonUtils;
import ml.shifu.core.util.Params;
import ml.shifu.plugin.spark.stats.interfaces.UnitState;

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
        if(CommonUtils.isValidNumber(strValue)) {
            Double value= Double.valueOf(strValue);
            this.n++;
            this.sum+= value;
            this.sumSqr+= Math.pow(value, 2);
            this.max= Math.max(this.max, value);
            this.min= Math.min(this.min, value);
        }
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
    
    public NumericInfo getNumericInfo() {
        NumericInfo numInfo= new NumericInfo();
        numInfo.withMaximum(this.max);
        numInfo.withMinimum(this.min);
        if(n==0 || sum.isInfinite() || sumSqr.isInfinite())
            return numInfo;
        
        numInfo.setMean(sum/n);
        Double EPS= 1e-6;
        // Why the + EPS and -1?
        double stdDev = Math.sqrt((this.sumSqr - (this.sum * this.sum) / this.n + EPS)
                / (this.n - 1));
        numInfo.setStandardDeviation(stdDev);
        // does not set median and quartile range
        return numInfo;
    }
    
    public void populateUnivariateStats(UnivariateStats univariateStats, Params params) {
        ContStats contStats= univariateStats.getContStats();
        if(contStats==null)
            contStats= new ContStats();
        
        contStats.setTotalValuesSum(this.sum);
        contStats.setTotalSquaresSum(this.sumSqr);
        univariateStats.withContStats(contStats);
        
        NumericInfo numInfo= univariateStats.getNumericInfo();
        if(numInfo==null)
            numInfo= new NumericInfo();
        numInfo.withMaximum(this.max);
        numInfo.withMinimum(this.min);
        if(n==0 || sum.isInfinite() || sumSqr.isInfinite()) {
            univariateStats.withNumericInfo(numInfo);
            return;
        }
            
        numInfo.setMean(sum/n);
        Double EPS= 1e-6;
        // Why the + EPS and -1?
        double stdDev = Math.sqrt((this.sumSqr - (this.sum * this.sum) / this.n + EPS)
                / (this.n - 1));
        numInfo.setStandardDeviation(stdDev);
        univariateStats.withNumericInfo(numInfo);
        // does not set median and quartile range
        
    }
    
    public ContStats getContStats() {
        ContStats contStats= new ContStats();
        contStats.setTotalValuesSum(this.sum);
        contStats.setTotalSquaresSum(this.sumSqr);
        return contStats;
    }

}
