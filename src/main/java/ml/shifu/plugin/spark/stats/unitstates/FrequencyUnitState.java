package ml.shifu.plugin.spark.stats.unitstates;

import org.dmg.pmml.Counts;
import org.dmg.pmml.UnivariateStats;

import ml.shifu.core.util.Params;
import ml.shifu.plugin.spark.stats.interfaces.UnitState;

public class FrequencyUnitState implements UnitState {
    private double totalFreq = 0;
    private double missingFreq = 0;
    private double invalidFreq = 0;

    public FrequencyUnitState() {
        this.totalFreq= 0.0;
        this.missingFreq= 0.0;
        this.invalidFreq= 0.0;
                
    }
    public UnitState getNewBlank() {
        return new FrequencyUnitState();
    }

    public void merge(UnitState state) throws Exception {
        if(!(state instanceof FrequencyUnitState))
            throw new Exception("Expected FrequencyUnitState, got " + state.getClass().toString());
        FrequencyUnitState newState= (FrequencyUnitState) state;
        this.totalFreq+= newState.getTotal();
        this.missingFreq+= newState.getMissing();
        this.invalidFreq+= newState.getInvalid();
    }

    public void addData(String strValue) {
        if(strValue==null || strValue.length()==0)
            this.missingFreq++;
        else if(!isNumeric(strValue))
            this.invalidFreq++;
        this.totalFreq++;

    }

    private double getInvalid() {
        return this.invalidFreq;
    }
    private double getMissing() {
        return this.missingFreq;
    }
    private double getTotal() {
        return this.totalFreq;
    }
    
    
    private boolean isNumeric(String str)  
    {  
        try  {  
            Double.parseDouble(str);  
        }  catch(NumberFormatException e)  {
            return false;  
        }  
        return true;  
    }
    
    public void populateUnivariateStats(UnivariateStats univariateStats, Params params) {
        Counts counts= univariateStats.getCounts();
        if(counts==null) {
            counts= new Counts();
        }
        counts.withInvalidFreq(this.invalidFreq);
        counts.withMissingFreq(this.missingFreq);
        counts.withTotalFreq(this.totalFreq);        
        
        univariateStats.withCounts(counts);
    }
    
    public Counts getCounts() {
        // TODO: Does not compute cardinality
        Counts counts= new Counts();
        counts.withInvalidFreq(this.invalidFreq);
        counts.withMissingFreq(this.missingFreq);
        counts.withTotalFreq(this.totalFreq);
        return counts;
    }
    
}
