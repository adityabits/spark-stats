package ml.shifu.plugin.spark.stats.unitstates;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.dmg.pmml.Array;
import org.dmg.pmml.DiscrStats;
import org.dmg.pmml.UnivariateStats;

import ml.shifu.core.util.Params;
import ml.shifu.plugin.spark.stats.interfaces.UnitState;

public class HistogramUnitState implements UnitState {
    private Map<String, Integer> histogram;
    
    public HistogramUnitState() {
        this.histogram= new HashMap<String, Integer>();
    }
    
    public UnitState getNewBlank() {
        return new HistogramUnitState();
    }

    public void merge(UnitState state) throws Exception {
        if(!(state instanceof HistogramUnitState))
            throw new Exception("Expected HistogramUnitState, got " + state.getClass().toString());
        HistogramUnitState newState= (HistogramUnitState) state;
        
        for(String key: newState.getHistogram().keySet()) 
            incMapCnt(key, newState.getHistogram().get(key));            
    }

    public Map<String, Integer> getHistogram() {
        return this.histogram;
    }
    
    // TODO: add max value check (?)
    private void incMapCnt(String key, int by) {
        int cnt = this.histogram.containsKey(key) ? this.histogram.get(key) : 0;
        this.histogram.put(key, cnt + by);
    }

    public void addData(String strValue) {
        incMapCnt(strValue, 1);
    }
    
    public void populateUnivariateStats(UnivariateStats univariateStats, Params params) {
        DiscrStats discrStats= univariateStats.getDiscrStats();
        if(discrStats==null)
            discrStats= new DiscrStats();
        
        Array countArray = new Array();
        countArray.setType(Array.Type.INT);
        countArray.setN(this.histogram.size());
        countArray.setValue(StringUtils.join(this.histogram.values(), " "));

        Array stringArray = new Array();
        stringArray.setType(Array.Type.STRING);
        stringArray.setN(this.histogram.size());
        stringArray.setValue(StringUtils.join(this.histogram.keySet(), " "));

        discrStats.withArrays(countArray, stringArray);
        univariateStats.withDiscrStats(discrStats);
    }

    public DiscrStats getDiscrStats() {
        DiscrStats discrStats= new DiscrStats();
        Array countArray = new Array();
        countArray.setType(Array.Type.INT);
        countArray.setN(this.histogram.size());
        countArray.setValue(StringUtils.join(this.histogram.values(), " "));

        Array stringArray = new Array();
        stringArray.setType(Array.Type.STRING);
        stringArray.setN(this.histogram.size());
        stringArray.setValue(StringUtils.join(this.histogram.keySet(), " "));

        discrStats.withArrays(countArray, stringArray);
        return discrStats;

    }

}
