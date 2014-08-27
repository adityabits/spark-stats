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
    private Map<Object, Integer> histogram;
    private int maxSize;
    
    public HistogramUnitState(int maxSize) {
        this.histogram= new HashMap<Object, Integer>();
        this.maxSize= maxSize;
    }
    
    public UnitState getNewBlank() {
        return new HistogramUnitState(maxSize);
    }

    public void merge(UnitState state) throws Exception {
        if(!(state instanceof HistogramUnitState))
            throw new Exception("Expected HistogramUnitState, got " + state.getClass().toString());
        HistogramUnitState newState= (HistogramUnitState) state;
        
        for(Object key: newState.getHistogram().keySet()) 
            incMapCnt(key, newState.getHistogram().get(key));            
    }

    public Map<Object, Integer> getHistogram() {
        return this.histogram;
    }
    
    private void incMapCnt(Object key, int by) {
        int cnt = this.histogram.containsKey(key) ? this.histogram.get(key) : 0;
        if(cnt==0 && this.histogram.size() >= maxSize)
            return;
        this.histogram.put(key, cnt + by);
    }

    public void addData(Object objValue) {
        incMapCnt(objValue, 1);
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

}
