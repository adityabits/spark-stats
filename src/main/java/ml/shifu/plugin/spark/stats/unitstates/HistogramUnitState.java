package ml.shifu.plugin.spark.stats.unitstates;

import java.util.HashMap;
import java.util.Map;

import ml.shifu.plugin.spark.UnitState;

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
            incMapCnt(this.getHistogram(), key);            
    }

    public Map<String, Integer> getHistogram() {
        return this.histogram;
    }
    
    // TODO: make method act on this.histogram
    // add max value check (?)
    private void incMapCnt(Map<String, Integer> map, String key) {
        int cnt = map.containsKey(key) ? map.get(key) : 0;
        map.put(key, cnt + 1);
    }

    public void addData(String strValue) {
        incMapCnt(this.getHistogram(), strValue);
    }

}
