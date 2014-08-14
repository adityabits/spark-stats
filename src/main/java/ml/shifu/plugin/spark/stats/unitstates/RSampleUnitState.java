package ml.shifu.plugin.spark.stats.unitstates;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import ml.shifu.plugin.spark.UnitState;

/*
 * Maintains a reservoir sample
 */
public class RSampleUnitState implements UnitState {
    private List<Double> samples;
    private int maxSize;
    private Random intRand;
    private int n;
    
    public RSampleUnitState(int maxSize) {
        this.samples= new ArrayList<Double>();
        this.maxSize= maxSize;
        this.intRand= new Random();
        this.n= 0;
    }
    
    public UnitState getNewBlank() {
        return new RSampleUnitState(this.maxSize);
    }

    public void merge(UnitState state) throws Exception {
        if(!(state instanceof RSampleUnitState))
            throw new Exception("Expected RSampleUnitState, got " + state.getClass().toString());
        RSampleUnitState newState= (RSampleUnitState) state;
        
        for(Double sample: newState.getSamples()) {
            // TODO: NOT correct. Use weighted reservoir sampling based on n.
            addSample(sample);
        }
            
    }

    public List<Double> getSamples() {
        return this.samples;
    }
    
    public void addData(String strValue) {
        addSample(Double.parseDouble(strValue));
    }
    
    public void addSample(Double value) {
        this.n++;
        if(this.samples.size() < this.maxSize)
            this.samples.add(value);
        else {
            // inclusive range for nextInt()
            int position= intRand.nextInt(n+1);
            if(position < this.maxSize ) {
                // include element in sample
                samples.set(position, value);
            }
        }
    }

}
