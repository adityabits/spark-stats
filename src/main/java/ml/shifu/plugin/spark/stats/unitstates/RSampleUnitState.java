package ml.shifu.plugin.spark.stats.unitstates;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import ml.shifu.core.container.NumericalValueObject;
import ml.shifu.plugin.spark.stats.SerializedNumericalValueObject;


/*
 * Maintains a reservoir sample. This class is not thread safe due to the use of sortSamples() method.
 */
public class RSampleUnitState<T> implements Serializable {

    
    private static final long serialVersionUID = 1L;
    protected List<T> samples;
    protected boolean sorted;
    protected int maxSize;
    protected Random intRand;
    protected int n;
    
    public RSampleUnitState(int maxSize) {
        this.samples= new ArrayList<T>();
        this.sorted= false;
        this.maxSize= maxSize;
        this.intRand= new Random();
        this.n= 0;
    }
    
    
    public void merge(RSampleUnitState<T> otherState) throws Exception {
        for(T otherSample: otherState.getSamples()) {
            // TODO: NOT correct. Use weighted reservoir sampling based on n.
            addSample(otherSample);
        }
        n+= otherState.n;
        sorted= false;
    }
    
    public List<T> getSamples() {
        return samples;
    }
        
    
    public void addSample(T sample) {
        n++;
        if(samples.size() < maxSize)
            samples.add(sample);
        else {
            // inclusive range for nextInt()
            int position= intRand.nextInt(n+1);
            if(position < maxSize ) {
                // include element in sample
                samples.set(position, sample);
            }
        }
    }
    
    
}
