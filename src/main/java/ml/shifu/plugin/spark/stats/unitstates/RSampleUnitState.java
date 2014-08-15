package ml.shifu.plugin.spark.stats.unitstates;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.dmg.pmml.NumericInfo;
import org.dmg.pmml.Quantile;
import org.dmg.pmml.UnivariateStats;

import ml.shifu.core.di.builtin.QuantileCalculator;
import ml.shifu.core.util.Params;
import ml.shifu.plugin.spark.stats.interfaces.UnitState;

/*
 * Maintains a reservoir sample. This class is not thread safe due to the use of sortSamples() method.
 */
public class RSampleUnitState implements UnitState {
    private List<Double> samples;
    private boolean sorted;
    private int maxSize;
    private Random intRand;
    private int n;
    
    public RSampleUnitState(int maxSize) {
        this.samples= new ArrayList<Double>();
        this.sorted= false;
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
        this.sorted= false;
    }

    public void sortSamples() {
        if(sorted==false)
            Collections.sort(this.samples);
        sorted= true;
    }
    public List<Double> getSamples() {
        return this.samples;
    }
        
    public void addData(String strValue) {
        this.sorted= false;
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
    
    
    public void populateUnivariateStats(UnivariateStats univariateStats, Params params) {
        this.sortSamples();
        NumericInfo numInfo= univariateStats.getNumericInfo();
        if(numInfo==null)
            numInfo= new NumericInfo();
        
        int numQuantiles= Integer.parseInt(params.get("numQuantiles", "11").toString());        
        numInfo.withQuantiles(getQuantiles(numQuantiles));
        numInfo.withInterQuartileRange(getInterQuantileRange());
        numInfo.withMedian(getMedian());
        univariateStats.withNumericInfo(numInfo);
    }
    
    public List<Quantile> getQuantiles(int num) {
        this.sortSamples();
        QuantileCalculator quantileCalculator = new QuantileCalculator();
        return quantileCalculator.getEvenlySpacedQuantiles(this.samples, num);
    }
    
    public Double getMedian() {
        if(this.samples.size() == 0)
            return null;
        this.sortSamples();
        return this.samples.get(this.samples.size()/2);
    }
    
    public Double getInterQuantileRange() {
        this.sortSamples();
        int n= this.samples.size();
        if(n==0)
            return null;
        return this.samples.get((int) Math.floor(n * 0.75)) - this.samples.get((int) Math.floor(n * 0.25));
    }

}
