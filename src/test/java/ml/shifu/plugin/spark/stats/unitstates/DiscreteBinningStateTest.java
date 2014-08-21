package ml.shifu.plugin.spark.stats.unitstates;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;
import ml.shifu.plugin.spark.stats.SerializedCategoricalValueObject;

import org.dmg.pmml.DiscrStats;
import org.dmg.pmml.Extension;
import org.dmg.pmml.UnivariateStats;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class DiscreteBinningStateTest {
    DiscreteBinningUnitState state= new DiscreteBinningUnitState();
    
    @BeforeClass
    public void populateStatetest() {
        state.addData(new SerializedCategoricalValueObject("a", (double) 1, true));
        state.addData(new SerializedCategoricalValueObject("a", (double) 2, false));
        state.addData(new SerializedCategoricalValueObject("b", (double) 2, true));
        state.addData(new SerializedCategoricalValueObject("b", (double) -1, false));
        state.addData(new SerializedCategoricalValueObject("b", (double) 0, false));
        

    }
    
    // TODO: Null/ ClassCast tests
    
    @Test
    public void testCategoryHistNeg() {
        Map<String, Integer> m= state.getCategoryHistNeg();
        Assert.assertEquals(m.get("a"), (Integer)1);
        Assert.assertEquals(m.get("b"), (Integer)2);
    }

    @Test
    public void testCategoryHistPos() {
        Map<String, Integer> m= state.getCategoryHistPos();
        Assert.assertEquals(m.get("a"), (Integer)1);
        Assert.assertEquals(m.get("b"), (Integer)1);
    }

    @Test
    public  void testCategoryWeightPos() {
        Map<String, Double> m= state.getCategoryWeightPos();
        Assert.assertEquals(m.get("a"), 1.0);
        Assert.assertEquals(m.get("b"), 2.0);
        
    }

    @Test
    public  void testCategoryWeightNeg() {
        Map<String, Double> m= state.getCategoryWeightNeg();
        Assert.assertEquals(m.get("a"), 2.0);
        Assert.assertEquals(m.get("b"), -1.0);
    }
    
    @Test
    public void testCategorySet() {
        Set<String> s= state.getCategorySet();
        Assert.assertEquals(s.size(), 2);
        Assert.assertTrue(s.contains("a"));
        Assert.assertTrue(s.contains("b"));
    }

    
    @Test
    public void testPMML() {
        UnivariateStats us= new UnivariateStats();
        state.populateUnivariateStats(us, null);
        DiscrStats ds= us.getDiscrStats();
        List<Extension>extList= ds.getExtensions();
        // assert length
        Assert.assertEquals(extList.size(), 5);
        List<String> names= new ArrayList<String>();
        List<String> values= new ArrayList<String>();
        
        for(Extension ext: extList) {
            names.add(ext.getName());
            values.add(ext.getValue());
        }
        
        //System.out.println("" + values + ", " + names);
        values.get(names.indexOf("BinCountPos"));
        // TODO: parse values into doubles and compare
        Assert.assertTrue(names.contains("BinCountPos"));
        Assert.assertTrue(names.contains("BinCountPos"));
        Assert.assertTrue(names.contains("BinCountPos"));
        Assert.assertTrue(names.contains("BinCountPos"));
        Assert.assertTrue(names.contains("BinCountPos"));
    }
    
    
}
