/*
 * This class holds a collection of unit states to be maintained for a particular column in the table.
 */
package ml.shifu.plugin.spark;

import java.util.List;

import org.dmg.pmml.UnivariateStats;


public interface ColumnState {
    
    public ColumnState getNewBlank();
    public void merge(ColumnState colState) throws Exception;
    public void addData(String strValue);
    public UnivariateStats populatePMML();
    
}
