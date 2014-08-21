package ml.shifu.plugin.spark.stats.interfaces;
/*
 * includes addData(double d) for optimization purposes
 */
public interface DoubleUnitState extends UnitState {
    public void addData(Double dVal);
}
