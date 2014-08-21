package ml.shifu.plugin.spark.stats;

import java.util.HashMap;
import java.util.Map;

import com.google.inject.AbstractModule;

import ml.shifu.core.di.module.SimpleModule;
import ml.shifu.core.request.Binding;
import ml.shifu.core.request.Request;
import ml.shifu.core.util.CommonUtils;
/*
 * Modifieds the SimpleModule in shifu-core to look for interfaces in ml.shifu.plugin.spark.stats.interfaces
 */
public class SparkModule extends AbstractModule {
    
    private Map<String, String> bindings = new HashMap<String, String>();

    public SparkModule(){
    }


    public Map<String, String> getBindings() {
        return bindings;
    }

    public void setBindings(Map<String, String> bindings) {
        this.bindings = bindings;
    }

    public void set(String spi, String impl) {
        bindings.put(spi, impl);
    }

    public void set(Binding binding) {
        if (binding != null) {
            bindings.put(binding.getSpi(), binding.getImpl());
        }
    }

    public void set(Request req) {


        this.set(req.getProcessor());

        for (Binding binding : req.getBindings()) {
            this.set(binding);
        }
    }

    public Boolean has(String spi) {
        return bindings.containsKey(spi) && bindings.get(spi) != null;

    }

    @Override
    protected void configure() {

        for (String spiName : bindings.keySet()) {
            Class spi = CommonUtils.getClass("ml.shifu.plugin.spark.stats.interfaces." + spiName);
            Class impl = CommonUtils.getClass(bindings.get(spiName));
            bind(spi).to(impl);
        }
    }
}
