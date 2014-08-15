package ml.shifu.plugin.spark.stats;

import org.apache.spark.serializer.KryoRegistrator;

import com.esotericsoftware.kryo.Kryo;

public class MyRegistrator implements KryoRegistrator {

    public MyRegistrator() {
        // TODO Auto-generated constructor stub
    }

    @Override
    public void registerClasses(Kryo kryo) {
        kryo.register(ColumnStateArray.class);
        kryo.register(SparkAccumulableWrapper.class);
    }

}
