package ar.edu.itba.pod.api.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import org.apache.commons.lang3.tuple.Pair;

import java.util.concurrent.atomic.AtomicLong;

public class ThousandsCountReducerFactory implements ReducerFactory<String, Long, Long> {

    @Override
    public Reducer<Long, Long> newReducer(String key) {
        return new ThousandMovementCountReducer();
    }

    private class ThousandMovementCountReducer extends Reducer<Long, Long> {

        private AtomicLong sum;

        @Override
        public void beginReduce () {
            sum = new AtomicLong(0);
        }

        @Override
        public void reduce(Long value) {
            sum.getAndAdd(value);
        }

        @Override
        public Long finalizeReduce() {
            return (sum.get() / 1000) * 1000;
        }
    }
}
