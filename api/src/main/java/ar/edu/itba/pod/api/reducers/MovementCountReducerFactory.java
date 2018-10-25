package ar.edu.itba.pod.api.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class MovementCountReducerFactory implements ReducerFactory<String, Long, Long> {

    @Override
    public Reducer<Long, Long> newReducer(String key) {
        return new MovementCountReducer();
    }

    private class MovementCountReducer extends Reducer<Long, Long> {

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
            return sum.get();
        }
    }
}
