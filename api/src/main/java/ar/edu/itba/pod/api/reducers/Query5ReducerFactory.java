package ar.edu.itba.pod.api.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import org.apache.commons.lang3.tuple.Pair;

import java.util.concurrent.atomic.AtomicLong;

public class Query5ReducerFactory implements ReducerFactory<Pair<String, String>, Boolean, Integer> {

    @Override
    public Reducer<Boolean, Integer> newReducer(Pair<String, String> pair) {
        return new InternationalMovementsPercentageReducer();
    }

    private class InternationalMovementsPercentageReducer extends Reducer<Boolean, Integer> {

    	private AtomicLong internationalMovements;
        private AtomicLong totalMovements;

        @Override
        public void beginReduce () {
            internationalMovements = new AtomicLong(0);
            totalMovements = new AtomicLong(0);
        }

        @Override
        public void reduce(Boolean value) {
            totalMovements.getAndAdd(1L);
            if (value) internationalMovements.getAndAdd(1L);
        }

        @Override
        public Integer finalizeReduce() {
            return (int) Math.floor((double) internationalMovements.get() / (double) totalMovements.get());
        }
    }
}
