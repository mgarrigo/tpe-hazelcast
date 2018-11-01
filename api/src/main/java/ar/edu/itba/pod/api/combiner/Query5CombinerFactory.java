package ar.edu.itba.pod.api.combiner;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.concurrent.atomic.AtomicLong;

public class Query5CombinerFactory<T> implements CombinerFactory<T, Boolean, Pair<Long, Long>> {

    @Override
    public Combiner<Boolean, Pair<Long, Long>> newCombiner(T key ) {
        return new Query5Combiner();
    }

    private class Query5Combiner extends Combiner<Boolean, Pair<Long, Long>> {

		private AtomicLong internationalMovements = new AtomicLong(0);
		private AtomicLong totalMovements = new AtomicLong(0);

        @Override
        public void combine(Boolean international) {
        	totalMovements.addAndGet(1L);
            if (international) internationalMovements.addAndGet(1L);
        }

        @Override
        public Pair<Long, Long> finalizeChunk() {
            return new ImmutablePair<>(internationalMovements.get(), totalMovements.get());
        }

        @Override
        public void reset() {
            internationalMovements = new AtomicLong(0);
            totalMovements = new AtomicLong(0);
        }
    }

}
