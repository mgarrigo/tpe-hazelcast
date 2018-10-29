package ar.edu.itba.pod.api.combiner;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.concurrent.atomic.AtomicLong;

public class PairSumCombinerFactory<T> implements CombinerFactory<T, Pair<Long, Long>, Pair<Long, Long>> {

    @Override
    public Combiner<Pair<Long, Long>, Pair<Long, Long>> newCombiner(T key ) {
        return new ElementSumCombiner();
    }

    private class ElementSumCombiner extends Combiner<Pair<Long, Long>, Pair<Long, Long>> {

        private AtomicLong firstValue = new AtomicLong(0);
        private AtomicLong secondValue = new AtomicLong(0);

        @Override
        public void combine(Pair<Long, Long> pair) {
            firstValue.addAndGet(pair.getKey());
            secondValue.addAndGet(pair.getValue());
        }

        @Override
        public Pair<Long, Long> finalizeChunk() {
            return new ImmutablePair<>(firstValue.get(), secondValue.get());
        }

        @Override
        public void reset() {
            firstValue = new AtomicLong(0);
            secondValue = new AtomicLong(0);
        }
    }

}
