package ar.edu.itba.pod.api.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

public class PairCountReducerFactory<T extends Serializable> implements ReducerFactory<Pair<T, T>, Pair<Long, Long>, Pair<Long, Long>> {


    @Override
    public Reducer<Pair<Long, Long>, Pair<Long, Long>> newReducer(Pair<T, T> tContainer) {
        return new PairCountReducer();
    }

    private class PairCountReducer extends Reducer<Pair<Long, Long>, Pair<Long, Long>> {

        private AtomicLong fromOrigin;
        private AtomicLong toDestiny;

        @Override
        public void beginReduce () {
            fromOrigin = new AtomicLong(0);
            toDestiny = new AtomicLong(0);
        }

        @Override
        public void reduce(Pair<Long, Long> longContainer) {
            fromOrigin.getAndAdd(longContainer.getKey());
            toDestiny.getAndAdd(longContainer.getValue());
        }

        @Override
        public Pair<Long, Long> finalizeReduce() {
            return new ImmutablePair<>(fromOrigin.get(), toDestiny.get());
        }
    }
}
