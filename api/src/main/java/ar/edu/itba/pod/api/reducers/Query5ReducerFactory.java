package ar.edu.itba.pod.api.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import org.apache.commons.lang3.tuple.Pair;

import java.util.concurrent.atomic.AtomicLong;

public class Query5ReducerFactory implements ReducerFactory<Pair<String, String>, Pair<Long, Long>, Integer> {

	@Override
	public Reducer<Pair<Long, Long>, Integer> newReducer(Pair<String, String> pair) {
		return new InternationalMovementsPercentageReducer();
	}

	private class InternationalMovementsPercentageReducer extends Reducer<Pair<Long, Long>, Integer> {

		private AtomicLong internationalMovements;
		private AtomicLong totalMovements;

		@Override
		public void beginReduce () {
			internationalMovements = new AtomicLong(0);
			totalMovements = new AtomicLong(0);
		}

		@Override
		public void reduce(Pair<Long, Long> pair) {
//			System.out.println(String.format("international = %s - total = %s", internationalMovements, totalMovements));
			internationalMovements.addAndGet(pair.getKey());
			totalMovements.addAndGet(pair.getValue());
		}

		@Override
		public Integer finalizeReduce() {
//			System.out.println(String.format("FinalizeReduce: %s", (int) Math.floor((double) internationalMovements.get() / (double) totalMovements.get())));
			return (int) Math.floor(100.0 * (double) internationalMovements.get() / (double) totalMovements.get());
		}
	}
}
