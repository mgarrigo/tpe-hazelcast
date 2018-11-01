package ar.edu.itba.pod.api.collators;

import com.hazelcast.mapreduce.Collator;
import org.apache.commons.lang3.tuple.Pair;


import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Query5Collator implements Collator<Map.Entry<Pair<String, String>, Integer>, List<Map.Entry<Pair<String, String>, Integer>>> {

	private int n;

	public Query5Collator(int n) {
		this.n = n;
	}

	@Override
	public List<Map.Entry<Pair<String, String>, Integer>> collate(Iterable<Map.Entry<Pair<String, String>, Integer>> values) {
		List<Map.Entry<Pair<String, String>, Integer>> result = new ArrayList<>();

		for (Map.Entry<Pair<String, String>, Integer> value : values) {
			result.add(value);
		}

		/* Order by descending number of percentages, and OACI */
		Comparator<Map.Entry<Pair<String, String>, Integer>> c = (o1, o2) -> {
			int percentageCompare = o2.getValue().compareTo(o1.getValue());
			if (percentageCompare == 0) return o1.getKey().getKey().compareTo(o2.getKey().getKey());
			return percentageCompare;
		};

		result.sort(c);
		return result.stream().limit(n).collect(Collectors.toList());
	}
}
