package ar.edu.itba.pod.api.collators;

import com.hazelcast.mapreduce.Collator;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

public class PairMovementCollator implements Collator<Map.Entry<Pair<String, String>, Pair<Long, Long>>,
        Set<Map.Entry<Pair<String, String>, Pair<Long, Long>>>> {

    @Override
    public Set<Map.Entry<Pair<String, String>, Pair<Long, Long>>> collate(
            Iterable<Map.Entry<Pair<String, String>, Pair<Long, Long>>> iterable) {
        Set<Map.Entry<Pair<String, String>, Pair<Long, Long>>> results = new TreeSet<>((o1, o2) -> {
            String s1 = o1.getKey().getKey() + o1.getKey().getValue();
            String s2 = o2.getKey().getKey() + o2.getKey().getValue();
            return s1.compareTo(s2);
        });
        iterable.forEach(results::add);
        return results;
    }
}
