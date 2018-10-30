package ar.edu.itba.pod.api.collators;

import com.hazelcast.mapreduce.Collator;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

public class OrderNumberAndOACICollator implements Collator<Map.Entry<String, Long>,
        Set<Map.Entry<Pair<String, String>, Long>>> {

    @Override
    public Set<Map.Entry<Pair<String, String>, Long>> collate(Iterable<Map.Entry<String, Long>> iterable) {
        Map<Long, List<String>> map = new HashMap<>();
        iterable.forEach(e -> {
            if (e.getValue() > 0){
                if (map.containsKey(e.getValue())){
                    List<String> l = map.get(e.getValue());
                    l.add(e.getKey());
                }else{
                    List<String> l = new LinkedList<>();
                    l.add(e.getKey());
                    map.put(e.getValue(), l);
                }
            }
        });

        Set<Map.Entry<Pair<String, String>, Long>> results = new TreeSet<>((o1, o2) -> {
            if (!o1.getValue().equals(o2.getValue())){
                return -(o1.getValue().compareTo(o2.getValue())); // reverse values
            }
            String s1 = o1.getKey().getKey() + o1.getKey().getValue();
            String s2 = o2.getKey().getKey() + o2.getKey().getValue();
            return s1.compareTo(s2);
        });

        map.forEach((number, list) -> list.forEach(oaci -> {
            list.forEach(oaci2 -> {
                if (!oaci.equals(oaci2)){
                    Pair<String, String> pair;
                    if (oaci.compareTo(oaci2) < 0)
                        pair = new ImmutablePair<>(oaci, oaci2);
                    else
                        pair = new ImmutablePair<>(oaci2, oaci);
                    results.add(new AbstractMap.SimpleEntry<>(pair, number));
                }
            });
        }));

        return results;
    }
}
