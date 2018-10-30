package ar.edu.itba.pod.api.collators;

import com.hazelcast.mapreduce.Collator;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class ProvinceCollator implements Collator<Map.Entry<Pair<String, String>, Long>,
        Set<Map.Entry<Pair<String, String>, Long>>> {

    private Long minimum;

    public ProvinceCollator(Long minimum){
        this.minimum = minimum;
    }

    @Override
    public Set<Map.Entry<Pair<String, String>, Long>> collate(Iterable<Map.Entry<Pair<String, String>, Long>> iterable) {
        Set<Map.Entry<Pair<String, String>, Long>> results = new TreeSet<>((o1, o2) -> {
            if (!o1.getValue().equals(o2.getValue())){
                return -(o1.getValue().compareTo(o2.getValue()));
            }
            String s1 = o1.getKey().getKey() + o1.getKey().getValue();
            String s2 = o2.getKey().getKey() + o2.getKey().getValue();
            return s1.compareTo(s2);
        });
        iterable.forEach(e -> {
            if (e.getValue() >= minimum){
                results.add(e);
            }
        });
        return results;
    }
}
