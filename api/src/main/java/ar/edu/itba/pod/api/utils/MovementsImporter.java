package ar.edu.itba.pod.api.utils;

import ar.edu.itba.pod.api.models.Movement;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MultiMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

public class MovementsImporter implements Importer<Movement> {

	public MovementsImporter() {
	}

	@Override
	public void importToIList(IList<Movement> iList, Collection<Movement> collection) {
        int LIMIT = 1000;
        List<Movement> list = new ArrayList<>();

        for (Movement m : collection){
            list.add(m);
            if (list.size() == LIMIT){
                iList.addAll(list);
                list.clear();
            }
        }
        // Add remaining
        iList.addAll(list);

        //collection.parallelStream().forEach(iList::add););
	}

	@Override
	public void importToIMap(IMap<String, Movement> iMap, Collection<Movement> collection, String field) {
		collection.parallelStream().forEach(movement -> iMap.put(Getters.getField(movement, field), movement));
	}

	@Override
	public void importToMultiMap(MultiMap<String, Movement> multiMap, Collection<Movement> collection, String field) {
		collection.parallelStream().forEach(movement -> multiMap.put(Getters.getField(movement, field), movement));
	}
}
