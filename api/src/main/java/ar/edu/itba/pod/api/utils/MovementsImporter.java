package ar.edu.itba.pod.api.utils;

import ar.edu.itba.pod.api.models.Movement;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MultiMap;

import java.util.Collection;

public class MovementsImporter implements Importer<Movement> {

	public MovementsImporter() {
	}

	@Override
	public void importToIList(IList<Movement> iList, Collection<Movement> collection) {
		iList.addAll(collection);
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
