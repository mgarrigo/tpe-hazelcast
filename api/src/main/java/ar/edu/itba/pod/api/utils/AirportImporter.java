package ar.edu.itba.pod.api.utils;

import ar.edu.itba.pod.api.models.Airport;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MultiMap;

import java.util.Collection;

public class AirportImporter implements Importer<Airport> {

	public AirportImporter() {
	}

	@Override
	public void importToIList(IList<Airport> iList, Collection<Airport> collection) {
		iList.addAll(collection);
	}

	@Override
	public void importToIMap(IMap<String, Airport> iMap, Collection<Airport> collection, String field) {
		collection.parallelStream().forEach(airport -> iMap.put(Getters.getField(airport, field), airport));
	}

	@Override
	public void importToMultiMap(MultiMap<String, Airport> multiMap, Collection<Airport> collection, String field) {
		collection.parallelStream().forEach(airport -> multiMap.put(Getters.getField(airport, field), airport));
	}
}
