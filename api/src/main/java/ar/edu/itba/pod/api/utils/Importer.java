package ar.edu.itba.pod.api.utils;

import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MultiMap;

import java.util.Collection;

public interface Importer<T> {

	void importToIList(IList<T> iList, Collection<T> collection);

	void importToIMap(IMap<String, T> iMap, Collection<T> collection, String field);

	void importToMultiMap(MultiMap<String, T> multiMap, Collection<T> collection, String field);
}
