package ar.edu.itba.pod.api.mappers;

import ar.edu.itba.pod.api.models.Airport;
import ar.edu.itba.pod.api.models.Movement;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Optional;

public class Query5Mapper implements Mapper<String, Movement, Pair<String, String>, Boolean>, HazelcastInstanceAware {

	private transient HazelcastInstance instance;

	@Override
	public void map(String s, Movement movement, Context<Pair<String, String>, Boolean> context) {

		IMap<String, Airport> airports = instance.getMap("airports");

		String airportOACI;
		if (movement.getMovementType().equals("Despegue")) {
			airportOACI = movement.getOrigin();
		} else {
			airportOACI = movement.getDestination();
		}
		Optional<Airport> airport = Optional.ofNullable(airports.get(airportOACI));

		airport.ifPresent(ap -> context.emit(new ImmutablePair<>(airportOACI, ap.getIata()), movement.getFlightClasification().equals("Internacional")));

	}

	@Override
	public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
		instance = hazelcastInstance;
	}
}
