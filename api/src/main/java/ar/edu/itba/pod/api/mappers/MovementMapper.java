package ar.edu.itba.pod.api.mappers;

import ar.edu.itba.pod.api.Airport;
import ar.edu.itba.pod.api.Movement;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.util.Optional;

public class MovementMapper implements Mapper<String, Movement, String, Long>, HazelcastInstanceAware {

    private transient HazelcastInstance instance;

    @Override
    public void map(String s, Movement movement, Context<String, Long> context) {

        IMap<String, Airport> airports = instance.getMap("airports");

        if (movement.getMovementType().equals("Despegue")) {
            String origin = movement.getOrigin();
            Optional<Airport> airport = Optional.ofNullable(airports.get(origin));
            airport.ifPresent(ap -> context.emit(origin, 1L));
        }else {
            String destination = movement.getDestination();
            Optional<Airport> airport = Optional.ofNullable(airports.get(destination));
            airport.ifPresent(ap -> context.emit(destination, 1L));
        }
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        instance = hazelcastInstance;
    }
}
