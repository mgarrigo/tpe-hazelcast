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

public class MovementBetweenProvincesMapper implements Mapper<String, Movement, Pair<String, String>, Long>, HazelcastInstanceAware {

    private transient HazelcastInstance instance;

    @Override
    public void map(String s, Movement movement, Context<Pair<String, String>, Long> context) {
        IMap<String, Airport> airports = instance.getMap("airports");
        Optional<Airport> firstAirport = Optional.ofNullable(airports.get(movement.getDestination()));
        Optional<Airport> secondAirport = Optional.ofNullable(airports.get(movement.getOrigin()));
        if (firstAirport.isPresent() && secondAirport.isPresent()){
            String firstProvince = firstAirport.get().getProvince();
            String secondProvince = secondAirport.get().getProvince();
            if (!firstProvince.equals(secondProvince)){
                if (firstProvince.compareTo(secondProvince) < 0){
                    context.emit(new ImmutablePair<>(firstProvince, secondProvince), 1L);
                }else{
                    context.emit(new ImmutablePair<>(secondProvince, firstProvince), 1L);
                }
            }
        }
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        instance = hazelcastInstance;
    }
}
