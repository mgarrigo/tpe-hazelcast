package ar.edu.itba.pod.api.mappers;

import ar.edu.itba.pod.api.models.Movement;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class UniqueMovementBetweenAirportsMapper implements Mapper<String, Movement, String, Long> {

    @Override
    public void map(String s, Movement movement, Context<String, Long> context) {
        context.emit(movement.getDestination(), 1L);
        context.emit(movement.getOrigin(), 1L);
    }

}
