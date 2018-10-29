package ar.edu.itba.pod.api.mappers;

import ar.edu.itba.pod.api.models.Movement;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import javafx.util.Pair;

public class MovementBetweenAirportsMapper implements Mapper<String, Movement, Pair<String, String>, Pair<Long, Long>>{

    @Override
    public void map(String s, Movement movement, Context<Pair<String, String>, Pair<Long, Long>> context) {
        Pair<String, String> originToDestiny = new Pair<>(movement.getOrigin(), movement.getDestination());
        Pair<String, String> destinyToOrigin = new Pair<>(movement.getDestination(), movement.getOrigin());
        Pair<Long, Long> originToDestinyAmount = new Pair<>(1L, 0L);
        Pair<Long, Long> destinyToOriginAmount = new Pair<>(0L, 1L);
        context.emit(originToDestiny, originToDestinyAmount);
        context.emit(destinyToOrigin, destinyToOriginAmount);
    }
}
