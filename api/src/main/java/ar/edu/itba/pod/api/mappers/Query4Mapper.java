package ar.edu.itba.pod.api.mappers;

import ar.edu.itba.pod.api.models.Movement;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class Query4Mapper implements Mapper<String, Movement, String, Long> {

    @Override
    public void map(String s, Movement m, Context<String, Long> context) {

        if (m.getMovementType().equals("Aterrizaje")){
            context.emit(m.getOrigin(), 1L);
        }

    }
}
