package ar.edu.itba.pod.api.predicates;

import com.hazelcast.mapreduce.KeyPredicate;

public class DestinationKeyPredicate implements KeyPredicate<String> {

    private String destination;

    public DestinationKeyPredicate(String destinationOaci) {
        this.destination = destinationOaci;
    }

    @Override
    public boolean evaluate(String s) {
        return s.equals(destination);
    }
}
