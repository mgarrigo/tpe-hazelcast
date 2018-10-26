package ar.edu.itba.pod.api.queries;

import com.hazelcast.core.HazelcastInstance;

import java.io.File;

public class MovementPairs extends Query {

    public MovementPairs(HazelcastInstance client, File airportsFile, File movementsFile) {
        super(client, airportsFile, movementsFile);
    }

    @Override
    public void readFiles() {

    }

    @Override
    public void mapReduce() {

    }

    @Override
    public void log() {

    }
}