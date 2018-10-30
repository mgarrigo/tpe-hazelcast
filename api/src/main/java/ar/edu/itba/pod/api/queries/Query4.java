package ar.edu.itba.pod.api.queries;

import ar.edu.itba.pod.api.collators.MovementCollator;
import ar.edu.itba.pod.api.combiner.ElementSumCombinerFactory;
import ar.edu.itba.pod.api.mappers.Query4Mapper;
import ar.edu.itba.pod.api.models.Movement;
import ar.edu.itba.pod.api.predicates.DestinationKeyPredicate;
import ar.edu.itba.pod.api.reducers.MovementCountReducerFactory;
import ar.edu.itba.pod.api.utils.FileReader;
import ar.edu.itba.pod.api.utils.MovementsImporter;
import ar.edu.itba.pod.api.utils.ParallelStreamFileReader;
import com.hazelcast.core.*;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class Query4 extends Query {

    private MultiMap<String, Movement> movementsMultiMap;
    private String destinationOaci;
    private int n;
    private List<Map.Entry<String, Long>> result;

    private static Logger LOGGER = LoggerFactory.getLogger(Query4.class);

    public Query4(HazelcastInstance client, File airportsFile, File movementsFile, String destinationOaci, int n) {
        super(client, airportsFile, movementsFile);
        this.destinationOaci = destinationOaci;
        this.n = n;
    }

    @Override
    public void readFiles() {
        FileReader fileReader = new ParallelStreamFileReader();

        Collection<Movement> movements = null;
        try {
            movements = fileReader.readMovements(getMovementsFile());
        } catch (IOException e) {
            LOGGER.error("Error reading files");
            System.exit(1);
        }

        movementsMultiMap = getClient().getMultiMap("movements");

        MovementsImporter movementsImporter = new MovementsImporter();
        movementsImporter.importToMultiMap(movementsMultiMap, movements, "destination");
    }

    @Override
    public void mapReduce() {
        JobTracker jobTracker = getClient().getJobTracker("query4");
        final KeyValueSource<String, Movement> source = KeyValueSource.fromMultiMap(movementsMultiMap);
        Job<String, Movement> job = jobTracker.newJob(source);

        ICompletableFuture<List<Map.Entry<String, Long>>> future = job
                .keyPredicate( new DestinationKeyPredicate(destinationOaci) )
                .mapper( new Query4Mapper() )
                .combiner( new ElementSumCombinerFactory<>() )
                .reducer( new MovementCountReducerFactory<>() )
                .submit( new MovementCollator() );

        try {
            result = future.get();
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void log() {
        System.out.println("OACI;Aterrizajes");
        int count = 0;
        for (Map.Entry<String, Long> e : result){
            String oaci = e.getKey();
            System.out.println(oaci + ";" + e.getValue());
            count++;
            if (count == n)
                break;
        }
    }
}
