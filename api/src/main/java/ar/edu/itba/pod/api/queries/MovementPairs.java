package ar.edu.itba.pod.api.queries;

import ar.edu.itba.pod.api.collators.OrderNumberAndOACICollator;
import ar.edu.itba.pod.api.combiner.ElementSumCombinerFactory;
import ar.edu.itba.pod.api.mappers.UniqueMovementBetweenAirportsMapper;
import ar.edu.itba.pod.api.models.Movement;
import ar.edu.itba.pod.api.reducers.ThousandsCountReducerFactory;
import ar.edu.itba.pod.api.utils.FileReader;
import ar.edu.itba.pod.api.utils.MovementsImporter;
import ar.edu.itba.pod.api.utils.ParallelStreamFileReader;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class MovementPairs extends Query {

    private Set<Map.Entry<Pair<String, String>, Long>> result;
    private IList<Movement> movementsIList;
    private static Logger LOGGER = LoggerFactory.getLogger(MovementPairs.class);

    public MovementPairs(HazelcastInstance client, File airportsFile, File movementsFile) {
        super(client, airportsFile, movementsFile);
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

        movementsIList = getClient().getList("movements");

        MovementsImporter movementsImporter = new MovementsImporter();

        movementsImporter.importToIList(movementsIList, movements);
    }

    @Override
    public void mapReduce() {
        JobTracker jobTracker = getClient().getJobTracker("movement-pair");
        final KeyValueSource<String, Movement> source = KeyValueSource.fromList(movementsIList);
        Job<String, Movement> job = jobTracker.newJob(source);

        ICompletableFuture<Set<Map.Entry<Pair<String, String>, Long>>> future = job
                .mapper( new UniqueMovementBetweenAirportsMapper() )
                .combiner( new ElementSumCombinerFactory<>() )
                .reducer( new ThousandsCountReducerFactory() )
                .submit( new OrderNumberAndOACICollator() );

        result = null;
        try {
            result = future.get();
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void log() {
        System.out.println("Grupo;Aeropuerto A;Aeropuerto B");
        result.forEach(e -> System.out.println(e.getValue() + ";" + e.getKey().getKey() + ";" + e.getKey().getValue()));
    }
}