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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
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
    public void log(String outPath) {
        Path path = Paths.get(outPath);
        String header = "Grupo;Aeropuerto A;Aeropuerto B\n";
        try {
            Files.write(path, header.getBytes());
            for (Map.Entry<Pair<String, String>, Long> e : result){
                String out = e.getValue() + ";" + e.getKey().getKey() + ";" + e.getKey().getValue() + "\n";
                Files.write(path, out.getBytes(), StandardOpenOption.APPEND);
            }
        }
        catch (IOException e) {
            LOGGER.error("Error writing to out file");
        }
    }
}