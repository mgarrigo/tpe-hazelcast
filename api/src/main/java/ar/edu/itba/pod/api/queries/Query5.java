package ar.edu.itba.pod.api.queries;

import ar.edu.itba.pod.api.collators.Query5Collator;
import ar.edu.itba.pod.api.combiner.Query5CombinerFactory;
import ar.edu.itba.pod.api.mappers.Query5Mapper;
import ar.edu.itba.pod.api.models.Airport;
import ar.edu.itba.pod.api.models.Movement;
import ar.edu.itba.pod.api.reducers.Query5ReducerFactory;
import ar.edu.itba.pod.api.utils.AirportImporter;
import ar.edu.itba.pod.api.utils.FileReader;
import ar.edu.itba.pod.api.utils.MovementsImporter;
import ar.edu.itba.pod.api.utils.ParallelStreamFileReader;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
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
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class Query5 extends Query {

    private IList<Movement> movementsIList;
    private IMap<String, Airport> airportIMap;
    private List<Map.Entry<Pair<String, String>, Integer>> result;
    private int n;

    private static Logger LOGGER = LoggerFactory.getLogger(Query5.class);

    public Query5(HazelcastInstance client, File airportsFile, File movementsFile, int n) {
        super(client, airportsFile, movementsFile);
        this.n = n;
    }

    public void readFiles(){

        FileReader fileReader = new ParallelStreamFileReader();

        Collection<Airport> airports = null;
        Collection<Movement> movements = null;
        try {
            airports = fileReader.readAirports(getAirportsFile());
            movements = fileReader.readMovements(getMovementsFile());
        } catch (IOException e) {
            LOGGER.error("Error reading files");
            System.exit(1);
        }

        movementsIList = getClient().getList("movements");
        airportIMap = getClient().getMap("airports");

        MovementsImporter movementsImporter = new MovementsImporter();
        AirportImporter airportImporter = new AirportImporter();

        movementsImporter.importToIList(movementsIList, movements);
        airportImporter.importToIMap(airportIMap, airports, "OACI");
    }

    public void mapReduce(){

        JobTracker jobTracker = getClient().getJobTracker("query5");
        final KeyValueSource<String, Movement> source = KeyValueSource.fromList(movementsIList);
        Job<String, Movement> job = jobTracker.newJob(source);

        ICompletableFuture<List<Map.Entry<Pair<String, String>, Integer>>> future = job
                .mapper(new Query5Mapper())
				.combiner(new Query5CombinerFactory<>())
                .reducer(new Query5ReducerFactory())
                .submit(new Query5Collator(n));

        result = null;
        try {
            result = future.get();
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void log(Path path) {

        String header = "IATA;Porcentaje\n";

        try {
            Files.write(path, header.getBytes());
            for (Map.Entry<Pair<String, String>, Integer> e : result) {
                String out = String.format("%s;%s%%\n", e.getKey().getValue(), e.getValue());
                Files.write(path, out.getBytes(), StandardOpenOption.APPEND);
            }
        }
        catch (IOException e) {
            LOGGER.error("Error writing to out file");
        }

    }
}
