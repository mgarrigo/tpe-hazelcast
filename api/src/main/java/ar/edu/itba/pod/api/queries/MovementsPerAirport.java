package ar.edu.itba.pod.api.queries;

import ar.edu.itba.pod.api.combiner.ElementSumCombinerFactory;
import ar.edu.itba.pod.api.models.Airport;
import ar.edu.itba.pod.api.models.Movement;
import ar.edu.itba.pod.api.collators.MovementCollator;
import ar.edu.itba.pod.api.mappers.MovementMapper;
import ar.edu.itba.pod.api.reducers.MovementCountReducerFactory;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class MovementsPerAirport extends Query {

    private IList<Movement> movementsIList;
    private IMap<String, Airport> airportIMap;
    private List<Map.Entry<String, Long>> result;

    private static Logger LOGGER = LoggerFactory.getLogger(MovementsPerAirport.class);

    public MovementsPerAirport(HazelcastInstance client, File airportsFile, File movementsFile) {
        super(client, airportsFile, movementsFile);
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

        JobTracker jobTracker = getClient().getJobTracker("movement-count");
        final KeyValueSource<String, Movement> source = KeyValueSource.fromList(movementsIList);
        Job<String, Movement> job = jobTracker.newJob(source);

        ICompletableFuture<List<Map.Entry<String, Long>>> future = job
                .mapper( new MovementMapper() )
                .combiner( new ElementSumCombinerFactory<>() )
                .reducer( new MovementCountReducerFactory<>() )
                .submit( new MovementCollator() );

        result = null;
        try {
            result = future.get();
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void log() {

        System.out.println("OACI;Denominación;Movimientos");
        for (Map.Entry<String, Long> e : result){
            String oaci = e.getKey();
            System.out.println(oaci + ";" +  airportIMap.get(oaci).getName() + ";" + e.getValue());
        }

    }
}
