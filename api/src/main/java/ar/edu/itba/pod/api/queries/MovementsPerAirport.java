package ar.edu.itba.pod.api.queries;

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

public class MovementsPerAirport implements Runnable {

    private HazelcastInstance client;
    private File airportsFile;
    private File movementsFile;

    private static Logger LOGGER = LoggerFactory.getLogger(MovementsPerAirport.class);

    public MovementsPerAirport(HazelcastInstance client, File airportsFile, File movementsFile){
        this.client = client;
        this.airportsFile = airportsFile;
        this.movementsFile = movementsFile;
    }

    @Override
    public void run() {

        FileReader fileReader = new ParallelStreamFileReader();

        LOGGER.debug("Inicio de la lectura del archivo");
        Collection<Airport> airports = null;
        Collection<Movement> movements = null;
        try {
            airports = fileReader.readAirports(airportsFile);
            movements = fileReader.readMovements(movementsFile);
        } catch (IOException e) {
            LOGGER.error("Error reading files");
            System.exit(1);
        }

        final IList<Movement> movementsIList = client.getList("movements");
        final IMap<String, Airport> airportIMap = client.getMap("airports");

        MovementsImporter movementsImporter = new MovementsImporter();
        AirportImporter airportImporter = new AirportImporter();

        movementsImporter.importToIList(movementsIList, movements);
        airportImporter.importToIMap(airportIMap, airports, "OACI");

        LOGGER.debug("Fin de la lectura del archivo");

        JobTracker jobTracker = client.getJobTracker("movement-count");
        final KeyValueSource<String, Movement> source = KeyValueSource.fromList(movementsIList);
        Job<String, Movement> job = jobTracker.newJob(source);

        LOGGER.info("Inicio del trabajo map/reduce");
        ICompletableFuture<List<Map.Entry<String, Long>>> future = job
                .mapper( new MovementMapper() )
                .reducer( new MovementCountReducerFactory() )
                .submit( new MovementCollator() );

        List<Map.Entry<String, Long>> result = null;
        try {
            result = future.get();
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
        LOGGER.info("Fin del trabajo map/reduce");

        System.out.println("OACI;Denominación;Movimientos");
        for (Map.Entry<String, Long> e : result){
            String oaci = e.getKey();
            System.out.println(oaci + ";" +  airportIMap.get(oaci).getName() + ";" + e.getValue());
        }

    }
}
