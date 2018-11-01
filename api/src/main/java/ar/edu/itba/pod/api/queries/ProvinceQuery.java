package ar.edu.itba.pod.api.queries;

import ar.edu.itba.pod.api.collators.ProvinceCollator;
import ar.edu.itba.pod.api.combiner.ElementSumCombinerFactory;
import ar.edu.itba.pod.api.mappers.MovementBetweenProvincesMapper;
import ar.edu.itba.pod.api.models.Airport;
import ar.edu.itba.pod.api.models.Movement;
import ar.edu.itba.pod.api.reducers.MovementCountReducerFactory;
import ar.edu.itba.pod.api.utils.AirportImporter;
import ar.edu.itba.pod.api.utils.FileReader;
import ar.edu.itba.pod.api.utils.MovementsImporter;
import ar.edu.itba.pod.api.utils.ParallelStreamFileReader;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobCompletableFuture;
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

public class ProvinceQuery extends Query  {

    private IList<Movement> movementsIList;
    private IMap<String, Airport> airportIMap;
    private Set<Map.Entry<Pair<String, String>, Long>> result;

    private Long min;
    private static Logger LOGGER = LoggerFactory.getLogger(MovementsPerAirport.class);

    public ProvinceQuery(HazelcastInstance client, File airportsFile, File movementsFile, Long min) {
        super(client, airportsFile, movementsFile);
        this.min = min;
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

        JobCompletableFuture<Set<Map.Entry<Pair<String, String>, Long>>> future = job
                .mapper( new MovementBetweenProvincesMapper() )
                .combiner( new ElementSumCombinerFactory<>() )
                .reducer( new MovementCountReducerFactory<>() )
                .submit( new ProvinceCollator(this.min) );

        result = null;
        try {
            result = future.get();
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void log(Path path) {

        String header = "Provincia A;Provincia B;Movimientos\n";
        try{
            Files.write(path, header.getBytes());
            for (Map.Entry<Pair<String, String>, Long> e : result){
                String out = e.getKey().getKey()+";"+e.getKey().getValue()+";"+e.getValue()+"\n";
                Files.write(path, out.getBytes(), StandardOpenOption.APPEND);
            }
        }
        catch (IOException e) {
            LOGGER.error("Error writing to out file");
        }
    }
}
