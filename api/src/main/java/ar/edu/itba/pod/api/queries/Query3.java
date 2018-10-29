package ar.edu.itba.pod.api.queries;

import ar.edu.itba.pod.api.collators.PairMovementCollator;
import ar.edu.itba.pod.api.combiner.PairSumCombinerFactory;
import ar.edu.itba.pod.api.mappers.MovementBetweenAirportsMapper;
import ar.edu.itba.pod.api.models.Movement;
import ar.edu.itba.pod.api.reducers.PairCountReducerFactory;
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

public class Query3 extends Query {

    private IList<Movement> movementsIList;
    private Set<Map.Entry<Pair<String, String>, Pair<Long, Long>>> result;
    private static Logger LOGGER = LoggerFactory.getLogger(Query3.class);

    public Query3(HazelcastInstance client, File airportsFile, File movementsFile) {
        super(client, airportsFile, movementsFile);
    }


    @Override
    public void readFiles() {
        FileReader fileReader = new ParallelStreamFileReader();

        Collection<Movement> movements = null;
        try {
            movements = fileReader.readMovements(this.getMovementsFile());
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
        JobTracker jobTracker = getClient().getJobTracker("query3");
        final KeyValueSource<String, Movement> source = KeyValueSource.fromList(movementsIList);
        Job<String, Movement> job = jobTracker.newJob(source);

        ICompletableFuture<Set<Map.Entry<Pair<String, String>, Pair<Long, Long>>>> future = job
                .mapper( new MovementBetweenAirportsMapper() )
                .combiner( new PairSumCombinerFactory<>())
                .reducer( new PairCountReducerFactory<>())
                .submit( new PairMovementCollator() );

        try {
            result = future.get();
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void log() {
        System.out.println("Origen;Destino;Origen->Destino;Destino->Origen");
        for (Map.Entry<Pair<String, String>, Pair<Long, Long>> e : result){
            System.out.println(e.getKey().getKey() + ';' + e.getKey().getValue() + ';' + e.getValue().getKey() + ';' + e.getValue().getValue());
        }
    }
}
