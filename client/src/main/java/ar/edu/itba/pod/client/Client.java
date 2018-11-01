package ar.edu.itba.pod.client;

import ar.edu.itba.pod.api.queries.*;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Map;

public class Client {
	private static Logger LOGGER = LoggerFactory.getLogger(Client.class);

	public static void main(String[] args) throws URISyntaxException {
		LOGGER.info("tpe-hazelcast Client Starting ...");

		ClientConfig clientConfig = new ClientConfig();
		ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
		clientConfig.getGroupConfig().setName("54393-56399-55382").setPassword("asdasd");

		Parameters p = new Parameters();
		networkConfig.addAddress(p.getAddresses().split(","));

		HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

		File airportsFile = new File(p.getAirportsInPath());
		File movementsFile = new File(p.getMovementsInPath());

		Query query;

		switch (p.getQuery()){
			case "1":
				query = new MovementsPerAirport(client, airportsFile, movementsFile);
				break;
            case "2":
                query = new MovementPairs(client, airportsFile, movementsFile);
                break;
			case "3":
				query = new Query3(client, airportsFile, movementsFile);
				break;
			case "4":
				query = new Query4(client, airportsFile, movementsFile, p.getOaci(), Integer.valueOf(p.getN()));
				break;
			case "5":
				query = new Query5(client, airportsFile, movementsFile, Integer.valueOf(p.getN()));
				break;
			case "6":
				query = new ProvinceQuery(client, airportsFile, movementsFile, new Long(p.getMin()));
				break;
            default:
				LOGGER.error("Invalid query number.");
				return;
		}

		MapReduceLogger mrlogger;
		try {
			mrlogger = new MapReduceLogger(p.getTimeOutPath(), Client.class);
			mrlogger.info("Inicio de la lectura de archivos");
			query.readFiles();
			mrlogger.info("Fin de la lectura de archivos");
			mrlogger.info("Inicio del trabajo map/reduce");
			query.mapReduce();
			mrlogger.info("Fin del trabajo map/reduce");
			mrlogger.close();
		} catch (IOException e) {
			LOGGER.error("Could not write in log");
		}
		query.log(Paths.get(p.getOutPath()));
		client.shutdown();
	}
}