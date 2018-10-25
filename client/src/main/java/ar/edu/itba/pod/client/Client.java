package ar.edu.itba.pod.client;

import ar.edu.itba.pod.api.MovementsPerAirport;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URISyntaxException;

public class Client {
	private static Logger LOGGER = LoggerFactory.getLogger(Client.class);

	public static void main(String[] args) throws URISyntaxException {
		LOGGER.info("tpe-hazelcast Client Starting ...");

		ClientConfig clientConfig = new ClientConfig();
		clientConfig.getGroupConfig().setName("tpe").setPassword("asdasd");


		HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

		File airportsFile = new File(Client.class.getClassLoader().getResource("aeropuertos.csv").toURI());
		File movementsFile = new File(Client.class.getClassLoader().getResource("movimientos.csv").toURI());

		MovementsPerAirport mv = new MovementsPerAirport(client, airportsFile, movementsFile);
		mv.run();
	}
}
