package ar.edu.itba.pod.client;

import ar.edu.itba.pod.api.Airport;
import ar.edu.itba.pod.api.FileReader;
import ar.edu.itba.pod.api.Movement;
import ar.edu.itba.pod.api.ParallelStreamFileReader;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;

public class Client {
	private static Logger LOGGER = LoggerFactory.getLogger(Client.class);

	public static void main(String[] args) throws URISyntaxException, IOException {
		LOGGER.info("tpe-hazelcast Client Starting ...");

		ClientConfig clientConfig = new ClientConfig();
		clientConfig.getGroupConfig().setName("tpe").setPassword("asdasd");


		HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

		final IList<Object> airportsIList = client.getList("airports");
		final IList<Object> movementsIList = client.getList("movements");


		File airportsFile = new File(Client.class.getClassLoader().getResource("aeropuertos.csv").toURI());
		File movementsFile = new File(Client.class.getClassLoader().getResource("movimientos.csv").toURI());

		FileReader fileReader = new ParallelStreamFileReader();

		LOGGER.debug("Inicio de la lectura del archivo");

		Collection<Airport> airports = fileReader.readAirports(airportsFile);
		Collection<Movement> movements = fileReader.readMovements(movementsFile);

		airportsIList.addAll(airports);
		movementsIList.addAll(movements);

		LOGGER.debug("Fin de la lectura del archivo");


//		airports.forEach(System.out::println);
//		movements.forEach(System.out::println);
	}

	@Deprecated
	private static List<String> readFile(String file, String encoding) throws URISyntaxException {
		LOGGER.info("Inicio de la lectura del archivo");
		List<String> lines = null;
		try {
			lines = Files.readAllLines(
					Paths.get(Client.class.getClassLoader().getResource(file).toURI()),
					Charset.forName(encoding)
			);
		} catch (IOException | NullPointerException e) {
			LOGGER.error("Error de lectura del archivo");
			System.exit(1);
		}
		LOGGER.info("Fin de la lectura del archivo");
		return lines;
	}
}
