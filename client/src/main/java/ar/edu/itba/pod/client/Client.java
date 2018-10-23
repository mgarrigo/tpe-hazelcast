package ar.edu.itba.pod.client;

import ar.edu.itba.pod.api.Airport;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class Client {
	private static Logger logger = LoggerFactory.getLogger(Client.class);

	public static void main(String[] args) throws URISyntaxException {
		logger.info("tpe-hazelcast Client Starting ...");

		ClientConfig clientConfig = new ClientConfig();
		clientConfig.getGroupConfig().setName("tpe").setPassword("asdasd");


		HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

		final IList<Object> airports = client.getList("airports");

		List<String> airportLines = readFile("aeropuertos.csv", "utf-8");
		List<String> movementLines = readFile("movimientos.csv", "iso-8859-1");

		airportLines.stream().skip(1).map(line -> line.split(";"))
				.map(val -> new Airport(val[1], val[2], val[4], val[21]))
				.forEach(airports::add);
	}

	private static List<String> readFile(String file, String encoding) throws URISyntaxException {
		logger.info("Inicio de la lectura del archivo");
		List<String> lines = null;
		try {
			lines = Files.readAllLines(
					Paths.get(Client.class.getClassLoader().getResource(file).toURI()),
					Charset.forName(encoding)
			);
		} catch (IOException | NullPointerException e) {
			logger.error("Error de lectura del archivo");
			System.exit(1);
		}
		logger.info("Fin de la lectura del archivo");
		return lines;
	}
}
