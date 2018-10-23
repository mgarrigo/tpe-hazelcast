package ar.edu.itba.pod.client;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Client {
	private static Logger logger = LoggerFactory.getLogger(Client.class);

	public static void main(String[] args) {
		logger.info("tpe-hazelcast Client Starting ...");

		ClientConfig clientConfig = new ClientConfig();
		clientConfig.getGroupConfig().setName("tpe").setPassword("asdasd");


		HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
	}
}
