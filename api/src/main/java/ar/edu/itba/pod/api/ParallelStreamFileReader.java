package ar.edu.itba.pod.api;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Collection;
import java.util.stream.Collectors;

public class ParallelStreamFileReader implements FileReader {
	@Override
	public Collection<Airport> readAirports(File airportsFile) throws IOException {
		return Files.readAllLines(airportsFile.toPath(), Charset.forName("utf-8")).parallelStream().skip(1)
				.map(line -> line.split(";"))
				.map(val -> new Airport(val[1], val[2], val[4], val[21]))
				.collect(Collectors.toList());
	}

	@Override
	public Collection<Movement> readMovements(File movementsFile) throws IOException {
		return Files.readAllLines(movementsFile.toPath(), Charset.forName("iso-8859-1")).parallelStream().skip(1)
				.map(line -> line.split(";"))
				.map(val -> new Movement(val[3], val[4], val[5], val[6]))
				.collect(Collectors.toList());
	}
}
