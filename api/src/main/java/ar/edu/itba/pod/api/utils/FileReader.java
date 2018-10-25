package ar.edu.itba.pod.api.utils;

import ar.edu.itba.pod.api.models.Airport;
import ar.edu.itba.pod.api.models.Movement;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

public interface FileReader {

	Collection<Airport> readAirports(File airportsFile) throws IOException;

	Collection<Movement> readMovements(File movementsFile) throws IOException;

}
