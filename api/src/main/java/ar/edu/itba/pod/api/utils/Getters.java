package ar.edu.itba.pod.api.utils;

import ar.edu.itba.pod.api.models.Airport;
import ar.edu.itba.pod.api.models.Movement;

public class Getters<T> {

	public static String getField(Airport airport, String field) {
		switch (field.toUpperCase()) {
			case "OACI":
				return airport.getOaci();
			case "IATA":
				return airport.getIata();
			case "NAME":
				return airport.getName();
			case "PROVINCE":
				return airport.getProvince();
		}
		throw new IllegalArgumentException("Invalid field");
	}

	public static String getField(Movement movement, String field) {
		switch (field.toUpperCase()) {
			case "FLIGHTTYPE":
				return movement.getFlightType();
			case "MOVEMENTTYPE":
				return movement.getMovementType();
			case "ORIGIN":
				return movement.getOrigin();
			case "DESTINATION":
				return movement.getDestination();
		}
		throw new IllegalArgumentException("Invalid field");
	}
}
