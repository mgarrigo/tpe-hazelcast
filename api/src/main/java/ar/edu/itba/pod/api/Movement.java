package ar.edu.itba.pod.api;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class Movement implements DataSerializable {

	private String flightType;
	private String movementType;
	private String origin;
	private String destination;

	public Movement(String flightType, String movementType, String origin, String destination) {
		this.flightType = flightType;
		this.movementType = movementType;
		this.origin = origin;
		this.destination = destination;
	}

	public String getFlightType() {
		return flightType;
	}

	public String getMovementType() {
		return movementType;
	}

	public String getOrigin() {
		return origin;
	}

	public String getDestination() {
		return destination;
	}

	@Override
	public String toString() {
		return String.format("Movement: { Flight Type: %s, Movement Type: %s, Origin OACI: %s, Destination OACI: %s}",
				getFlightType(), getMovementType(), getOrigin(), getDestination());
	}

	@Override
	public void writeData(ObjectDataOutput out) throws IOException {
		out.writeUTF(this.flightType);
		out.writeUTF(this.movementType);
		out.writeUTF(this.origin);
		out.writeUTF(this.destination);
	}

	@Override
	public void readData(ObjectDataInput in) throws IOException {
		this.flightType = in.readUTF();
		this.movementType = in.readUTF();
		this.origin = in.readUTF();
		this.destination = in.readUTF();
	}
}
