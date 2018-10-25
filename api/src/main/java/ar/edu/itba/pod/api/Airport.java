package ar.edu.itba.pod.api;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class Airport implements DataSerializable {

    private String oaci;
    private String iata;
    private String name;
    private String province;

    public Airport(){}

    public Airport(String oaci, String iata, String name, String province) {
        this.oaci = oaci;
        this.iata = iata;
        this.name = name;
        this.province = province;
    }

    public String getOaci() {
        return oaci;
    }

    public String getIata() {
        return iata;
    }

    public String getName() {
        return name;
    }

    public String getProvince() {
        return province;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(oaci);
        out.writeUTF(iata);
        out.writeUTF(name);
        out.writeUTF(province);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        oaci = in.readUTF();
        iata = in.readUTF();
        name = in.readUTF();
        province = in.readUTF();
    }

    @Override
    public String toString() {
        return "Airport{" +
                "oaci='" + oaci + '\'' +
                ", iata='" + iata + '\'' +
                ", name='" + name + '\'' +
                ", province='" + province + '\'' +
                '}';
    }
}
