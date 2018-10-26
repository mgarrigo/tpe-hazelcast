package ar.edu.itba.pod.client;

public class Parameters {

    private String query = System.getProperty("query", "1");
    private String movementsInPath = System.getProperty("movementsInPath", "movimientos.csv");
    private String airportsInPath = System.getProperty("airportsInPath", "aeropuertos.csv");
    private String outPath = System.getProperty("outPath", "query.csv");
    private String timeOutPath = System.getProperty("timeOutPath", "time.txt");
    private String oaci = System.getProperty("oaci", "SAEZ");
    private String n = System.getProperty("n", "5");
    private String min = System.getProperty("min", "10000");
    private String addresses = System.getProperty("addresses", "127.0.0.1");

    public String getQuery() {
        return query;
    }

    public String getMovementsInPath() {
        return movementsInPath;
    }

    public String getAirportsInPath() {
        return airportsInPath;
    }

    public String getOutPath() {
        return outPath;
    }

    public String getTimeOutPath() {
        return timeOutPath;
    }

    public String getOaci() {
        return oaci;
    }

    public String getN() {
        return n;
    }

    public String getMin() {
        return min;
    }

    public String getAddresses() {
        return addresses;
    }
}