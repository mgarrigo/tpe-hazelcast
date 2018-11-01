# TP Hazelcast

## Coders

- Mariano Garrigo
- Ariel Debrouvier
- Eliseo Parodi Almaraz

## Description

This git repository contains the code for a Hazelcast cluster with a client and an api to execute queries in the
cluster. The objective of this is to execute some Map-Reduce operations to analyze the data of the flights in Argentina.

## How to Run

To compile and run this code you need Java 8 or more and Maven. Please check if the configuration for the network
interface in *hazelcast.xml* supports your interface in the <interfaces> tag.

### Compilation

```
mvn package
```

### Run server

```
cd server/target
tar -xzf tpe-hazelcast-server-1.0-SNAPSHOT-bin.tar.gz
cd tpe-hazelcast-server-1.0-SNAPSHOT
chmod +x run-server.sh
./run-server.sh
```

### Run client

Check that the .csv files are in the same folder as the run-client.sh.

```
cd client/target
tar -xzf tpe-hazelcast-client-1.0-SNAPSHOT-bin.tar.gz
cd tpe-hazelcast-client-1.0-SNAPSHOT
chmod +x run-client.sh
./run-client.sh <parameters>
```

## Parameters

- -Daddresses=<ip>;<ip>: The ip address of the nodes in the cluster.
- -Dquery: The number of the query. From 1 to 6.
- -DmovementsInPath: The path of the movements file.
- -DairportsInPath: The path of the airports file.
- -DoutPath: The file where to write the output.
- -DtimeOutPath: The file where to write the timestamps.
- -Doaci: (query 4) The airport to check the movements.
- -Dn: (query 4/5) The number of lines to show.
- -Dmin: (query 6) The minimum number of movements to filter a pair of provinces.
