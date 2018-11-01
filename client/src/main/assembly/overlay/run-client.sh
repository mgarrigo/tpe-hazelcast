#!/bin/bash

PATH_TO_CODE_BASE=`pwd`

#JAVA_OPTS="-Djava.rmi.server.codebase=file://$PATH_TO_CODE_BASE/lib/jars/rmi-params-client-1.0-SNAPSHOT.jar"

MAIN_CLASS="ar.edu.itba.pod.client.Client"

java $@ -cp 'lib/jars/*'  $MAIN_CLASS $*
