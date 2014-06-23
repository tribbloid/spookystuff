#!/bin/sh

$SPARK_HOME/bin/spark-submit --class org.tribbloid.spookystuff.example.MoreLinkedIn --master "local-cluster[2,4,1000]" --deploy-mode client ./../../../target/spookystuff-example-assembly-0.1.0-SNAPSHOT.jar