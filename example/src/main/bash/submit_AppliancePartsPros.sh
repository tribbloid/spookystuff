#!/bin/sh

$SPARK_HOME/bin/spark-submit \
 --class org.tribbloid.spookystuff.example.AppliancePartsPros \
 --master "spark://peng-HP-Pavilion-dv7-Notebook-PC:7077" \
 --deploy-mode client\
 ./../../../target/spookystuff-example-assembly-0.1.0-SNAPSHOT.jar