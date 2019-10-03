#!/usr/bin/env bash

SPARK_NAME="${SPARK:-spark-2.4.4}"

SPARK_DIR_NAME="$SPARK_NAME"-bin-hadoop2.7-scala-2.12

# Download Spark
wget -N https://spookystuff.s3.amazonaws.com/"$SPARK_NAME"/"$SPARK_DIR_NAME".tgz -P /tmp/spark-dist
# TODO: change to /tmp
tar -xzf /tmp/spark-dist/"$SPARK_DIR_NAME".tgz -C /tmp/spark-dist

export SPARK_HOME=/tmp/spark-dist/"$SPARK_DIR_NAME"

export MVN_PROFILES=("-Pscala-suffix" "-Pspark-2.4")
