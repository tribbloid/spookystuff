#!/usr/bin/env bash

SPARK_NAME="${SPARK:-spark-2.4.4}"

SPARK_DIR_NAME="$SPARK_NAME"-bin-hadoop2.7

# Download Spark
wget -N http://archive.apache.org/dist/spark/"$SPARK_NAME"/"$SPARK_DIR_NAME".tgz -P /tmp/spark-dist
# TODO: change to /tmp
tar -xzf /tmp/spark-dist/"$SPARK_DIR_NAME".tgz -C /tmp/spark-dist

export SPARK_HOME=/tmp/spark-dist/"$SPARK_DIR_NAME"
echo $SPARK_HOME