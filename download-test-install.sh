#!/usr/bin/env bash

CRDIR="$(cd "`dirname "$0"`"; pwd)"
SPARK_NAME="${SPARK:-spark-1.6.2}"

SPARK_DIR_NAME="$SPARK_NAME"-bin-hadoop2.4

# Download Spark
wget -nc http://mirror.csclub.uwaterloo.ca/apache/spark/"$SPARK_NAME"/"$SPARK_DIR_NAME".tgz
tar -xzf "$SPARK_DIR_NAME".tgz

export SPARK_HOME="$CRDIR"/"$SPARK_DIR_NAME"
echo $SPARK_HOME

exec ./test-install.sh