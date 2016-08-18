#!/usr/bin/env bash

FWDIR="$(cd "`dirname "$0"`"/..; pwd)"
SPARK_NAME="${SPARK:-spark-1.6.2}"

SPARK_DIR_NAME="$SPARK_NAME"-bin-hadoop2.4

# Download Spark
wget -nc http://mirror.csclub.uwaterloo.ca/apache/spark/"$SPARK_NAME"/"$SPARK_DIR_NAME".tgz
tar -xvzf "$SPARK_DIR_NAME".tgz

export SPARK_HOME="$FWDIR"/"$SPARK_DIR_NAME"

./test-install.sh