#!/usr/bin/env bash

CRDIR="$(cd "`dirname "$0"`"; pwd)"
SPARK_NAME="${SPARK:-spark-2.4.3}"

SPARK_DIR_NAME="$SPARK_NAME"-bin-hadoop2.7

# Download Spark
wget -N http://archive.apache.org/dist/spark/"$SPARK_NAME"/"$SPARK_DIR_NAME".tgz -P .spark-dist
tar -xzf .spark-dist/"$SPARK_DIR_NAME".tgz -C .spark-dist

export SPARK_HOME="$PWD"/.spark-dist/"$SPARK_DIR_NAME"
echo $SPARK_HOME

ALL_ARGS="-Pdummy $*"

exec "$CRDIR"/mvn-install.sh "$ALL_ARGS" && \
exec "$CRDIR"/test.sh "$ALL_ARGS" -Puav -pl uav
