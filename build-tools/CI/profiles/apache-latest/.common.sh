#!/usr/bin/env bash

SPARK_NAME="${SPARK:-spark-2.4.4}"
SPARK_DIR_ROOT="$HOME/.ci/spark-dist"

SPARK_DIR_NAME="$SPARK_NAME"-bin-hadoop2.7-scala-2.12

# Download Spark
wget -N https://spookystuff.s3.amazonaws.com/"$SPARK_NAME"/"$SPARK_DIR_NAME".tgz -P "$SPARK_DIR_ROOT"
# TODO: change to /tmp
tar -xzf "$SPARK_DIR_ROOT/$SPARK_DIR_NAME".tgz -C "$SPARK_DIR_ROOT"

export SPARK_HOME="$SPARK_DIR_ROOT/$SPARK_DIR_NAME"

export MVN_PROFILES=("-Pscala-suffix" "-Pspark-2.4" "-Pscala-2.12")
