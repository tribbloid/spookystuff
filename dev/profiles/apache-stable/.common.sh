#!/usr/bin/env bash

SPARK_NAME="${SPARK:-spark-3.5.0}"
SPARK_DIR_ROOT="$HOME/.ci/spark-dist"

export SPARK_SCALA_VERSION="2.13"
SPARK_DIR_NAME="$SPARK_NAME"-bin-hadoop3-scala"${SPARK_SCALA_VERSION}"

SPARK_URL="http://archive.apache.org/dist/spark/${SPARK_NAME}/${SPARK_DIR_NAME}.tgz"

# Download Spark
wget -N -q "$SPARK_URL" -P "$SPARK_DIR_ROOT"
tar -xzf "$SPARK_DIR_ROOT/$SPARK_DIR_NAME".tgz -C "$SPARK_DIR_ROOT"

export SPARK_HOME="$SPARK_DIR_ROOT/$SPARK_DIR_NAME"

export BUILD_PROFILES=("-PsparkVersion=3.5.0")
