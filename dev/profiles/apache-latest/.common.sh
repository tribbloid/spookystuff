#!/usr/bin/env bash

SPARK_NAME="${SPARK:-spark-2.4.8}"
SPARK_DIR_ROOT="$HOME/.ci/spark-dist"

SPARK_DIR_NAME="$SPARK_NAME"-bin-hadoop2.7-scala2.12

SPARK_URL="https://storage.googleapis.com/ci_public/spark/${SPARK_DIR_NAME}.tgz"

# Download Spark
wget -N -q "$SPARK_URL" -P "$SPARK_DIR_ROOT"
tar -xzf "$SPARK_DIR_ROOT/$SPARK_DIR_NAME".tgz -C "$SPARK_DIR_ROOT"

export SPARK_HOME="$SPARK_DIR_ROOT/$SPARK_DIR_NAME"

export BUILD_PROFILES=("-PsparkVersion=2.4.8" "-PscalaVersion=2.12.16")
