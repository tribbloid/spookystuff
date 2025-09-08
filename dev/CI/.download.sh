#!/usr/bin/env bash

CRDIR="$(cd "`dirname "$0"`"; pwd)"

SPARK_V="${SPARK_V:-3.5.6}"

SPARK_NAME="${SPARK:-spark-${SPARK_V}}"

export SPARK_SCALA_VERSION=${SPARK_SCALA_VERSION:-2.13}
SPARK_DIR_NAME="$SPARK_NAME"-bin-hadoop3-scala"${SPARK_SCALA_VERSION}"

SPARK_URL="http://archive.apache.org/dist/spark/${SPARK_NAME}/${SPARK_DIR_NAME}.tgz"

SPARK_DIR_ROOT="$HOME/.ci/spark-dist"

# Download Spark
wget -N "$SPARK_URL" -P "$SPARK_DIR_ROOT"
tar -xzf "$SPARK_DIR_ROOT/$SPARK_DIR_NAME".tgz -C "$SPARK_DIR_ROOT"

export BUILD_PROFILES=("-PsparkVersion=${SPARK_V}")

export SPARK_HOME="${SPARK_HOME:-$SPARK_DIR_ROOT/$SPARK_DIR_NAME}"

${CRDIR}/.shared.sh