#!/usr/bin/env bash


# mandatory after Spark 2.3
# https://stackoverflow.com/questions/49143271/invalid-spark-url-in-local-spark-session
export SPARK_LOCAL_HOSTNAME=localhost

export MAVEN_OPTS="-Xmx4g -XX:ReservedCodeCacheSize=512m"

export DATE=$(date --iso-8601=second)
