#!/usr/bin/env bash

FWDIR="$(cd "`dirname "$0"`"/..; pwd)"
DATE=$(date --iso-8601=second)

mkdir -p logs
mvn dependency:tree --batch-mode -f "$FWDIR"/pom.xml -Puav -Pdist "$@" > "$FWDIR"/logs/mvnTree_"$DATE".log

# mandatory after Spark 2.3
# https://stackoverflow.com/questions/49143271/invalid-spark-url-in-local-spark-session
MAVEN_OPTS="-Xmx4g -XX:MaxPermSize=4g -XX:ReservedCodeCacheSize=512m" \
SPARK_LOCAL_HOSTNAME=localhost \
mvn clean install -f "$FWDIR"/pom.xml -Pdist "$@"