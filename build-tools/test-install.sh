#!/usr/bin/env bash

FWDIR="$(cd "`dirname "$0"`"/..; pwd)"
DATE=$(date --iso-8601=second)

mvn dependency:tree -f "$FWDIR"/pom.xml -Puav -Pdist "$@" > "$FWDIR"/mvnTree_"$DATE".log

MAVEN_OPTS="-Xmx4g -XX:MaxPermSize=4g -XX:ReservedCodeCacheSize=512m" \
mvn clean install -f "$FWDIR"/pom.xml -Pdist "$@"