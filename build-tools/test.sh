#!/usr/bin/env bash

FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

mvn dependency:tree -f "$FWDIR"/pom.xml -Puav -Pdist "$@" > "$FWDIR"/mvnTree_"$(date +"%s")".log

MAVEN_OPTS="-Xmx4g -XX:MaxPermSize=4g -XX:ReservedCodeCacheSize=512m" \
mvn test -f "$FWDIR"/pom.xml -Pdist "$@"