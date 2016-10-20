#!/usr/bin/env bash

FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

MAVEN_OPTS="-Xmx4g -XX:MaxPermSize=4g -XX:ReservedCodeCacheSize=512m" \
mvn clean install -f "$FWDIR" -Pdist "$@"

mvn dependency:tree > mvnTree.log -f "$FWDIR"