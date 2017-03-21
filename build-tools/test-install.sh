#!/usr/bin/env bash

FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

mvn dependency:tree -f "$FWDIR"  -Pacceptance -Pdist "$@" > mvnTree.log

MAVEN_OPTS="-Xmx4g -XX:MaxPermSize=4g -XX:ReservedCodeCacheSize=512m" \
mvn clean install -f "$FWDIR" -Pdist -Puav "$@"
#mvn clean install -f "$FWDIR" -Puav  "$@" -pl uav