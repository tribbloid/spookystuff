#!/usr/bin/env bash

FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

mvn dependency:tree -f "$FWDIR"  -Pacceptance -Pdist "$@" > mvnTree.log

MAVEN_OPTS="-Xmx4g -XX:MaxPermSize=4g -XX:ReservedCodeCacheSize=512m" \
exec mvn clean install -f "$FWDIR" -Pacceptance -Pdist "$@"
