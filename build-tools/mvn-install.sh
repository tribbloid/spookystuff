#!/usr/bin/env bash

FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

MAVEN_OPTS="-Xmx4g -XX:MaxPermSize=4g -XX:ReservedCodeCacheSize=512m" mvn clean install -DskipTests=true "$@" -f "$FWDIR"