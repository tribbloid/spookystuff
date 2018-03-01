#!/usr/bin/env bash

FWDIR="$(cd "`dirname "$0"`"/..; pwd)"
PYTHON_EXE="python"

mvn dependency:tree -f "$FWDIR"/pom.xml -Puav -Pdist "$@" > mvnTree_"$(date +"%s")".log
"$PYTHON_EXE" -m pipdeptree > pipTree_"$(date +"%s")".log

MAVEN_OPTS="-Xmx4g -XX:MaxPermSize=4g -XX:ReservedCodeCacheSize=512m" \
mvn clean install -f "$FWDIR"/pom.xml -Pdist "$@"
