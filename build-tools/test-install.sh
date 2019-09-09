#!/usr/bin/env bash

CRDIR="$(cd "`dirname "$0"`"; pwd)"
FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

source "${CRDIR}/.test-common.sh"

mkdir -p logs
mvn dependency:tree --batch-mode -f "$FWDIR"/pom.xml -Puav -Pdist "$@" > "$FWDIR"/logs/mvnTree_"$DATE".log

mvn clean install -f "$FWDIR"/pom.xml -Pdist "$@"
