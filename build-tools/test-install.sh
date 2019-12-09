#!/usr/bin/env bash

# TODO: replaced with compile + test_only?

CRDIR="$(cd "`dirname "$0"`"; pwd)"
FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

source "${CRDIR}/.mvn-common.sh"

mkdir -p logs
mvn dependency:tree --batch-mode -f "$FWDIR"/pom.xml -Puav -Pdist "$@" > "$FWDIR"/logs/mvnTree_"$DATE".log

exec mvn clean install -T 2 -f "$FWDIR"/pom.xml -Pdist "$@"
