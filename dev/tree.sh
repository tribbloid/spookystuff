#!/usr/bin/env bash

# TODO: replaced with compile + test_only?

CRDIR="$(cd "`dirname "$0"`"; pwd)"
FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

source "${CRDIR}/.mvn-common.sh"

mkdir -p "$FWDIR"/repackaged/selenium-bundle/logs

mvn dependency:tree -Dverbose --batch-mode --errors -f "$FWDIR"/repackaged/selenium-bundle/pom.xml -Pdist "$@" \
> "$FWDIR"/repackaged/selenium-bundle/logs/mvnTree_"$DATE".log

mkdir -p "$FWDIR"/logs

mvn dependency:tree -Dverbose --batch-mode --errors -f "$FWDIR"/pom.xml -Pdist "$@" \
> "$FWDIR"/logs/mvnTree_"$DATE".log
