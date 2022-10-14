#!/usr/bin/env bash

# TODO: replaced with compile + test_only?

CRDIR="$(cd "`dirname "$0"`"; pwd)"
FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

source "${CRDIR}/.mvn-common.sh"

mkdir -p "$FWDIR"/repack/selenium-bundle/logs

mvn dependency:tree -Dverbose --batch-mode --errors -f "$FWDIR"/repack/selenium-bundle/pom.xml -Pdist "$@" \
> "$FWDIR"/repack/selenium-bundle/logs/mvnTree_"$DATE".log

mkdir -p "$FWDIR"/logs

mvn dependency:tree -Dverbose --batch-mode --errors -f "$FWDIR"/pom.xml -Pdist "$@" \
> "$FWDIR"/logs/mvnTree_"$DATE".log
