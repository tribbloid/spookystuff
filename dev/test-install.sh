#!/usr/bin/env bash

# TODO: replaced with compile + test_only?

CRDIR="$(cd "`dirname "$0"`"; pwd)"
FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

source "${CRDIR}/.mvn-common.sh"

mvn --version

mvn clean install --errors -f "$FWDIR"/repackaged/selenium-bundle/pom.xml "$@"
"${CRDIR}/tree.sh" "$@"

mvn clean install --errors -f "$FWDIR"/pom.xml -Pdist "$@"
