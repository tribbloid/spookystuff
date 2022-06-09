#!/usr/bin/env bash

# TODO: replaced with compile + test_only?

CRDIR="$(cd "`dirname "$0"`"; pwd)"
FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

source "${CRDIR}/.mvn-common.sh"

"$FWDIR"/mvnw --version

"${CRDIR}/tree.sh" "$@"

"$FWDIR"/mvnw clean install --errors -f "$FWDIR"/pom.xml -Pdist "$@"
