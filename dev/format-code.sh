#!/usr/bin/env bash

CRDIR="$(cd "`dirname "$0"`"; pwd)"

source "${CRDIR}/profiles/apache-latest/.common.sh"

"$CRDIR"/mvn-install.sh "${MVN_PROFILES[@]}"

mvn scalafix:scalafix "${MVN_PROFILES[@]}" -Pscalafix -f "$CRDIR"/../pom.xml
