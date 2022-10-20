#!/usr/bin/env bash

CRDIR="$(cd "`dirname "$0"`"; pwd)"
FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

source "${CRDIR}/profiles/apache-latest/.common.sh"

cd "$FWDIR" || exit
mvn clean compile test-compile scalafix:scalafix "${MVN_PROFILES[@]}" -Pbenchmark -Pscalafix -f "$FWDIR"/pom.xml
