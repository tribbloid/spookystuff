#!/usr/bin/env bash

CRDIR="$(cd "`dirname "$0"`"; pwd)"

source "${CRDIR}/profiles/apache-latest/.common.sh"

"$CRDIR"/mvn-install.sh "${MVN_PROFILES[@]}" -Pscalafix

"$FWDIR"/mvnw scalafix:scalafix "${MVN_PROFILES[@]}" -Pscalafix -DskipTests -f "$CRDIR"/../pom.xml
