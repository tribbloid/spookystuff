#!/usr/bin/env bash

CRDIR="$(cd "`dirname "$0"`"; pwd)"
FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

#source "${CRDIR}/profiles/apache-latest/.common.sh"
#source "${CRDIR}/.shared.sh"

cd "${FWDIR}" || exit

"${FWDIR}"/gradlew scalafix -Dorg.gradle.parallel=false "${BUILD_PROFILES[@]}"
 # consumes too much memory to run in parallel

scalafmt

"${FWDIR}"/gradlew scalafix -Dorg.gradle.parallel=false "${BUILD_PROFILES[@]}"