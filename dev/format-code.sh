#!/usr/bin/env bash

CRDIR="$(cd "`dirname "$0"`"; pwd)"
FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

cd "${FWDIR}" || exit

"${FWDIR}"/gradlew scalafix -Dorg.gradle.parallel=false "${BUILD_PROFILES[@]}"
 # consumes too much memory to run in parallel

scalafmt

"${FWDIR}"/gradlew scalafix -Dorg.gradle.parallel=false "${BUILD_PROFILES[@]}"