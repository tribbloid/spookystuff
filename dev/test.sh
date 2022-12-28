#!/usr/bin/env bash

CRDIR="$(cd "`dirname "$0"`"; pwd)"
FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

source "${CRDIR}/.shared.sh"

${FWDIR}/gradlew test "-PnoUnused" "-Dorg.gradle.parallel=false" "${@}"

cat **/build/test-results/**/*.xml| grep -5 "<failure" || \
  echo "No test failure found"
