#!/usr/bin/env bash

CRDIR="$(cd "`dirname "$0"`"; pwd)"
FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

source "${CRDIR}/.shared.sh"

${FWDIR}/gradlew test --continue "-PnoUnused" "-Dorg.gradle.parallel=false" "${@}"
