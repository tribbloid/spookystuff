#!/usr/bin/env bash

# TODO: replaced with compile + test_only?

CRDIR="$(cd "`dirname "$0"`"; pwd)"
FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

source "${CRDIR}/.shared.sh"

mkdir -p ${FWDIR}/logs/dependencyTree
${FWDIR}/gradlew -q dependencyTree -Dorg.gradle.parallel=false "${@}" > ${FWDIR}/logs/dependencyTree/"$DATE".log
