#!/usr/bin/env bash

# TODO: replaced with compile + test_only?

CRDIR="$(cd "`dirname "$0"`"; pwd)"
FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

source "${CRDIR}/.shared.sh"

mkdir -p ${FWDIR}/logs/dependencyTree

DATE=$(date +%Y_%m_%d_%H_%M_%S)
${FWDIR}/gradlew -q dependencyTree -Dorg.gradle.parallel=false "${@}"
