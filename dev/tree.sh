#!/usr/bin/env bash

# TODO: replaced with compile + test_only?

CRDIR="$(cd "`dirname "$0"`"; pwd)"
FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

${FWDIR}/gradlew -q dependencyTree "${@}" > ${FWDIR}/logs/dependencyTree/"$DATE".log
