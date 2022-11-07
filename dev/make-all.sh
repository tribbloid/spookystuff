#!/usr/bin/env bash


CRDIR="$(cd "`dirname "$0"`"; pwd)"
FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

mkdir -p ${FWDIR}/logs
mkdir -p ${FWDIR}/logs/dependencyTree

source "${CRDIR}/.shared.sh"

${FWDIR}/gradlew clean

${FWDIR}/gradlew -q dependencyTree "${@}" > ${FWDIR}/logs/dependencyTree/"$DATE".log

${FWDIR}/gradlew testClasses shadow "${@}"
