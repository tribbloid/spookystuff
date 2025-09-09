#!/usr/bin/env bash

CRDIR="$(cd "`dirname "$0"`"; pwd)"
FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

source "${CRDIR}/profiles/apache-latest.sh"
source "${CRDIR}/.shared.sh"

cd "${FWDIR}" || exit
exec "${FWDIR}"/gradlew checkScalafix "${BUILD_PROFILES[@]}"
