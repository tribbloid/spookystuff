#!/usr/bin/env bash

FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

# shellcheck source=profiles/apache-stable/apache-latest.sh
source "${FWDIR}/profiles/${1}.sh"

"${FWDIR}"/../gradlew

if [ "${2}" = "prepare" ]; then
  exit 0
fi

"$FWDIR"/update-submodules.sh && \
  "$FWDIR"/CI/pipeline.sh "-PnotLocal" "${BUILD_PROFILES[@]}"
