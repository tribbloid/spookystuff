#!/usr/bin/env bash

FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

# shellcheck source=profiles/apache-stable/.common.sh
source "${FWDIR}/profiles/${1}/.common.sh"

if [ "${2}" = "prepare" ]; then
  exit 0
fi

"$FWDIR"/update-submodules.sh && \
  "$FWDIR"/CI/pipeline.sh "-PnotLocal" "${BUILD_PROFILES[@]}"
