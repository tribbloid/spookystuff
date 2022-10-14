#!/usr/bin/env bash

FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

# shellcheck source=profiles/apache-stable/.common.sh
source "${FWDIR}/profiles/${1}/.common.sh"

"$FWDIR"/CI-pipeline.sh "-PnotLocal" "${BUILD_PROFILES[@]}"
