#!/usr/bin/env bash

CRDIR="$(cd "`dirname "$0"`"; pwd)"

# shellcheck source=profiles/stable/.common.sh
source "${CRDIR}/profiles/${1}/.common.sh"

"$CRDIR"/../test.sh "${MVN_PROFILES[@]}" -Pbenchmark
