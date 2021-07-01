#!/usr/bin/env bash

CRDIR="$(cd "`dirname "$0"`"; pwd)"

set -e

# shellcheck source=profiles/stable/.common.sh
source "${CRDIR}/profiles/${1}/.common.sh"

"$CRDIR"/../mvn-install.sh "${MVN_PROFILES[@]}" -Pbenchmark && \
exec "$CRDIR"/../test.sh "${MVN_PROFILES[@]}" -Pbenchmark
