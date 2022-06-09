#!/usr/bin/env bash

FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

# shellcheck source=profiles/apache-stable/.common.sh
source "${FWDIR}/profiles/${1}/.common.sh"

"$FWDIR"/../mvn-install.sh -U "${MVN_PROFILES[@]}" -Puav && \
exec "$FWDIR"/../test.sh "${MVN_PROFILES[@]}" -Puav -pl uav
