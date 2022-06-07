##!/usr/bin/env bash
#
#FWDIR="$(cd "`dirname "$0"`"/..; pwd)"
#
## shellcheck source=profiles/apache-stable/.common.sh
#source "${FWDIR}/profiles/${1}/.common.sh"
#
#"$FWDIR"/make-all.sh "${BUILD_PROFILES[@]}" && \
#exec "$FWDIR"/test.sh "${BUILD_PROFILES[@]}" -pl uav
