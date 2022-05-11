#!/usr/bin/env bash

FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

set -e

# shellcheck source=profiles/apache-stable/.common.sh
source "${FWDIR}/profiles/${1}/.common.sh"

"$FWDIR"/mvn-install.sh "${MVN_PROFILES[@]}" -Pbenchmark && \
"$FWDIR"/test.sh "${MVN_PROFILES[@]}" -Pbenchmark && \
"$FWDIR"/test-reports.sh
