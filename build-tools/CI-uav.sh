#!/usr/bin/env bash

CRDIR="$(cd "`dirname "$0"`"; pwd)"

source "${CRDIR}/.CI-common.sh"

ALL_ARGS="-Pdummy $*"

exec "$CRDIR"/mvn-install.sh "$ALL_ARGS" && \
exec "$CRDIR"/test.sh "$ALL_ARGS" -Pscala-suffix -Puav -pl uav
