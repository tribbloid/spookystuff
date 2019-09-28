#!/usr/bin/env bash

CRDIR="$(cd "`dirname "$0"`"; pwd)"

source "${CRDIR}/.CI-common.sh"

"$CRDIR"/test.sh -Pbenchmark -Pscala-suffix -Pspark-2.4 "$@"
