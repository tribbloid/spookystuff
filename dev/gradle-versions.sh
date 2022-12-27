#!/usr/bin/env bash

CRDIR="$(cd "`dirname "$0"`"; pwd)"
FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

${FWDIR}/gradlew wrapper --gradle-version=7.6

${FWDIR}/gradlew dependencyUpdates "$@"
