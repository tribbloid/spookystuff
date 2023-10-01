#!/usr/bin/env bash

CRDIR="$(cd "`dirname "$0"`"; pwd)"
FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

${FWDIR}/gradlew wrapper --gradle-version=8.4

${FWDIR}/gradlew dependencyUpdates "$@"
