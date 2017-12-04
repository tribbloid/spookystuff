#!/usr/bin/env bash

FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

#mvn versions:use-latest-releases

mvn versions:set -DnewVersion=0.5.0-SNAPSHOT -Puav -f "$FWDIR" "$@"