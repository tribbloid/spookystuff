#!/usr/bin/env bash

FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

#mvn versions:use-latest-releases

mvn versions:set -DnewVersion=0.4.0-SNAPSHOT  -f "$FWDIR"/pom.xml "$@"