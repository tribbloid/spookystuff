#!/usr/bin/env bash

FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

#mvn versions:use-latest-releases

# TODO: this is rendered obsolete by: https://maven.apache.org/maven-ci-friendly.html
mvn versions:set -DnewVersion=0.6.1-SNAPSHOT -Puav -Pbenchmark -f "$FWDIR"/pom.xml "$@"