#!/usr/bin/env bash

CRDIR="$(cd "`dirname "$0"`"; pwd)"
FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

source "${CRDIR}/.mvn-common.sh"

#see http://www.bloggure.info/MavenFailTestAtEnd/
mvn test -f "$FWDIR"/pom.xml -Pdist "$@"
