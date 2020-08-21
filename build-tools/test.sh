#!/usr/bin/env bash

CRDIR="$(cd "`dirname "$0"`"; pwd)"
FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

source "${CRDIR}/.mvn-common.sh"

#see http://www.bloggure.info/MavenFailTestAtEnd/
exec mvn test -f "$FWDIR"/pom.xml --fail-at-end -Pdist "$@"
