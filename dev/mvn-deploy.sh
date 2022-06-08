#!/usr/bin/env bash

FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

"$FWDIR"/mvnw clean deploy -f "$FWDIR"/pom.xml -Pdist -DskipTests=true \
-Prelease-sign-artifacts -Dgpg.passphrase=****** "$@"