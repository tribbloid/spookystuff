#!/usr/bin/env bash

FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

mvn clean deploy -DskipTests=true -Prelease-sign-artifacts -Ppipeline -Dgpg.passphrase=****** "$@" -f "$FWDIR"