#!/usr/bin/env bash

CRDIR="$(cd "`dirname "$0"`"; pwd)"

exec "$CRDIR"/test-install.sh -DskipTests=true -Puav -q "$@"