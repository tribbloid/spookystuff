#!/usr/bin/env bash

CRDIR="$(cd "`dirname "$0"`"; pwd)"
FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

"$CRDIR"/mvn-install.sh scalafix,benchmark,scala-2.12,spark-2.4

mvn scalafix:scalafix -P scalafix,benchmark,scala-2.12,spark-2.4 -f "$FWDIR"/pom.xml
