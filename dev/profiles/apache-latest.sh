#!/usr/bin/env bash
FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

#SPARK_V="${SPARK_V:-4.0.1}" TODO: migrate to it

source "${FWDIR}/CI/download.sh"
