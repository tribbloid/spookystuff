#!/usr/bin/env bash

FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

find "${FWDIR}" -wholename "**/scalatest-report.txt" -exec cat {} \;