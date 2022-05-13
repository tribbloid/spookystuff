#!/usr/bin/env bash

FWDIR="$(cd "`dirname "$0"`"/..; pwd)"


echo ""
echo "###############"
echo "# TEST REPORT #"
echo "###############"
echo ""

find "${FWDIR}" -wholename "**/scalatest-report.txt" -print -exec cat {} \;