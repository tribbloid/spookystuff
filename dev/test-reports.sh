#!/usr/bin/env bash

FWDIR="$(cd "`dirname "$0"`"/..; pwd)"


echo ""
echo "###############"
echo "# TEST REPORT #"
echo "###############"
echo ""

cd "${FWDIR}"

#find cannot use symbolic link, so cd is used
#see https://unix.stackexchange.com/questions/93857/find-does-not-work-on-symlinked-path
find . -wholename "./**/scalatest-report.txt" -print -exec cat {} \;