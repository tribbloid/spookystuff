#!/usr/bin/env bash

FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

echo ""
echo "###############"
echo "# TEST REPORT #"
echo "###############"
echo ""

cd "${FWDIR}"

# Define the glob pattern
glob_pattern="**/build/test-results/**/*.xml"

#find cannot use symbolic link, so cd is used
#see https://unix.stackexchange.com/questions/93857/find-does-not-work-on-symlinked-path
find . -wholename ${glob_pattern} -print -exec cat {} \; | grep -5 "<failure" || \
  echo "No test failure found"
