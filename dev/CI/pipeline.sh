#!/usr/bin/env bash

# TODO: replaced with compile + test_only?

FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

echo "[ENV]"
echo "SPARK_HOME=${SPARK_HOME}"
echo "SPARK_SCALA_VERSION=${SPARK_SCALA_VERSION}"

echo "[COMPILING]" && \
"${FWDIR}"/make-all.sh "${@}" && \
echo "[RUNNING TESTS]" && \
"${FWDIR}"/test.sh "${@}"

# Save exit status of command1 immediately after it executes
exit_status=$?

"${FWDIR}"/test-report.sh

exit ${exit_status}