#!/usr/bin/env bash

# TODO: replaced with compile + test_only?

CRDIR="$(cd "`dirname "$0"`"; pwd)"

echo "[COMPILING]" && \
"${CRDIR}"/make-all.sh "${@}" && \
echo "[RUNNING TESTS]" && \
"${CRDIR}"/test.sh "${@}"

# Save exit status of command1 immediately after it executes
exit_status=$?

"${CRDIR}"/test-report.sh

exit ${exit_status}