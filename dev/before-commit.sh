#!/usr/bin/env bash

CRDIR="$(
  cd "$(dirname "$0")" || exit
  pwd
)"

ARGS=("-PsplainVersion=" "${@}")

echo "[FORMATTING]" && \
"${CRDIR}"/format-code.sh "${@}" && \
echo "[RUNNING TESTS]" && \
"${CRDIR}"/test.sh "${@}"
