#!/usr/bin/env bash

# TODO: replaced with compile + test_only?

CRDIR="$(cd "`dirname "$0"`"; pwd)"

echo "[COMPILING]" && \
"${CRDIR}"/make-all.sh --console=plain "${@}" && \
echo "[RUNNING TESTS]" && \
"${CRDIR}"/test.sh --console=plain "${@}"
