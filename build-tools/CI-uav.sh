#!/usr/bin/env bash

# TODO: this file is merely kept for backward compatibility

CRDIR="$(cd "`dirname "$0"`"; pwd)"

"$CRDIR"/CI/uav-stage1.sh apache-stable
