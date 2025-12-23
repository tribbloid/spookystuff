#!/usr/bin/env bash

CRDIR="$(cd "`dirname "$0"`"; pwd)"

SPARK_V="${SPARK_V:-4.0.1}"

SPARK_NAME="${SPARK:-spark-${SPARK_V}}"

export SPARK_SCALA_VERSION=${SPARK_SCALA_VERSION:-2.13}
SPARK_DIR_NAME="$SPARK_NAME"-bin-hadoop3

SPARK_URL="https://archive.apache.org/dist/spark/${SPARK_NAME}/${SPARK_DIR_NAME}.tgz"

SPARK_DIR_ROOT="$HOME/.ci/spark-dist"

# Download Spark
_spookystuff_ci_is_sourced() {
  [[ "${BASH_SOURCE[0]}" != "${0}" ]]
}

_spookystuff_ci_die() {
  echo "$1" >&2
  if _spookystuff_ci_is_sourced; then
    return 1
  else
    exit 1
  fi
}

mkdir -p "$SPARK_DIR_ROOT" || _spookystuff_ci_die "cannot create dir: $SPARK_DIR_ROOT"

SPARK_TARBALL_PATH="$SPARK_DIR_ROOT/$SPARK_DIR_NAME.tgz"
if command -v wget >/dev/null 2>&1; then
  wget -q -O "$SPARK_TARBALL_PATH" "$SPARK_URL" || _spookystuff_ci_die "failed to download: $SPARK_URL"
elif command -v curl >/dev/null 2>&1; then
  curl -fsSL -o "$SPARK_TARBALL_PATH" "$SPARK_URL" || _spookystuff_ci_die "failed to download: $SPARK_URL"
else
  _spookystuff_ci_die "neither wget nor curl is available"
fi

test -s "$SPARK_TARBALL_PATH" || _spookystuff_ci_die "downloaded file is empty: $SPARK_TARBALL_PATH"
tar -xzf "$SPARK_TARBALL_PATH" -C "$SPARK_DIR_ROOT" || _spookystuff_ci_die "failed to extract: $SPARK_TARBALL_PATH"
test -d "$SPARK_DIR_ROOT/$SPARK_DIR_NAME" || _spookystuff_ci_die "Spark dir missing after extract: $SPARK_DIR_ROOT/$SPARK_DIR_NAME"

export BUILD_PROFILES=("-PsparkVersion=${SPARK_V}")

export SPARK_HOME="${SPARK_HOME:-$SPARK_DIR_ROOT/$SPARK_DIR_NAME}"

unset -f _spookystuff_ci_is_sourced _spookystuff_ci_die
