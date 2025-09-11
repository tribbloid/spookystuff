#!/usr/bin/env bash

CRDIR="$(cd "`dirname "$0"`"; pwd)"
FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

source "${CRDIR}/.shared.sh"


# ${FWDIR}/gradlew test                         # runs the test task
# --info                                        # displays verbose output
# --rerun-tasks                                # reruns all tasks on every build
# --continue                                   # continues task execution after failures
# -PnoUnused                                   # disables checking unused dependencies
# -Dorg.gradle.parallel=false                  # disables parallel execution of tasks
# "${@}"                                       # appends any additional arguments passed to the script
${FWDIR}/gradlew test --info --rerun --continue "-PnoUnused" "-Dorg.gradle.parallel=false" "${@}"