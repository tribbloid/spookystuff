#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Enter posix mode for bash
#set -o posix

source bin/rootkey.csv

if [[ "$@" = *--help ]] || [[ "$@" = *-h ]]; then
  echo "Usage: ./bin/spooky-shell [options]"
  $SPARK_HOME/bin/spark-submit --help 2>&1 | grep -v Usage 1>&2
  exit 0
fi

function main(){
  export SPARK_SUBMIT_OPTS

  printf "AWS-credential = $AWSAccessKeyId:$AWSSecretKey\n"

  AWS_ACCESS_KEY_ID=$AWSAccessKeyId \
  AWS_SECRET_ACCESS_KEY=$AWSSecretKey \
  $SPARK_HOME/bin/spark-submit \
  ./shell/target/spookystuff-shell-assembly-0.1.0-SNAPSHOT.jar \
  "$@" --class org.tribbloid.spookystuff.shell.Main \
  --executor-memory 2G
}

# Copy restore-TTY-on-exit functions from Scala script so spark-shell exits properly even in
# binary distribution of Spark where Scala is not installed
exit_status=127
saved_stty=""

# restore stty settings (echo in particular)
function restoreSttySettings() {
  stty $saved_stty
  saved_stty=""
}

function onExit() {
  if [[ "$saved_stty" != "" ]]; then
    restoreSttySettings
  fi
  exit $exit_status
}

# to reenable echo if we are interrupted before completing.
trap onExit INT

# save terminal settings
saved_stty=$(stty -g 2>/dev/null)
# clear on error so we don't later try to restore them
if [[ ! $? ]]; then
  saved_stty=""
fi

main "$@"

# record the exit status lest it be overwritten:
# then reenable echo and propagate the code.
exit_status=$?
onExit
