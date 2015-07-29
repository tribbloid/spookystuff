#!/usr/bin/env bash

source bin/.env

if [[ -z $AWSAccessKeyId || -z $AWSSecretKey || -z $AWSBucket ]]; then
  echo "Failed to find AWS variables in bin/.env" >&2
  echo "You need to specify AWS keys and a bucket name before running this program" >&2
  exit 1
fi

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

SCALA_VERSION=2.10

if [ -n "$1" ]; then
  EXAMPLE_CLASS="$1"
  shift
else
  echo "Usage: ./bin/submit-example <example-class> [example-args]"
  echo "  - set MASTER=XX to use a specific master"
  echo "  - can use abbreviated example class name (e.g. LinkedIn, largescale.GoogleImage)"
  exit 1
fi

export SPOOKY_EXAMPLES_JAR=example/target/scala-$SCALA_VERSION/spookystuff-example-assembly-0.3.0-SNAPSHOT.jar

if [[ -z SPOOKY_EXAMPLES_JAR ]]; then
  echo "Failed to find Spookystuff examples assembly in ./example/target/" >&2
  echo "You need to build Spookystuff before running this program" >&2
  exit 1
fi

EXAMPLE_MASTER=${MASTER:-"local-cluster[2,4,1000]"}

if [[ ! $EXAMPLE_CLASS == org.tribbloid.spookystuff.example* ]]; then
  EXAMPLE_CLASS="org.tribbloid.spookystuff.example.$EXAMPLE_CLASS"
fi

AWS_ACCESS_KEY_ID=$AWSAccessKeyId \
AWS_SECRET_ACCESS_KEY=$AWSSecretKey \
$SPARK_HOME/bin/spark-submit \
  --master $EXAMPLE_MASTER \
  --class $EXAMPLE_CLASS \
  --executor-memory 2G \
  --conf spooky.root=s3n://$AWSBucket \
  "$SPOOKY_EXAMPLES_JAR" \
  "$@"

#--master "spark://peng-HP-Pavilion-dv7-Notebook-PC:7077" \
#--master "local-cluster[2,4,1000]" \
#--master "local[*]" \
