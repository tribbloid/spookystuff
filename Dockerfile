# Licensed to Datalayer (http://datalayer.io) under one or more
# contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. Datalayer licenses this file
# to you under the Apache License, Version 2.0 (the 
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

FROM phusion/baseimage:0.9.18

MAINTAINER long <yl1984108@gmail.com>

# Use baseimage-docker's init system.
CMD ["/sbin/my_init"]

### ENV ###

ENV PATH .:$PATH

RUN apt-get update

RUN apt-get install -y git wget unzip curl  \
  net-tools build-essential npm python python-setuptools \
  python-dev python-numpy openssh-server sysstat

# libfontconfig is needed for grunt phantomjs...
RUN apt-get install -y libfontconfig

ENV ZEPPELIN_REPO_URL        https://github.com/Karmacons/incubator-zeppelin.git
ENV ZEPPELIN_REPO_BRANCH     master
ENV ZEPPELIN_HOME            /opt/zeppelin
ENV ZEPPELIN_CONF_DIR        $ZEPPELIN_HOME/conf
ENV ZEPPELIN_NOTEBOOK_DIR    $ZEPPELIN_HOME/notebook
ENV ZEPPELIN_PORT            8080
ENV SCALA_BINARY_VERSION     2.10
ENV SCALA_VERSION            $SCALA_BINARY_VERSION.4
ENV SPARK_PROFILE            1.6
ENV SPARK_VERSION            1.6.0
ENV HADOOP_PROFILE           2.6
ENV HADOOP_VERSION           2.7.1

ENV SPOOKY_REPO_URL          https://github.com/tribbloid/spookystuff.git
ENV SPOOKY_REPO_BRANCH       master
ENV SPOOKY_HOME              /opt/spookystuff

### JAVA ###

RUN curl -sL --retry 3 --insecure \
  --header "Cookie: oraclelicense=accept-securebackup-cookie;" \
  "http://download.oracle.com/otn-pub/java/jdk/8u31-b13/jdk-8u31-linux-x64.tar.gz" \
  | gunzip \
  | tar x -C /opt/
ENV JAVA_HOME /opt/jdk1.8.0_31
RUN ln -s $JAVA_HOME /opt/java
ENV PATH $JAVA_HOME/bin:$PATH

### PYTHON ###

RUN apt-get install -y python-pip python-matplotlib python-pandas ipython python-nose
# RUN apt-get install -y scipy
RUN easy_install py4j pandas pattern pandasql sympy

### MAVEN ###

ENV MAVEN_VERSION 3.3.1
ENV MAVEN_HOME /opt/apache-maven-$MAVEN_VERSION
ENV PATH $PATH:$MAVEN_HOME/bin
RUN curl -sL http://archive.apache.org/dist/maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz \
  | gunzip \
  | tar x -C /opt/
RUN ln -s $MAVEN_HOME /opt/maven

### SPARK ###

WORKDIR /opt

RUN curl -sL --retry 3 \
  "http://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_PROFILE.tgz" \
  | gunzip \
  | tar x -C /opt/  \
  && ln -s /opt/spark-$SPARK_VERSION-bin-hadoop$HADOOP_PROFILE /opt/spark \
  && rm -rf /opt/spark/examples \
  && rm /opt/spark/lib/spark-examples*.jar


### SPOOKYSTUFF ###

ENV MAVEN_OPTS "-Xmx2g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m"

RUN git clone $SPOOKY_REPO_URL $SPOOKY_HOME

WORKDIR $SPOOKY_HOME

RUN git checkout $SPOOKY_REPO_BRANCH

RUN mvn install -DskipTests=true

### ZEPPELIN ###

ENV PATH $ZEPPELIN_HOME/zeppelin-web/node:$PATH
ENV PATH $ZEPPELIN_HOME/zeppelin-web/node_modules/grunt-cli/bin:$PATH

RUN git config --global url."https://".insteadOf git://

RUN git clone $ZEPPELIN_REPO_URL $ZEPPELIN_HOME

WORKDIR $ZEPPELIN_HOME

RUN git checkout $ZEPPELIN_REPO_BRANCH

RUN mvn clean \
  install \
  -pl '!cassandra,!elasticsearch,!flink,!hive,!ignite,!kylin,!lens,!phoenix,!postgresql,!tajo' \
  -Phadoop-$HADOOP_PROFILE \
  -Dhadoop.version=$HADOOP_VERSION \
  -Pspark-$SPARK_PROFILE \
  -Dspark.version=$SPARK_VERSION \
  -Ppyspark \
  -Dscala.version=$SCALA_VERSION \
  -Dscala.binary.version=$SCALA_BINARY_VERSION \
  -Dmaven.findbugs.enable=false \
  -Drat.skip=true \
  -Dcheckstyle.skip=true \
  -DskipTests

# Temporary fix to deal with conflicting akka provided jackson jars.
RUN rm zeppelin-server/target/lib/jackson-*
RUN rm zeppelin-zengine/target/lib/jackson-*

ENV PATH $ZEPPELIN_HOME/bin:$PATH

RUN mkdir $ZEPPELIN_HOME/logs
RUN mkdir $ZEPPELIN_HOME/run

### WEBAPP ###

### NOTEBOOK ###

# RUN mkdir /notebook
# ADD notebook/tutorial /notebook/tutorial

# Don't add interpreter.json @see https://github.com/datalayer/datalayer-docker/issues/1
# COPY ./resources/interpreter.json $ZEPPELIN_HOME/conf/interpreter.json

### DATASET ###

# RUN mkdir /dataset
# ADD dataset /dataset

### HADOOP ###

# RUN mkdir -p /etc/hadoop/conf
# ADD resources/hadoop /etc/hadoop/conf

### CLEAN ###

RUN rm -rf /root/.m2
RUN rm -rf /root/.npm
RUN apt-get clean
RUN rm -rf /var/lib/apt/lists/*

### INTERFACE ###

EXPOSE 22
EXPOSE 4040
EXPOSE 8080

ENTRYPOINT ["/opt/zeppelin/bin/datalayer-zeppelin.sh"]
