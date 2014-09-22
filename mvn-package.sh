#!/usr/bin/env bash

MAVEN_OPTS="-Xmx2g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m" mvn package -DskipTests=true -P ispark