#!/usr/bin/env bash

mvn versions:use-latest-releases

mvn versions:set -DnewVersion=0.4.0-SNAPSHOT