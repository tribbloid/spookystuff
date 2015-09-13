#!/usr/bin/env bash

mvn clean deploy -DskipTests=true -Prelease-sign-artifacts -Dgpg.passphrase=******