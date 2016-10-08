#!/usr/bin/env bash
#should deploy both classifiers

cd ..

mvn clean deploy -DskipTests=true -Prelease-sign-artifacts -Ppipeline -Dgpg.passphrase=****** "$@"