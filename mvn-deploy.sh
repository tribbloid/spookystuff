#!/usr/bin/env bash

mvn clean deploy -DskipTests=true -Prelease-sign-artifacts -Ppipeline -Dgpg.passphrase=******