#!/usr/bin/env bash

#make sure your password-less login works first
ansible-playbook deploy-worker.yml -i ./inventories --private-key=$HOME/.ssh/id_rsa