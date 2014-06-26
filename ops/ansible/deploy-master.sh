#!/bin/bash

#make sure your password-less login works first
ansible-playbook deploy-master.yml -i ./inventories --private-key=$HOME/.ssh/id_rsa