#!/bin/bash

source rootkey.csv

printf "AWS-credential = $AWSAccessKeyId:$AWSSecretKey\n"

AWS_ACCESS_KEY_ID=$AWSAccessKeyId \
AWS_SECRET_ACCESS_KEY=$AWSSecretKey \
$SPARK_HOME/ec2/spark-ec2 -k master -i /home/peng/.ssh/id_rsa -s 1 launch prod \
--wait=600 \
--instance-type=r3.large \
--zone=us-west-1b \
--spot-price=0.02