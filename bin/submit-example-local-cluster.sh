#!/bin/bash

source rootkey.csv

printf "AWS-credential = $AWSAccessKeyId:$AWSSecretKey\n"

AWS_ACCESS_KEY_ID=$AWSAccessKeyId \
AWS_SECRET_ACCESS_KEY=$AWSSecretKey \
$SPARK_HOME/bin/spark-submit \
--class org.tribbloid.spookystuff.example.$1 \
--master "local-cluster[2,4,1000]" \
--deploy-mode client \
./example/target/spookystuff-example-assembly-0.1.0-SNAPSHOT.jar \

#--master "spark://peng-HP-Pavilion-dv7-Notebook-PC:7077" \
#--master "local-cluster[2,4,1000]" \
#--master "local[*]" \