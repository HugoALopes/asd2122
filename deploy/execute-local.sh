#!/bin/bash


port=$1
i=$2
contactaddr=$3

(java -jar target/asdProj.jar -conf babel_config.properties address=$(hostname) port=$[$port] contact=$contactaddr my_index=$(($i)) | tee results/results-$(hostname)-$[$port].txt)&
