#!/bin/bash

processes=$1

i=0
port=10000

while [ $i -lt $processes ]
do
	(java -jar target/asdProj.jar -conf babel_config.properties address=$(hostname -i) port=$[$port+$i] contact=$(hostname -i):$port storage_port=$[$port+$i+1] my_index=$(($i + 1)) | tee results/results-$(hostname)-$[$port+$i].txt)&
	i=$[$i+2]
done
