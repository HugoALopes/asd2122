#!/bin/bash

processes=$1

i=0
port=10000

while [ $i -lt $processes ]
do
	(java -jar asdProj.jar -conf babel_config.properties address=$(hostname -i) port=$[$port+$i] contact=$(hostname -i):$port my_index=$(($i + 1)) | tee results/results-$(hostname)-$[$port+$i].txt)&
	i=$[$i+1]	
done
