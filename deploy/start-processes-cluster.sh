#!/bin/bash

processes=$1

if [ -z $processes ] || [ $processes -lt 1 ]; then
	echo "please indicate a number of processes of at least one"
	exit 0
fi

port=5000
i=0

(java -jar asdProj.jar -conf babel_config.properties interface=bond0 port=$port my_index=$(($i + 1))| tee results/results-$(hostname)-$[$port+$i].txt) &

#(java -jar asdProj.jar -conf config.properties address=$(hostname -i) port=$port </dev/null >results/results-$(hostname)-$[$port+$i].txt 2>$1) &

i=1

contactaddr=$(ifconfig bond0 | awk '/inet / {print $2}'):$port

while [ $i -lt $processes ]
do
	for node in $(oarprint host); do
		oarsh $node "nohup ./execute-local.sh $contactaddr $[$port+$i] $i&"
	  i=$[$i+1]
        done	  
done
