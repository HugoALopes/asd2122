#!/bin/bash

contactaddr=$1
port=$2
index=$(($3 + 1))

(java -jar asdProj.jar -conf config.properties interface=bond0 port=$port contact=$contactaddr my_index=$index </dev/null >results/results-$(hostname)-$port.txt 2>&1)&
