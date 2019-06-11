#!/bin/bash

cd /home/ram16/epaxos/src/clientlat

CMD="go run client.go -e -maddr $1 -q 500 -c $2 -w $3 -fr $4 -app $5 -T 10"

echo $CMD

$CMD
