#!/bin/bash

cd /home/ram16/epaxos/src/clientlat

CMD="go run client.go -e -maddr $1 -c $2 -q $3 -sr $4 -w $5 -fr $6 -app $7 -T 20"

echo $CMD

$CMD
