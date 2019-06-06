#!/bin/bash

cd /home/ram16/epaxos/src/clientlat

CMD="go run client.go -e -maddr $1 -c $2 -q $3 -sr $4 -T 10"

echo $CMD

$CMD
