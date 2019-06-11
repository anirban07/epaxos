#!/bin/bash

cd /home/ram16/epaxos/src/clientlat

# Takes at most 4 minutes
CMD="go run client.go -e -maddr $1 -c $2 -q 500 -w $3 -fr $4 -app $5 -T 10"

echo $CMD

$CMD
