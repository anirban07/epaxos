#!/bin/bash

cd /home/ram16/epaxos/src/master

CMD="go run master.go -N $1"

echo $CMD

CMD