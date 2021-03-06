#!/bin/bash

cd /home/ram16/epaxos/src/server

IP=`ip addr show eth0 | grep 'inet ' | cut -d ' ' -f 8`

CMD="go run server.go -maddr $1 -addr $IP -e -exec -dreply -app $2"

echo $CMD

$CMD

# To Kill Servers using pssh
# run pgrep -l server to see which processes show up (make sure they are correct)
# run pkill server
