#!/bin/bash
NODES=3
while true; do
    for n in $(seq $NODES); do
        echo "---- NODE $n ----"
        redis-cli -p $(( 5000 + $n )) info raft
    done
    echo "==="
    sleep 1
done
