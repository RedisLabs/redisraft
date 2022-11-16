#!/bin/bash

# Copyright Redis Ltd. 2020 - present
# Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
# the Server Side Public License v1 (SSPLv1).

NODES=3
while true; do
    for n in $(seq $NODES); do
        echo "---- NODE $n ----"
        redis-cli -p $(( 5000 + $n )) info raft
    done
    echo "==="
    sleep 1
done
