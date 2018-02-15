#!/bin/bash
../redis/src/redis-server --port 5001 --loadmodule `pwd`/rafter.so 1 &
../redis/src/redis-server --port 5002 --loadmodule `pwd`/rafter.so 2 &
sleep 1
redis-cli -p 5001 raft.addnode 2 localhost:5002
redis-cli -p 5002 raft.addnode 1 localhost:5001
read
