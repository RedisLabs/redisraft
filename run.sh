#!/bin/bash
LOGLEVEL=notice
../redis/src/redis-server \
    --port 5001 \
    --loglevel $LOGLEVEL \
    --logfile redis1.log \
    --dbfilename redis1.rdb \
    --loadmodule `pwd`/redisraft.so raft-log-filename=raftlog1.db addr=localhost:5001

../redis/src/redis-server \
    --port 5002 \
    --loglevel $LOGLEVEL \
    --logfile redis2.log \
    --dbfilename redis2.rdb \
    --loadmodule `pwd`/redisraft.so raft-log-filename=raftlog2.db addr=localhost:5002

../redis/src/redis-server \
    --port 5003 \
    --loglevel $LOGLEVEL \
    --logfile redis3.log \
    --loadmodule `pwd`/redisraft.so raft-log-filename=raftlog3.db addr=localhost:5003

sleep 1
redis-cli -p 5001 raft.cluster init
sleep 1
redis-cli -p 5002 raft.cluster join localhost:5001
redis-cli -p 5003 raft.cluster join localhost:5002
read
