#!/bin/bash

# Copyright Redis Ltd. 2020 - present
# Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
# the Server Side Public License v1 (SSPLv1).

usage() {
    echo "usage: redisraft_cluster.sh [--redis <executable>] [--raftmodule <module>]"
    echo "          [--modulearg <arg>] [--nodes <count>] [--port <base-port>] [redis arguments]"
    exit 2
}

panic() {
    echo "Error: $*"
    exit 1
}
echo $* > cluster
redis_args=()
module_args=()

while [ $# -gt 0 ]; do
    case $1 in
        --redis)
            shift
            redis=$1
            ;;
        --raftmodule)
            shift
            raftmodule=$1
            ;;
        --modulearg)
            shift
            module_args+=($1)
            ;;
        --nodes)
            shift
            nodes=$1
            ;;
        --port)
            shift
            port=$1
            ;;
        *)
            redis_args+=($1)
            ;;
    esac
    shift
done

# Check required arguments
for arg in redis raftmodule nodes port;
do
    if [ -z "${!arg}" ]; then
        echo "Error: --${arg} is required."
        usage
    fi
done

# Check redis-cli
redis-cli --version >/dev/null 2>&1
if [ $? != 0 ]; then
    echo "Error: redis-cli not found/not working."
    exit 3
fi

# Run servers
procs=()
kill_procs() {
    kill ${procs[@]}
    wait ${procs[@]}
}
trap "kill_procs" EXIT
export LD_LIBRARY_PATH=`pwd`    # For redisraft.so in case it's not abspath
for n in $(seq ${nodes}); do
    p=$((${port} + $n - 1))
    raftlog=redisraft${n}.db
    rm -f ${raftlog} ${raftlog}.idx
    ${redis} --loadmodule ${raftmodule} \
        --raft.id ${n} \
        --raft.addr 127.0.0.1:${p} \
        --raft.log-filename redisraft${n}.db \
        --raft.follower-proxy yes \
        ${module_args[@]} \
        --logfile redis${n}.log \
        --port ${p} \
        ${redis_args[@]} &
    procs+=($!)
done

# Wait for things to settle
sleep 1

# Create cluster
redis-cli -p ${port} RAFT.CLUSTER INIT || panic "Failed to configure cluster"
for n in $(seq 2 ${nodes}); do
    p=$((${port} + $n - 1))
    redis-cli -p ${p} RAFT.CLUSTER JOIN 127.0.0.1:${port}
done

# Wait to be killed
sleep infinity
