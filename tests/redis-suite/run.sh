#!/bin/bash

REDIS_DIR=${REDIS_DIR:-${PWD}/../redis}

export REDIS_SERVER_BINARY=${REDIS_DIR}/src/redis-server
export REDIS_CLI_BINARY=${REDIS_DIR}/src/redis-cli
export ADDITIONAL_OPTIONS="raft-log-fsync no"

setup() {
    pushd ./utils/create-cluster
    ./create-cluster stop
    ./create-cluster clean
    ./create-cluster start
    ./create-cluster create
    popd
}

teardown() {
    pushd ./utils/create-cluster
    ./create-cluster stop
    popd
}

run_tests() {
    local tests_dir=${PWD}/tests/redis-suite
    pushd $REDIS_DIR
    ./runtest \
        --host 127.0.0.1 \
        --port 5001 \
        --cluster-mode \
        --singledb \
        --ignore-encoding \
        --ignore-digest \
        --skipfile ${tests_dir}/skip.txt \
        --tags -needs:repl \
        --tags -needs:debug \
        --tags -needs:save \
        --tags -needs:reset \
        --tags -needs:config-maxmemory \
        --tags -pause \
        --tags -tracking \
        $*
    popd
}

# Make sure we're running from the right place
if [ ! -f tests/redis-suite/run.sh ]; then
    echo Please run this script from the top level directory.
    exit 1
fi

setup
run_tests $*
teardown
