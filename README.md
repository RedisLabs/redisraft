# Redis Raft Module

This is a Redis module that implements the [Raft Consensus
Algorithm](https://raft.github.io/) as a Redis module, making it possible to
create strongly consistent clusters of Redis servers.

The Raft algorithm is implemented by a [standalone Raft
library](https://github.com/willemt/raft) by Willem-Hendrik Thiart.

## Main Features

* Create a Raft cluster of Redis processes that replicate a dataset while
  offering strong consistency, effectively a CP system.
* Most Redis data types and commands are supported as-is.
* Dynamic cluster configuration (adding / removing nodes).
* Persistent or in-memory Raft log.
* Snapshots for log compaction.
* Configurable quorum or fast reads.
* Follower proxy support.

## Getting Started

### Building

The module is mostly self contained and comes with its dependencies as git
submodules under `deps`.

To compile you will need:
* Obvious build essentials (compiler, GNU make, etc.)
* CMake
* GNU autotools (autoconf, automake, libtool)
* libbsd-dev (on Debian/Ubuntu) or an equivalent for `bsd/sys/queue.h`.

To build, simply run:

    git submodule init
    git submodule update
    make

### Starting a cluster

Note: Make sure you're using a recent Redis 6.0 release candidate or a private
build from the `unstable branch. Redis Raft depends on Module API capabilities
not available in earlier versions.

To create a three node cluster, start the first node and initialize the
cluster:

    redis-server \
        --port 5001 --dbfilename raft1.rdb \
        --loadmodule <path-to>/redisraft.so \
            raft-log-filename=raftlog1.db addr=localhost:5001
    redis-cli -p 5001 raft.cluster init

Then start the second node and make it join the cluster:

    redis-server \
        --port 5002 --dbfilename raft2.rdb \
        --loadmodule <path-to>/redisraft.so \
            raft-log-filename=raftlog2.db addr=localhost:5002
    redis-cli -p 5002 raft.cluster join localhost:5001

And the third node:

    redis-server \
        --port 5003 --dbfilename raft3.rdb \
        --loadmodule <path-to>/redisraft.so \
            raft-log-filename=raftlog3.db addr=localhost:5003
    redis-cli -p 5003 raft.cluster join localhost:5001

To query the cluster state:

    redis-cli --raw -p 5001 RAFT.INFO

And to submit a Raft operation:

    redis-cli -p 5001 SET mykey myvalue

## Documentation

Please consult the [documentation](docs/TOC.md) for more information.
