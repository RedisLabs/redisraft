# Redis Raft Module

This is an **experimental, work-inprogress** Redis module that implements the
[Raft Consensus Algorithm](https://raft.github.io/) as a Redis module.

Using this module it is possible to form a cluster of Redis servers which
provides the fault tolerance properties of Raft:

1. Leader election.  The servers elect a single leader at a time, and only the
   leader is willing to accept user requests.  Other members of the cluster
   will reply with a redirect message.
2. User requests are replied only after they have been replicated to a majority
   of the cluster.
3. The Raft log and other critical state can be persisted to disk or stored
   in-memory only.
4. Cluster configuration is dynamic and it is possible to add or remove members
   on the fly.

## Getting Started

### Building

The module is mostly self contained and comes with its dependencies as git
submodules under `deps`.  To compile you will need:
* Obvious build essentials (compiler, GNU make, etc.)
* CMake
* GNU autotools (autoconf, automake, libtool)

### Testing

The module includes a minimal set of unit tests and integration tests.  To run
them you'll need:
* lcov (for coverage analysis, on Linux)
* Python and a few packages (e.g. nose, redis, etc.)
* redis-server in your PATH, or in `../redis/src`.

To run tests in a Python virtualenv, follow these steps:

```
$ mkdir -p .env
$ virtualenv .env
$ . .env/bin/active
$ pip install -r tests/integration/requirements.txt
$ make tests
```

Integration tests are based on Python nose, and specific parameters can be
provided to configure tests.  For example, running a single test with
no logging capture, so output is printed in runtime:

```
NOSE_OPTS="-v --nologcapture --logging-config=tests/integration/logging.ini --tests tests/integration/TestSnapshots.py:test_new_snapshot_with_old_log" make integration-tests
```

### Jepsen

The module ships with a basic Jepsen test to verify its safety.  It is set up to
execute a 5-node cluster + a Jepsen control node, based on the Jepsen Docker
Compose setup.

First, you'll need to build a tarball that contains Redis + the module, compiled
for a debian image used for Jepsen.  To do that, run:

```
./jepsen/docker/build_dist.sh
```

This should result with a `jepsen/docker/dist` directory being created with a
single tarball.

To start Jepsen:

```
cd jepsen/docker
./up.sh
```

This will launch Jepsen's built-in web server on `http://localhost:8080`, but do
nothing else.  To start an actual test, use a command such as:

```
docker exec -ti jepsen-control bash
lein run test --time-limit 60 --concurrency 200
```

### Starting a cluster

To create a three node cluster, start the first node and initialize the
cluster:

```
redis-server \
    --port 5001 --dbfilename raft1.rdb \
    --loadmodule <path-to>/redisraft.so \
        id=1 raftlog=raftlog1.db addr=localhost:5001
redis-cli -p 5001 raft.cluster init
```

Then start the second node and make it join the cluster:

```
redis-server \
    --port 5002 --dbfilename raft2.rdb \
    --loadmodule <path-to>/redisraft.so \
        id=2 raftlog=raftlog2.db addr=localhost:5002
redis-cli -p 5002 raft.cluster join localhost:5001
```

And the third node:

```
redis-server \
    --port 5003 --dbfilename raft3.rdb \
    --loadmodule <path-to>/redisraft.so \
        id=3 raftlog=raftlog3.db addr=localhost:5003
redis-cli -p 5003 raft.cluster join localhost:5001
```

To query the cluster state:

```
redis-cli --raw -p 5001 RAFT.INFO
```

And to submit a Raft operation:

```
redis-cli -p 5001 RAFT SET mykey myvalue
```

## Using the module

TBD: multi/exec, consensus vs stale reads

## Configuration

TBD: Specify all config parameters

## Module Architecture

The module uses [Standalone C library implementation of
Raft](https://github.com/willemt/raft) by Willem-Hendrik Thiart.

A single `RAFT` command is implemented as a prefix command for users to submit
requests to the Raft log.  This triggers the following series of events:

1. The command is appended to the local log.  If log is persistent, it is also
   persisted to disk.
2. The log is replicated to the majority of cluster members.  This is done by
   the Raft module communicating with the other Raft modules using
   module-specific commands.
3. When a majority has been reached and Raft determines the entry can be
   committed, it is executed locally as a regular Redis command and the
   response is sent to the user.

Raft communication between cluster members is handled by `RAFT.APPENDENTRIES`
and `RAFT.REQUESTVOTE` commands which are also implemented by the module.

The module starts a background thread which handles all Raft related tasks,
such as:
* Maintain connections with all cluster members
* Periodically send heartbeats (leader) or initiate vote if heartbeats are not
  seen (follower/candidate).
* Process committed entries (deliver to Redis through a thread safe context)

All received Raft commands are placed on a queue and handled by the Raft
thread itself, using the blocking API and a thread safe context.

## Raft Cluster Design

### Node Membership

When a new node starts up, it can follow one of the following flows:

1. Start as the first node of a new cluster.
2. Start as a new node of an existing cluster (with a new unique ID).
   Initially it will be a non-voting node, only receiving logs (or snapshot).
3. Start as an existing cluster node which recovers from crash.  Typically
   this is done by loading persistent data from disk.

Configuration changes are propagated as special Raft log entries, as described
in the Raft paper.

The trigger for configuration changes is provided by `RAFT.NODE ADD` and
`RAFT.NODE REMOVE` commands.

*NOTE: Currently membership operations are node-centric. That is, a node is
started with module arguments that describe how it should behave.  For example,
a `RAFT.CLUSTER JOIN` is invoked on a new node in order to initiate a connection
to the leader and execute a `RAFT.NODE ADD` command.

While there are some benefits to this approach, it may make more sense to change
to a cluster-centric approach which is the way Redis Cluster does things.*

### Persistence

Most implementations of Raft assume a disk based crash recovery model.  This
means that a crashed process can re-start, load its state (log and snapshots)
from disk and resume.

The Raft module can work in either persistent or non-persistent mode.  In
persistent mode: 
1. A `raftlog=` parameter is required and specifies the name of the Raft log
   file to use.
2. On startup, Raft log is read and applied on top of the latest snapshot (i.e.
   RDB file as loaded by Redis).

If no Raft log is specified, the module assumes it operates in non-persistent
mode:
1. If the process crashes, it is equivalent to a total node failure (e.g. as if
   a persistent Raft node crashed and lost its files).
2. On start, the process will need to re-join the cluster as a new node (require
   a new ID), and the operator will need to explicitly delete the old node ID.

### Log Compaction

Raft defines a mechanism for compaction of logs by storing and exchanging
snapshots.  The snapshot is expected to be persisted just like the log, and
include information that was removed from the log during compaction.

#### Live Snapshot
In the context of Redis, we can think about the Redis dataset as a constantly
updated snapshot.  If persistence is required, then our snapshot is version of
the data that was last saved to RDB or AOF.

#### Compaction Threshold

**If persistence is not required**: we may be able to continuously compact the
Raft log and remove entries that have been received by *all* cluster members.
A majority in this case is not enough, as those nodes that did not receive the
entries would have to fall back to snapshots.

**If persistence is required**: ideally we can follow the same compaction
strategy above, but make sure we only compact entries that have been persisted
by Redis.

*NOTE: The Redis Module API currently does not offer a way to implement these
strategies.  Currently compaction can be done explicitly or automatically by
configuring specific thresholds.*

#### Compaction & Snapshot Creation

When the Raft modules determines it needs to perform log compaction, it does the
following:

First, a child process is forked and:
1. Performs a Redis `SAVE` operation after modifying the `dbfilename`
   configuration, so a temporary file is created.
2. Iterates the Raft log and creates a new temporary Raft log with
   only the entries that follow the snapshot.
3. Exits and reports success to the parent.

The parent detects that the child has completed and:
1. Renames the temporary snapshot (rdb) file so it overwrites the
   existing one.
2. Appends all Raft log entries that have been received since the
   child was forked to the temporary Raft log file.
3. Renames the temporary Raft log so it overwrites the existing one.

#### Snapshot Delivery

When a Raft follower node lags behind and requires log entries that have been
compacted, a snapshot needs to be delivered instead:

1. Leader decides it needs to send a snapshot to a remote node.
2. Leader sends a `RAFT.LOADSNAPSHOT` command, which includes the
   snapshot (RDB file) as well as *last-included-term* and
   *last-included-index*.
3. Follower may respond in different ways:
   * `1` indicates snapshot was successfully loaded.
   * `0` indicates the local index already matches the required snapshot index so
     nothing needs to be done.
   * `-LOADING` indicates snapshot loading is already in progress.

*NOTE: Because of the store-and-forward implementation in Redis, this is not
very efficient and will fail on very large datasets. In the future this should
be optimized*.

#### Read request handling

The naive implementation of reads is identical to writes.  Reads are prefixed
by the `RAFT` command which places the read command in the Raft log and only
executes it when it can be "committed".

This has two limitations:
1. It does not offer a choice for faster, but potentially stale reads which
   many Raft implementations support.
2. It bloats the Raft log for no reason.  An optimization would trigger a
   heartbeat to avoid stale reads but not generate a real log entry.

A better approach, which needs to be implemented at the Raft library level, is
to synchronize reads with heartbeats received from the majority.

## Roadmap

- [ ] Decouple log implementation, to allow storing most of the log on disk and
      only a recent cache in memory (Raft lib).
- [ ] Optimize reads, so they are not added as log entries (Raft lib).
- [ ] More friendly membership management through Redis commands, to avoid
      changing process arguments.
- [ ] Add NO-OP log entry when starting up, to force commit index computing.
- [ ] Improve automatic proxying performance.
- [ ] Improve debug logging (Redis Module API).
- [ ] Batch log operations (Raft lib).
- [ ] Optimize memory management (Raft lib).
- [ ] Cleaner snapshot RDB loading (Redis Module API).
- [ ] Stream snapshot data on LOAD.SNAPSHOT (hiredis streaming support).
