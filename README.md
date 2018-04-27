# Redis Raft Module

This is an **experimental, work-inprogress** Redis module that implements the
[Raft Consensus Algorithm](https://raft.github.io/) as a Redis module.

Using this module it is possible to form a cluster of Redis servers which
provides the fault tolerance properties of Raft.

The main capabilities are:
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

### Building (Linux only at the moment)

The module is mostly self contained and comes with its dependencies as git
submodules under `deps`.  To compile you will need:
* Obvious build essentials (gcc, make, etc.)
* CMake
* GNU autotools (autoconf, automake, libtool)

### Testing

The module includes a minimal set of unit tests and integration tests.  To run
them you'll need:
* lcov (for coverage analysis)
* Python and nose (for flows tests)
* redis-server in your PATH, or in `../redis/src`.

Run `make tests` to test everything.

### Starting a cluster

To create a three node cluster, start the first node and initialize the
cluster:

```
redis-server \
    --port 5001 --loadmodule <path-to>/redisraft.so \
        id=1 raftlog=raftlog1.db init addr=localhost:5001
```

Then start the second node and make it join the cluster:

```
redis-server \
    --port 5002 --loadmodule <path-to>/redisraft.so \
        id=2 raftlog=raftlog2.db join=localhost:5001 addr=localhost:5002
```

And the third node:

```
redis-server \
    --port 5003 --loadmodule <path-to>/redisraft.so \
        id=3 raftlog=raftlog3.db join=localhost:5001 addr=localhost:5003
```

To query the cluster state:

```
redis-cli --raw -p 5001 RAFT.INFO
```

And to submit a Raft operation:

```
redis-cli -p 5001 RAFT SET mykey myvalue
```

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

The trigger for configuration changes is provided by `RAFT.ADDNODE` and
`RAFT.REMOVENODE` commands.

*NOTE: Currently membership operations are node-centric. That is, a node is
started with module arguments that describe how it should behave.  For
example, a joined node will have a `join=<leader-addr>` argument telling it to
connect to the leader and execute a `RAFT.ADDNODE` command.  While there are
some benefits to this approach, it may make more sense to change to a
cluster-centric approach which is the way Redis Cluster does things.*

### Persistence

Most implementations of Raft assume a disk based crash recovery model.  This
means that a crashed process can re-start, load its state (log and snapshots)
from disk and resume.

The Raft module has a `persist` parameter which controls the persistence mode:
1. In non-persistent mode, a crashed process is equivalent to a total node
   failure (i.e. a raft node crashed and lost its disk).  If this happens, the
   process needs to re-join the cluster with a new ID when it's back up and
   receive the full log (or snapshot).  *This may not be entirely true, a Raft
   node may crash recover and still maintain it's ID*.
2. In persistent mode, the Raft log is persisted to disk and can be read in
   case of a process crash.

### Log Compaction

#### Strategies

Raft defines a mechanism for compaction of logs by storing and exchanging
snapshots.  The snapshot is expected to be persisted just like the log, and
include information that was removed from the log during compaction.

In the context of Redis, we can think about the Redis dataset as a constantly
updated snapshot.  If persistence is required, then our snapshot is version of
the data that was last saved to RDB or AOF.

**If persistence is not required**: we may be able to continuously compact the
Raft log and remove entries that have been received by *all* cluster members.
A majority in this case is not enough, as those nodes that did not receive the
entries would have to fall back to snapshots.

**If persistence is required**: ideally we can follow the same compaction
strategy above, but make sure we only compact entries that have been persisted
by Redis.

*NOTE: The Redis Module API currently does not offer a way to implement these
strategies.*

#### Snapshot Delivery

The current implementation of snapshot delivery is constrained by the existing
Redis Module API and operates this way:

1. Leader decides it needs to send a snapshot to a remote node.
2. Leader sends a `RAFT.LOADSNAPSHOT` command.  The command includes the
   *last-included-term* and *last-included-index* of the snapshot (local
   dataset) so the remote node can quickly refuse it if it was already loaded.
3. The remote node's load snapshot implementation uses `SLAVEOF` to
   temporarily become a Redis Replication Slave and fetch the snapshot from
   the leader.
4. When the replication is complete, the Raft module reconfigures Redis to be
   a master again.

`RAFT.LOADSNAPSHOT` operates in the background and responds immediately, as it
is not possible to maintain a blocked client across role changes.  Possible
responses are:
* `1` indicates snapshot loading is started.
* `0` indicates the local index already matches the required snapshot index so
  nothing needs to be done.
* `-LOADING` indicates snapshot loading is already in progress.

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
