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

The module includes a set of unit tests and integration tests.  To run them
you'll need:
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

## Implementation

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

### Persistence

Most implementations of Raft assume a disk based crash recovery model.  This
means that a crashed process can re-start, load its state (log and snapshots)
from disk and resume.

The raft module has a `persist` parameter which controls the persistence mode:
1. In non-persistent mode, a crashed process is equivalent to a total node
   failure (i.e. a raft node crashed and lost its disk).  If this happens, the
   process needs to re-join the cluster with a new ID when it's back up and
   receive the full log (or snapshot).
2. In persistent mode, the Raft log is persisted to disk and can be read in
   case of a process crash.

### Log Compaction

Raft defines a mechanism for compaction of logs by storing and exchanging
snapshots.  In the context of Redis, we can think about the Redis dataset as a
constantly updated snapshot.

Snapshot delivery is based on the built in Redis replication mechanism.  A
follower temporarily becomes a slave and replicates the dataset from the
leader.

If persistence is not enabled, this means the Raft log can be continuously
compacted with different possible strategies:
1. Removing entries seen by all members
2. Removing entries seen by the majority, leaving a configurable fixed number
   of "backlog" entries that can be delivered to follows lagging behind.

If persistence is enabled, then log compaction can apply only to log entries
that have been committed and persisted (on last RDB save or AOF write).

### Read request handling

The naive implementation of reads is identical to writes.  Reads are prefixed
by the `RAFT` command which places the read command in the Raft log and only
executes it when it can be "committed".

This has two limitations:
1. It does not offer a choice for faster, but potentially stale reads which
   many Raft implementations support.
2. It bloats the Raft log for no reason.  An optimization would trigger a
   heartbeat to avoid stale reads but not generate a real log entry.
