RedisRaft Development
=====================

This chapter discusses topics relevant to the development of the RedisRaft
module itself.

Testing
-------

### Running Tests

The module includes a basic set of unit tests and integration tests. To run
them you'll need:

* lcov (for coverage analysis, on Linux)
* Python and a few packages (e.g. pytest, redis, etc.)
* redis-server in your PATH, or in `../redis/src`.

To run tests in a Python virtualenv, follow these steps:

    $ mkdir -p .env
    $ virtualenv .env
    $ . .env/bin/activate
    $ pip install -r tests/integration/requirements.txt
    $ make tests

Integration tests are based on pytest, and specific parameters can be provided
to configure tests.

For example, running a single test with live logging of all output:

    pytest tests/integration/test_snapshots.py::test_snapshot_delivery -v --log-cli-level=debug

To see a full list of custom test configuration options, use:

    pytest tests/integration --help

### Unit Test Coverage

To run unit tests and see a detailed coverage report:

    $ make clean
    $ make COVERAGE=1 unit-tests
    $ make unit-lcov-report

### Integration Tests Coverage

To see coverage reports for the entire set of integration tests, you'll first
need to build Redis with gcov support:

    $ cd <redis dir>
    $ make clean
    $ make gcov

Then execute:

    $ cd <redisraft dir>
    $ make clean
    $ COVERAGE=1 make
    $ make integration-tests
    $ make integration-lcov-report

### Jepsen

See [jepsen/README.md](../jepsen/README.md) for information on using Jepsen to test
Redis Raft for linearizability violations.

General Design
--------------

### Overview

A single `RAFT` command is implemented as a prefix command for clients to submit
requests to the Raft log.

This triggers the following series of events:

1. The command is appended as an entry to the local Raft log (in-memory cache and
   file).
2. The log is replicated to the majority of cluster members. The RedisRaft module
   uses module-specific commands against other nodes to accomplish this.
3. Once the log has been replicated to a majority of nodes and RedisRaft
   determines that the entry can be committed, the command is executed locally on all nodes as a regular Redis command and the response is sent to the user.

Raft communication between cluster members is handled by `RAFT.AE` and
`RAFT.REQUESTVOTE` commands, which are also implemented by the RedisRaft module.

The module starts a background thread which handles all Raft-related tasks, such
as:
* Maintaining connections with all cluster members
* Periodically sending heartbeats (leader) or initiating an election if
  heartbeats are not seen (follower/candidate).
* Processing committed entries (delivering to Redis in a thread-safe context)

All received Raft commands are placed on a queue and handled by the Raft thread
itself, using the blocking API and a thread-safe context.

### Node Membership

When a new node starts up, it can follow one of the these flows:

1. Start as the first node of a new cluster.
2. Start as a new node of an existing cluster (with a new unique ID). Initially
   it will be a non-voting node, only receiving logs (or a snapshot).
3. Start as an existing cluster node which recovers from a crash. Typically this
   is done by loading persistent data from disk.

Configuration changes are propagated as special Raft log entries, as described
in the Raft paper.

The trigger for configuration changes is provided by `RAFT.NODE ADD` and
`RAFT.NODE REMOVE` commands.

*NOTE: Currently membership operations are node-centric. That is, a node is
started with module arguments that describe how it should behave. For example, a
`RAFT.CLUSTER JOIN` is invoked on a new node in order to initiate a connection
to the leader and execute a `RAFT.NODE ADD` command.
While there are some benefits to this approach, it may make more sense to change
to a cluster-centric approach which is the way Redis Cluster does things.*

### Persistence

The Raft Log is persisted to disk in a dedicated log file managed by the module.
In addition, an in-memory cache of recent entries is maintained in order to
optimize log access.

The file format is based on RESP encoding and is similar to an AOF file. It
begins with a header entry that stores the Raft state at the time the log was
created, followed by a list of entries.

The header entry may be updated to persist additional data such as voting
information. For this reason, the entry size is fixed.

In addition, the module maintains a simple index file to store the 64-bit
offsets of every entry written to the log.

The index is updated on the fly as new entries are appended to the Raft log, but
if crash recovery takes place it will not be considered a source of truth and will be reconstructed as the Raft log is read.

### Log Compaction

Raft defines a mechanism for compaction of logs by storing and exchanging
snapshots. The snapshot is expected to be persisted just like the log, and
include information that was removed from the log during compaction.

#### Compaction & Snapshot Creation

When the Raft modules determines it needs to perform log compaction, it does the
following:

First, a child process is forked and:
1. Performs a Redis `SAVE` operation after modifying the `dbfilename`
   configuration, so a temporary file is created.
2. Iterates the Raft log and creates a new temporary Raft log with only the
   entries that follow the snapshot.
3. Exits and reports success to the parent.

The parent detects that the child has completed and:
1. Renames the temporary snapshot (rdb) file so it overwrites the existing one.
2. Appends all Raft log entries that have been received since the child was
   forked to the temporary Raft log file.
3. Renames the temporary Raft log so it overwrites the existing one.

Note that while the above is not atomic, operations are ordered such that a
failure at any given time would not result with data loss.

#### Snapshot Delivery

When a Raft follower node lags behind and requires log entries that have been
compacted, a snapshot needs to be delivered instead:

1. Leader decides it needs to send a snapshot to a remote node.
2. Leader sends a `RAFT.LOADSNAPSHOT` command, which includes the snapshot (RDB
   file) as well as *last-included-term* and *last-included-index*.
3. Follower may respond in different ways:
   * `1` indicates snapshot was successfully loaded.
   * `0` indicates the local index already matches the required snapshot index
     so nothing needs to be done.
   * `-LOADING` indicates snapshot loading is already in progress.

*NOTE: Because of the store-and-forward implementation in Redis, this is not
very efficient and will fail on very large datasets. In the future this should
be optimized*.


MULTI/EXEC Support
------------------

### Basic Flow

When `RAFT MULTI` is issued, an entry is created in the `multi_clients`
dictionary for the current client. When an entry exists, every new command is
append to the entry and a `QUEUED` response is generated.

When `RAFT EXEC` is issued and an entry exists for the client, a
`RaftRedisCommandArray` with all commands is created and processed as a regular
command.

A Redis Module API Event handler is created to catch client disconnect events,
and clean up their state (in case a client initiates `MULTI` and drops the
connection).

### WATCH

`WATCH` needs to be implemented by the Raft module itself, because we need to
detect and fail an `EXEC` before creating an entry and propagating it.

Supporting `WATCH` in proxying mode is difficult because of the need to
propagate the watched keys state between nodes on leader election (in case
re-election takes place between the `WATCH` and `EXEC`).

To support watch:
1. Every `WATCH` command should be propagated to Redis directly.
2. When `MULTI` and `EXEC` are performed, we consult the Module API context to
   determine if we have dirty keys, in which case EXEC would fail. In that case,
   we fail it ourselves.

### Follower Proxy Mode Support

Proxying should work for the simple `MULTI` case, but there are issues with
WATCH:

1. We need to be sure we maintain a dedicated proxy connection per client,
   because `WATCH` lives in a connection context.
2. If a new leader is elected between `WATCH` and `EXEC`, we must not proxy
   commands as the `WATCH` state will be invalid on the new leader.

At the moment this is not implemented.


Special Modes
----------------

RedisRaft supports two experimental and/or for-testing-only modes, which are described below.

### Follower Proxy Mode

Follower Proxy mode allows a follower (non-leader) node to proxy user commands
to the leader, wait for them to complete, and send the reply back to the client.

The benefit of this **experimental** mode of operation is that a client no
longer needs to deal with `-MOVED` redirect replies.

This mode has several limitations:
* It cannot preserve and manage state across commands. This affects commands
  like `MULTI/EXEC` and `WATCH` which will exhibit undefined behavior if
  proxied to a leader (or even different leaders over time).

* It uses a single connection and therefore may introduce additional performance
  limitations.

To enable Follower Proxy mode, specify `follower-proxy yes` as a
configuration directive.

### Explicit Mode

By default, RedisRaft works transparently by intercepting all user commands and
processing them through the Raft Log.

***Explicit Mode*** disables this automatic interception and allows the client to decide which commands should be run through the Raft log on a command-by-command basis.

To allow for this this, RedisRaft's **explicit mode** exposes the `RAFT` command.

For example:

    RAFT SET mykey myvalue

This sends the `SET mykey myvalue` Redis command to RedisRaft, causing it
to execute with the strongly-consistent guarantees.

TODO/Wish List
--------------

- [ ] Add NO-OP log entry when starting up, to force commit index computing.
- [ ] Latency optimizations through better concurrency (batch operations,
      distribute entries while syncing to disk, etc.).
- [ ] Improve debug logging (pending Redis Module API support).
- [ ] Batch log operations (pending Raft lib support).
- [ ] Cleaner snapshot RDB loading (pending Redis Module API support).
- [ ] Stream snapshot data on LOAD.SNAPSHOT (pending hiredis support/RESP3 or
  a dedicated side channel implementation).
- [ ] Improve follower proxy performance.
