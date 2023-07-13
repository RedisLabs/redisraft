RedisRaft Development
=====================

This chapter discusses topics relevant to the development of the RedisRaft
module itself.

Updating LibRaft
----------------
The LibRaft subtree is updated via

```git subtree pull --squash --prefix=deps/raft https://github.com/redislabs/raft <commit-sha>```

This operation essentially creates two commits: one commit with the new changes, and a merge commit.

Updating workflow:
- No direct changes to subtrees
- PRs with subtree changes should be rebased and not squash merged
- If possible, use a dedicated PR for subtree changes
- If not possible (due to dependencies/breaking changes), make sure the non-subtree commit(s) follow the squash-merge convention and directly reference the PR in the commit message. Because the PR is not squash merged, not doing this will make it hard to track commits to PRs.

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


### Tests Coverage

To see coverage reports for the tests, you'll first need to build Redis with gcov support:

    $ cd <redis dir>
    $ make clean
    $ make gcov

Then execute:

    $ cd <redisraft dir>
    $ mkdir build && cd build
    $ cmake .. -DCMAKE_BUILD_TYPE=Coverage
    $ make -j
    $ make coverage-report

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

### Node Membership

When a new node starts up, it can follow one of the these flows:

1. Start as the first node of a new cluster.
2. Start as a new node of an existing cluster (with a new unique ID). Initially
   it will be a non-voting node, only receiving logs (or a snapshot).
3. Start as an existing cluster node which recovers from a crash. Typically, this
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

1. Creates another log file and from now on, starts writing new entries to this file.
2. When all the entries in the first log file is committed, triggers the snapshot operation.
3. Calls fork() and creates a child process. Child process calls `RedisModule_RdbSave()` 
   to save database into a temporary RDB file.

The parent detects that the child has completed and:
1. Renames the temporary snapshot (rdb) file, so it overwrites the existing one.
2. Deletes the first log file. 
3. Renames the second log file as the first log file.

Note that while the above is not atomic, operations are ordered such that a
failure at any given time would not result with data loss.

#### Snapshot Delivery

When a Raft follower node lags behind and requires log entries that have been
compacted, a snapshot needs to be delivered instead. Leader starts sending
`RAFT.SNAPSHOT` commands that contain a snapshot chunk. Follower persist each 
chunk and after receiving the last part, it loads the RDB file. 


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

