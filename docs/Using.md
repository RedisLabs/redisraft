Using RedisRaft
===============

This chapter describes how RedisRaft handles Redis commands and how it differs
from the familiar Redis behavior.


Basic Guarantees
----------------

User commands received by RedisRaft are not sent immediately to Redis for
processing. Instead, they are first added to the distributed Raft log and
replicated to all cluster nodes.

RedisRaft will block the Redis client until the command has been replicated to a
majority of cluster nodes (N/2+1), and only then pass the command to Redis for
processing and deliver a reply to the client.

> :bulb: Receiving a reply is an indication that the operation has completed
> successful. However, not receiving a reply (e.g. a dropped connection) does
> not necessarily mean the operation was **not** completed.


Leader Redirection
------------------

RedisRaft requires all user interaction to be performed against the cluster's
current leader.

A client should be aware to the addresses of all cluster nodes, but cannot
determine which node is the leader at any given time. Sending a Redis command to
a non-leader node would result with a redirect response such as this:

    -MOVED <addr>:<port>

The client is then expected to establish a connection with the specified node
and re-send the command.

It is also possible that the cluster currently has no leader: it may be in the
process of re-electing a new leader, or it may even be down due to loss of
quorum. If that happens, the client will receive an error response such as this
one:

    -NOLEADER No Raft leader

In this case the client should simply re-try the operation at a later time.


Supported Commands
------------------

Most Redis commands that manipulate the dataset are supported, with the
following general exceptions:

* Multiple databases (i.e. `SELECT`) are not supported.
* Blocking commands (e.g. `BLPOP`) are not supported.
* Publish/Subscribe and Streams are not supported (yet).

The following table summarizes the supported commands along with caveats, where
applicable:

| Command           | Supported | Comments |
| ----------------- | --------- | -------- |
| APPEND            | Yes       |          |
| BITCOUNT          | Yes       |          |
| BITFIELD          | Yes       |          |
| BITOP             | Yes       |          |
| BITOPS            | Yes       |          |
| BLPOP             | No        | See [2]  |
| BRPOP             | No        | See [2]  |
| BRPOPLPUSH        | No        | See [2]  |
| BZPOPMAX          | No        | See [2]  |
| BZPOPMIN          | No        | See [2]  |
| DECR              | Yes       |          |
| DECRBY            | Yes       |          |
| DEL               | Yes       |          |
| DISCARD           | Yes       | See [3]  |
| EVAL              | Yes       | See [4]  |
| EVALSHA           | Yes       | See [4]  |
| EXEC              | Yes       | See [3]  |
| EXISTS            | Yes       |          |
| EXPIRE            | Yes       | See [1]  |
| EXPIREAT          | Yes       | See [1]  |
| GEOADD            | Yes       |          |
| GEODIST           | Yes       |          |
| GEOHASH           | Yes       |          |
| GEOPOS            | Yes       |          |
| GEORADIUS         | Yes       |          |
| GEORADIUSBYMEMBER | Yes       |          |
| GET               | Yes       |          |
| GETBIT            | Yes       |          |
| GETRANGE          | Yes       |          |
| GETSET            | Yes       |          |
| HDEL              | Yes       |          |
| HEXISTS           | Yes       |          |
| HGET              | Yes       |          |
| HGETALL           | Yes       |          |
| HINCRBY           | Yes       |          |
| HINCRBYFLOAT      | Yes       |          |
| HKEYS             | Yes       |          |
| HLEN              | Yes       |          |
| HMGET             | Yes       |          |
| HMSET             | Yes       |          |
| HSCAN             | Yes       |          |
| HSET              | Yes       |          |
| HSETNX            | Yes       |          |
| HSTRLEN           | Yes       |          |
| HVALS             | Yes       |          |
| INCR              | Yes       |          |
| INCRBY            | Yes       |          |
| INCRBYFLOAT       | Yes       |          |
| KEYS              | Yes       |          |
| LINDEX            | Yes       |          |
| LINSERT           | Yes       |          |
| LLEN              | Yes       |          |
| LPOP              | Yes       |          |
| LPUSH             | Yes       |          |
| LPUSHX            | Yes       |          |
| LRANGE            | Yes       |          |
| LREM              | Yes       |          |
| LSET              | Yes       |          |
| LTRIM             | Yes       |          |
| MGET              | Yes       |          |
| MSET              | Yes       |          |
| MSETNX            | Yes       |          |
| MULTI             | Yes       | See [3]  |
| PERSIST           | Yes       | See [1]  |
| PEXPIRE           | Yes       | See [1]  |
| PEXPIREAT         | Yes       | See [1]  |
| PFADD             | Yes       |          |
| PFCOUNT           | Yes       |          |
| PFMERGE           | Yes       |          |
| PSETEX            | Yes       |          |
| PTTL              | Yes       |          |
| RANDOMKEY         | Yes       |          |
| RENAME            | Yes       |          |
| RENAMENX          | Yes       |          |
| RPOP              | Yes       |          |
| RPOPLPUSH         | Yes       |          |
| RPUSH             | Yes       |          |
| RPUSHX            | Yes       |          |
| SADD              | Yes       |          |
| SCAN              | Yes       |          |
| SCARD             | Yes       |          |
| SCRIPT            | Yes       | See [4]  |
| SDIFF             | Yes       |          |
| SDIFFSTORE        | Yes       |          |
| SET               | Yes       |          |
| SETBIT            | Yes       |          |
| SETNX             | Yes       |          |
| SETRANGE          | Yes       |          |
| SINTER            | Yes       |          |
| SINTERSTORE       | Yes       |          |
| SISMEMBER         | Yes       |          |
| SMEMBERS          | Yes       |          |
| SMOVE             | Yes       |          |
| SORT              | Yes       |          |
| SPOP              | Yes       |          |
| SRANDMEMBER       | Yes       |          |
| SREM              | Yes       |          |
| SSCAN             | Yes       |          |
| STRLEN            | Yes       |          |
| SUNION            | Yes       |          |
| SUNIONSTORE       | Yes       |          |
| TOUCH             | Yes       | See [1]  |
| TTL               | Yes       | See [1]  |
| TYPE              | Yes       |          |
| UNLINK            | Yes       |          |
| UNWATCH           | Yes       | See [3]  |
| WATCH             | Yes       | See [3]  |
| ZADD              | Yes       |          |
| ZCARD             | Yes       |          |
| ZCOUNT            | Yes       |          |
| ZINCRBY           | Yes       |          |
| ZINTERSTORE       | Yes       |          |
| ZLEXCOUNT         | Yes       |          |
| ZPOPMAX           | Yes       |          |
| ZPOPMIN           | Yes       |          |
| ZRANGE            | Yes       |          |
| ZRANGEBYLEX       | Yes       |          |
| ZRANGEBYSCORE     | Yes       |          |
| ZRANK             | Yes       |          |
| ZREM              | Yes       |          |
| ZREMRANGEBYLEX    | Yes       |          |
| ZREMRANGEBYRANK   | Yes       |          |
| ZREMRANGEBYSCORE  | Yes       |          |
| ZREVRANGE         | Yes       |          |
| ZREVRANGEBYLEX    | Yes       |          |
| ZREVRANGEBYSCORE  | Yes       |          |
| ZREVRANK          | Yes       |          |
| ZSCAN             | Yes       |          |
| ZSCORE            | Yes       |          |
| ZUNIONSTORE       | Yes       |          |

Notes:

1. Expiration is performed as a local and independent operation on different
   cluster nodes. Because it depends on local clock as well as active expire
   logic, it may violate consistency for anything that involves volatile keys.

2. Blocking operations are not supported. You may use the command but only in
   its non blocking form.

3. `MULTI/EXEC` and `WATCH` are not supported in *Explicit Mode* or if *Follower
   Proxy* is enabled.

4. Lua scripts are supported but should be written as pure functions, i.e. as
   required when script replication rather than command replication is in use.

   For example, using commands such as `RANDOMKEY`, `SRANDMEMBER`, `TIME` or
   other non-deterministic operations must be avoided.

Read Consistency
----------------

So far the handling of commands was discussed without further distinction
between write commands (that modify the dataset) and read-only commands.

By now it's clear that the main consistency concern for a write is to only apply
it after it has been replicated to a majority, in order to guarantee it will not
be lost.

### What are stale reads?

The main consistency concern for a read, which may be less obvious, is avoiding
a stale read: reading from a node which is no longer a cluster leader.

Most of the time a node that is not the cluster's leader will be aware of that,
refuse the read and redirect the client to the true leader. This is, however,
not **always** the case. Consider the following scenario:

1. Node A is the current elected leader in a cluster of 5 nodes.
2. The network partitions and node A is no longer able to communicate with the
   other nodes.
3. The other nodes detect that node A is no longer available, and elect a new
   leader - node B.
4. Clients in the partitioned network immediately begin sending their writes to
   node B.
5. Node A is not yet aware that it is partitioned and thus no longer a leader,
   and is therefore willing to handle reads from clients on its side of the
   network partition. These are *stale reads*.

There are two things to note about this scenario:

1. One could claim the node A relies on the same time-based thresholds as the
   rest of the cluster and it should therefore initiate re-election (and fail)
   at the same time. While practically this may be true in many cases, it makes
   dangerous assumptions about the behavior of clocks and system time.
2. The reason this applies to reads but not writes is that writes require an
   explicit consensus.

### Quorum Reads

By default, RedisRaft uses quorum reads to eliminate the risk of stale reads.
Quorum reads are handled in a very similar way to writes: the leader needs to
confirm with a majority of the cluster nodes that it is still a leader, before
replying to the client.

Technically quorum reads could go through the Raft Log, but that would be
extremely inefficient as it would bloat the log with meaningless entries (as
reads don't modify the dataset).

It is possible to disable quorum reads in order to trade consistency and the
risk of stale reads for better read performance. To do that, use the
`quorum-reads=no` configuration directive.


Additional Modes
----------------

RedisRaft supports additional experimental and/or for-testing only modes, which
are described below.

### Follower Proxy Mode

Follower Proxy mode allows a follower (non-leader) node to proxy user commands
to the leader, wait for them to complete and send the reply back to the client.

The benefit of this **experimental** mode of operation is that a client no
longer needs to deal with `-MOVED` redirect replies.

This mode has several limitations:
* It cannot preserve and manage state across commands. This affects commands
  like `MULTI/EXEC` or `WATCH` which will experience undefined behavior if
  proxied to a leader (or even different leaders over time).

* It uses a single connection and therefore may introduce additional performance
  limitations.

To enable Follower Proxy mode, use specify `follower-proxy=yes` as a
configuration directive.

### Explicit Mode

By default, RedisRaft works transparently by intercepting all user commands and
processing them through the Raft Log.

It is possible to disable automatic interception and make the choice of
processing a command through Raft or directly more explicit.

RedisRaft exposes a Redis command named `RAFT` which prefixes commands Redis
commands that need to go through Raft.

For example:

    RAFT SET mykey myvalue

This would send the `SET mykey myvalue` Redis command to RedisRaft, causing it
to execute with the above guarantees.

On the other hand, performing the same operation without the `RAFT` command
prefix would cause it to execute locally **with no such guarantees** and even
more, **without being replicated to other nodes**.

To disable automatic interception and work in explicit mode, use the
`raftize-all-commands=no` configuration directive.

> :warning: Unless you really know what you're doing, there's probably no reason
> to use this mode.

