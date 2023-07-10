Using RedisRaft
===============

This chapter describes how RedisRaft handles Redis commands and how it differs from standard Redis.

Basic Guarantees
----------------

User commands received by RedisRaft are not sent immediately to Redis for processing. Instead, they are first added to the distributed Raft log and replicated to all cluster nodes.

RedisRaft will block the Redis client until the command has been replicated to a majority of cluster nodes (N/2+1), and only then pass the command to Redis for processing and reply to the client.

> :bulb: Receiving a reply is an indication that the operation has completed
> successfully. Conversely, not receiving a reply (e.g. a dropped connection) does
> not necessarily mean that the operation did **not** complete.


Leader Redirection
------------------

RedisRaft requires clients to execute all commands against the cluster's
current leader.

A client should be aware of the addresses of all cluster nodes, but cannot automatically determine which node is the leader at any given time. Sending a Redis command to a non-leader node results in a redirect response:

    -MOVED <slot> <addr>:<port>

The client is then expected to establish a connection with the specified node and re-send the command.

This response is compatible with the Redis Cluster specification, although the `<slot>` argument may contain a zero value if sharding is not enabled.

It is also possible for a RedisRaft cluster to have no leader. In this case, the cluster may be in the process of electing a new leader, or the cluster may be down due to a loss of quorum. If no leader is present, the client will receive an error response such as this one:

    -CLUSTERDOWN No Raft leader

In this case, the client should retry the operation at a later time.

Supported Commands
------------------

Most Redis commands that manipulate the dataset are supported, with the
following general exceptions:

* Multiple databases (i.e. `SELECT`) are not supported.
* Streams are not yet supported.
* WATCH and UNWATCH are not currently supported.

The following table summarizes the supported commands along with any caveats:

| Command           | Supported | Comments |
| ----------------- | --------- | -------- |
| APPEND            | Yes       |          |
| BITCOUNT          | Yes       |          |
| BITFIELD          | Yes       |          |
| BITOP             | Yes       |          |
| BITOPS            | Yes       |          |
| BLPOP             | Yes       |          |
| BRPOP             | Yes       |          |
| BRPOPLPUSH        | Yes       |          |
| BZPOPMAX          | Yes       |          |
| BZPOPMIN          | Yes       |          |
| DECR              | Yes       |          |
| DECRBY            | Yes       |          |
| DEL               | Yes       |          |
| DISCARD           | Yes       | See [2]  |
| EVAL              | Yes       | See [3]  |
| EVALSHA           | Yes       | See [3]  |
| EXEC              | Yes       | See [2]  |
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
| MULTI             | Yes       | See [2]  |
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
| SCRIPT            | Yes       | See [3]  |
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
| UNWATCH           | No        |          |
| WATCH             | No        |          |
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

1. Key expiration is performed as a local operation on each cluster node. The reason for this is that expiration depends on a local clock as well as on active expiry logic; thus, volatile keys may violate consistency.

2. `WATCH` and `UNWATCH` are not currently supported.

3. Lua scripts are supported but should be written as pure functions (i.e., as required when script replication rather than command replication is in use). This is because a RedisRaft cluster replicates the Lua script itself to each node, not the raw Redis commands that result from running the script.

   For example, avoid using non-deterministic commands such as `RANDOMKEY`, `SRANDMEMBER`, and `TIME`, as these will produce different values when executed on follower nodes.

Read Consistency
----------------

When discussing strongly-consistent systems, it's important to clarify the read and write semantics.

Writes to RedisRaft are consistent because they are applied only after being replicated to a majority of nodes.

Reads are also consistent since they're implemented as quorum reads. However, it's possible to disable quorum reads if you want to trade consistency for improved performance. This is discussed in detail below.

### Stale reads

The main consistency concern for a read is to avoid a stale read: that is, reading from a node which is no longer a cluster leader.

Most of the time, a node that is not the cluster's leader will be aware of this,
refuse the read, and redirect the client to the true leader. This is, however,
**not always** the case. Consider the following scenario in a 5-node cluster:

1. Node A is the current leader.
2. A network partition occurs, and node A is no longer able to communicate with  
   the other nodes (i.e., nodes B, C, D, and E).
3. The other nodes detect that node A is no longer available, and they elect a
   new leader: node B.
4. Clients in the same network partition as node B immediately begin sending
   their writes to node B.
5. Node A is not yet aware that it is partitioned and thus no longer a leader;
   therefore, node A is willing to handle reads from clients on its side of the
   network partition. These are *stale reads*.

There are two things to note about this scenario:

1. One could claim that node A relies on the same time-based thresholds as the
   rest of the cluster and that it should therefore initiate re-election (and fail) at the same time. While practically this may be true in many cases, it makes dangerous assumptions about the behavior of clocks and system time.
2. The reason this applies to reads but not to writes is that writes require an explicit consensus.

### Quorum Reads

By default, RedisRaft uses quorum reads to eliminate the risk of stale reads.
Quorum reads are handled in a very similar way to writes: the leader confirms with a majority of the cluster nodes that it is still a leader before
replying to the client.

(Technically, quorum reads could go through the Raft Log, but that would be
extremely inefficient as it would bloat the log with meaningless entries, since
reads don't modify the dataset).

It's possible to disable quorum reads to trade consistency and the
risk of stale reads for better read performance. To disable quorum reads, use the `quorum-reads no` configuration directive.
