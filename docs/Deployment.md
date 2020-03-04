Deploying RedisRaft
===================

Planning
--------

### Number of nodes

A RedisRaft cluster consists of multiple nodes in order to maximize the
availability and durability of the dataset it maintains.

Ideally it should be deployed in a way that reduces the chance of a single
failure affecting multiple nodes. For example, on most cloud environments that
would mean compute instances located in different availability zones in the same
region.

As with most consensus-based systems, RedisRaft should be deployed with an odd number of nodes (e.g. 3 or 5). This prevents the classic "split-brain" scenario, in which a single network split results with loss of quorum.

### Persistence

> :warning: RedisRaft does not use Redis RDB or AOF files, and these must be
> turned off in the Redis configuration file.

RedisRaft implements its own persistence mechanism, which is the Raft log of
operations. Every time a cluster configuration or dataset write operation is
performed, it is appended to the Raft log file.

In order to prevent the log from growing infinitely, RedisRaft implements a
snapshot mechanism which generates a snapshot file and trims the log to only
include operations that were performed after the snapshot is taken.

When a RedisRaft node is restarted, it will attempt to load its snapshot and
Raft log files in order to restore its last state. If this cannot be done (e.g.
data is lost or corrupted), the node will not be able to start and it should be
removed and re-added to the cluster as a new node (if desired).

#### Disabling Persistence

It is possible to use RedisRaft in a pure in-memory mode in order to trade
dataset durability for better performance.

An in-memory RedisRaft node cannot be restarted. As soon as the `redis-server`
process is down for any reason, the node needs to be removed and re-added to the
cluster as it loses its state.

#### Disabling fsync

By default, RedisRaft uses `fsync()` in order to guarantee that writes have
indeed been flushed from the operating system's memory to disk before
acknowledging them.

It is possible to disable the use of `fsync()` and trade durability for better
performance. The result is durability that is still better than being purely
in-memory, as a restart (or even crash) of the process would not result with
data loss or corruption.

### Dataset Size

Currently RedisRaft is not optimized for very large datasets.

The process of distributing snapshots between RedisRaft nodes relies on
store-and-forward delivery of the entire dataset as a single blob, which
involves a significant memory overhead.

As a rule of thumb, make sure the amount of memory available for Redis is at
least 3 times larger than the expected dataset size.

Building
--------

The module is mostly self contained and comes with its dependencies as git
submodules under `deps`.

To compile you will need:
* Obvious build essentials (compiler, GNU make, etc.)
* CMake
* GNU autotools (autoconf, automake, libtool)
* libbsd-dev (on Debian/Ubuntu) or an equivalent for `bsd/sys/queue.h`.

To build, simple run

    make

If successful, this should result with a `redisraft.so` Redis module shared
object.

Cluster Setup
-------------

### Supported Redis Versions

Redis Raft requires Redis 6, as it uses Module API capabilities that were not
available in earlier versions.

### Starting RedisRaft

To create the cluster, first launch the `redis-server` processes that will make
up the cluster nodes.

Redis configuration is provided in the usual way, using a configuration file or
by specifying configuration directives directly as command line arguments.

RedisRaft configuration is provided as additional arguments to the module
following the `loadmodule` directive (in a file or as arguments).

For example:

    redis-server \
        --bind 0.0.0.0 --port 5001 --dbfilename raft1.rdb \
        --loadmodule <path-to>/redisraft.so \
            raft-log-filename=raftlog1.db addr=127.0.0.1:5001

Notes:
* The `--bind` and `--port` configure Redis to accept incoming connections on
  all network interfaces (disabling Redis protected mode) on port 5001.
* The `--dbfilename` argument configures the name of the RDB file, used for Raft
  snapshot storage.
* The `--loadmodule` argument loads the RedisRaft module and passes additional
  configuration as arguments.
* The `raft-log-filename=` argument configures the name of the Raft log file.
* The `addr=` argument indicates how RedisRaft should advertise itself. This
  generally an optional argument - if not supplied it would be inferred from the
  network interfaces and Redis configuration. In this case we use localhost as
  we're going to run our nodes as local processes for educational purposes.

### Redis Configuration

RedisRaft is indirectly affected by Redis's own configuration, and there are
several limitations and requirements that must be met:

| Redis Config Parameter | Required Value | Notes |
| ---------------------- | -------------- | ----- |
| databases              | 1              | Multiple databases are not supported. It is okay to have a larger value configured as long as `SELECT` is not used to use other databases. |
| save                   | ""             | RedisRaft uses the RDB file as its own snapshot and manages the BGSAVE operation, so Redis needs to be configured in a way that does not interfere. |
| dbfilename             | user defined   | You are free to configure the filename as desired. |
| replicaof              | <none>         | RedisRaft implements its own replication mechanism and requires all nodes to operate as a standalone Redis master. |
| requirepass            | <none>         | RedisRaft cluster nodes currently exchange messages on the same Redis port and do not implement an authentication mechanism. |
| maxmemory-policy       | noeviction     | Eviction must be disabled, as it is not compatible with the consistency guarantee. |
| appendonly             | no             | Append-only file should not be used, and is generally redundant and not needed when a Raft Log is persisted to disk. |
| cluster-enabled        | no             | RedisRaft is not compatible with Redis Cluster. |

### Creating the cluster

We view the RedisRaft cluster as a logical entity which is associated with one
or more cluster nodes.

#### First Node

Before creating the cluster, the first node has to be launched. A node that is
launched with no existing Raft log/snapshot persistent data goes into an
`uninitialized` state, as it does not belong to any cluster.

We can validate our node is indeed in this state:

    $ redis-cli --raw -p 5001 RAFT.INFO
    # Raft
    node_id:0
    state:uninitialized
    [...]

You can see the `state` indicates the node is uninitialized and its `node_id` is
`0`.

To create the cluster, we initiate the `RAFT.CLUSTER INIT` command:

    $ redis-cli -p 5001 RAFT.CLUSTER INIT
    OK a8c5ffb374268551fd4ad0188fedcf93

The cluster has been created!

The second argument is the ID of the cluster. Every RedisRaft cluster has a
unique ID, which helps to prevent configuration mishaps such as reading the Raft
Log of the wrong cluster.

#### Additional Nodes

A RedisRaft cluster with a single node is not very useful, so we have to add
additional nodes to the cluster.

We'll launch their Redis process in a similar way to the way we've launched the
first node:

    redis-server \
        --bind 0.0.0.0 --port 5002 --dbfilename raft2.rdb \
        --loadmodule <path-to>/redisraft.so \
            raft-log-filename=raftlog2.db addr=127.0.0.1:5002

> :warning: For educational purposes it is possible to run multiple nodes on the
> same OS instance. To do so, just make sure every node is assigned a different
> port, and that the `dbfilename` and `raft-log-filename` configuration
> parameters point to different, unique files per node.

As before, we can confirm the new node has also started in `uninitialized` state
and is waiting to become part of a cluster.

As the cluster already exists by now, we don't want to use `RAFT.CLUSTER INIT`
but instead use `RAFT.CLUSTER JOIN` in order to make the new node join an
existing cluster:

    $ redis-cli -p 5002 RAFT.CLUSTER JOIN 127.0.0.1:5001
    OK

> :bulb: Notice we use `redis-cli` to communicate with the new node, and we
> specify the address port of the existing node.

At this point we can use `RAFT.INFO` again to query the status of our cluster.
Querying the second node should result with something similar to this:

    $ redis-cli -p 5002 --raw RAFT.INFO
    # Raft
    node_id:595100767
    state:up
    role:follower
    is_voting:yes
    leader_id:1733428433
    current_term:1
    num_nodes:2
    num_voting_nodes:2
    node1:id=1733428433,state=connected,voting=yes,addr=localhost,port=5001,last_conn_secs=5,conn_errors=0,conn_oks=1

Some things to notice now:
* When joining the cluster, our node has been assigned with a `node_id`.
* Its `state` has transitioned from `uninitialized` to `up`.
* It is a `follower` node, as it has joined the cluster when the first node was
  already a leader and there was no reason for re-election to take place.

We can now proceed to add additional nodes. While an even number of nodes is
generally not recommended for real world production systems, technically we
already have a RedisRaft cluster.

Cluster Management
------------------

### Monitoring

The primary tool for monitoring the status and health of a RedisRaft cluster is
the `RAFT.INFO` command.

RedisRaft nodes communicate with each other over the Redis port, using dedicated
commands for the implementation of the Raft RPC. It is important to verify the
network configuration is correct:

* The port Redis listens on is not blocked.
* The address and port advertised by each node is correct. RedisRaft infers this
  from the network interface's address and the port configured by Redis. In some
  cases it may be wrong though, e.g. if multiple network interfaces are in use,
  a container network that involves NAT, etc. In these cases, the `addr=`
  argument can be used to specify the correct address and port.

The `node<n>:` fields describe the various nodes as currently configured,
including:

| Field             | Description |
| -----             |------------ |
| state             | The current state of connection between the local and the specified node. |
| voting            | Indicates the remote node is up to date and can participate in voting for a new leader, if election is called for. |
| addr              | The address of the node as advertised. |
| port              | The port of the node as advertised. |
| last_conn_secs    | The number of seconds passed since last successful connection was made. |
| conn_errors       | A connection error counter. |
| conn_oks          | A successful connections counter. |

The `last_conn_secs`, `conn_errors` and `conn_oks` along with `state` provide an
quick way to identify connectivity issues.

### Removing Nodes

A node can be removed from the cluster as it is scaled down, if a node gets
replaced by a new node, or if a node has permanently lost its state and can no
longer recover.

To remove a node, use `RAFT.NODE REMOVE <node_id>`. For example:

    $ redis-cli -p 5001 RAFT.NODE REMOVE 595100767
    OK

Once removed, all remaining cluster nodes will drop connections to the removed
node and it will no longer be considered part of the cluster.

> :warning: The removed node itself should be terminated manually. RedisRaft
> does not handle this, and until it is shut down it may maintain stale state
> about the RedisRaft cluster it belonged to.

Configuration
-------------

RedisRaft has its own set of configuration parameters, which can be controlled
in different ways:

1. Passed as `param`=`value` pairs as module arguments.
2. Using `RAFT.CONFIG SET` and `RAFT.CONFIG GET`, which behave the same as as
   Redis `CONFIG` commands.

The following configuration parameters are supported:

### `id`

This option is used only for testing purposes, where a predictable or
easy-to-track ID is required. If it is not specified, RedisRaft allocates a
random ID which is validated unique when a node joins a cluster.

### `addr`

Address and port the node advertises itself with. Other nodes should be able to
establish connections to the local node using this address and port.

*Default: The IP address of the first non-local network interface, and the Redis
`port` parameter*.

### `raft-log-filename`

The name of the Raft Log file.
RedisRaft uses this as the base name of the Raft Log, and creates additional
files including `<filename>.idx`, `<filename>.tmp.idx` and `<filename>.tmp`.

*Default: `redisraft.db.`*

### `raft-interval`

The interval (in ms) in which RedisRaft wakes up and handles various cluster
chores such as heartbeats, etc.

If the timeout values are reduced for faster failure detection, it may be
necessary to reduce this value as well. Reducing this value will result with
more network traffic.

*Default: 100 (ms)*

### `request-timeout`

The interval time (in ms) for sending an AppendEntries request, as a heartbeat
or a re-transmission of a previously unacknowledged request.

*Default: 250 (ms)*

### `election-timeout`

The amount of time (in ms) we're willing to wait for a heartbeat from the
leader, before we assume it is down and initiate re-election.

This value should be sufficiently larger than `raft-interval` and
`request-timeout` in order to avoid false positives that would lead to cluster
instability.

*Default: 500 (ms)*

### `reconnect-interval`

The amount of time (in ms) we're holding back when a connection with another
node fails.

*Default: 100 (ms)*

### `follower-proxy`

Enables the Follower Proxy mode, as described in the [Follower Proxy Mode](Using.md#follower-proxy-mode) section.

If enabled, a follower node will attempt to proxy client commands to the leader
node. This has the benefit of allowing clients to perform operations on all
cluster nodes, but not all Redis commands are supported.

If disabled, a follower node will respond with a redirect message.

*Default: no*

### `raft-log-max-file-size`

The maximum desired Raft Log file size (in bytes). Once the file has grown
beyond this size, local compaction will be initiated.

*Default: 64000000 (64MB)*

### `raft-log-max-cache-size`

The memory limit for the in-memory Raft Log cache.

When RedisRaft operates using a persistent Raft Log, once the in-memory cache
reaches this limit older entries are evicted (as they also exist in the Raft Log
file).

If RedisRaft operates without a persistent Raft Log file, once the in-memory
cache reaches this limit compaction takes place.

*Default: 8000000 (8MB)*

### `raft-log-fsync`

Determines if Raft Log file writes must be synced. See [Disabling
fsync](#disabling-fsync) for more information.

*Default: yes*

### `quorum-reads`

Determines if quorum reads are used to prevent stale reads, trading off
performance for consistency. See [Quorum Reads](Using.md#quorum-reads) for
more information.

*Default: yes*

### `raftize-all-commands`

Determines if RedisRaft automatically intercepts all Redis commands and
processes them through the Raft Log.

See [Explicit Mode](Using.md#explicit-mode) for more information.

*Default: yes*

> :bulb: Note that interception of all commands requires a Redis version that
> supports the Redis Module Command Filtering API. Older versions of Redis will
> fail to enable this.
