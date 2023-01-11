Deploying RedisRaft
===================

Planning
--------

### Number of Nodes

A RedisRaft cluster consists of multiple nodes. This maximizes the
availability and durability of its dataset.

As with most consensus-based systems, RedisRaft should be deployed with an odd number of nodes (e.g., 3 or 5). This prevents the classic "split-brain" scenario in which a cluster having an even number of nodes is split down the middle by a network partition, leading to neither half being able to achieve quorum. With an odd number of nodes, one part will still include a majority of nodes.

RedisRaft clusters should be deployed in a way that reduces the chance of a single failure affecting multiple nodes. For example, in most cloud environments, this would mean locating nodes in multiple availability zones within the same region.

### Persistence

> :warning: RedisRaft does not use Redis RDB or AOF files; these persistence strategies must be disabled in RedisRaft deployments.

RedisRaft implements its own persistence mechanism using a replicated log (as described in the Raft specification). All cluster configuration changes and writes to the dataset are recorded in this replicated log.

To prevent the log from growing infinitely, RedisRaft periodically generates a snapshot of its dataset and then truncates the Raft log to include only those operations performed post-snapshot.

When a RedisRaft node is restarted, it will attempt to load its snapshot and log files to restore its last state. If this cannot be done (e.g., data is lost or corrupted), the node will not be able to start. In this case, the node must be removed and re-added to the cluster as a new node (how to do this is described below).

### FSync Control

By default, RedisRaft opts for the highest level of durability. This means calling `fsync()` on the log after each write. `fsync()` is a system call that forces buffered data in a file to be written to disk. However, users can disable the use of `fsync()` to achieve better performance at the cost of reduced durability.

With `fsync()` disabled, nodes can still survive a restart or a crash, but there's a greater likelihood of corruption, which would require a node to be re-added. More specifically, disabling `fsync()` limits corruption or data loss to kernel-level crash or a full system/VM crash. Data is still safe in the event of a restart or crash at the process level.

### Dataset Size

RedisRaft is not currently optimized for very large datasets.

The process of distributing snapshots between RedisRaft nodes relies on a store-and-forward delivery of the entire dataset as a single blob, and this requires significant memory overhead.

As a rule of thumb, make sure the amount of memory available for Redis is at least 3 times larger than the expected dataset size.

Building
--------

To compile the module you will need:
* Build essentials (a compiler, GNU make, etc.)
* CMake
* GNU autotools (autoconf, automake, libtool).

To build, simply run:

    make

If successful, this should result in the creation of the `redisraft.so` shared library.

Cluster Setup
-------------

### Supported Redis Versions

RedisRaft works only on Redis 7.0 and above.

### Starting RedisRaft

To create a RedisRaft cluster, first launch the `redis-server` processes that comprise the cluster nodes. As mentioned earlier, you should use an odd number of nodes.

The configuration of each Redis instance is performed in the usual way, using the `redis.conf` configuration file or by specifying configuration directives via command-line arguments.

The RedisRaft configuration is provided as additional arguments to the module following the `loadmodule` directive (in a file or as command-line arguments).

For example, here's how to start a Redis instance and configure RedisRaft via the command line:

    redis-server \
        --bind 0.0.0.0 --port 5001 --dbfilename raft1.rdb \
        --loadmodule <path-to>/redisraft.so \
        --raft.log-filename raftlog1.db \
        --raft.addr 127.0.0.1:5001

Note the following:
* Here, the `--bind` and `--port` configure Redis to accept incoming connections on all network interfaces (by binding to `0.0.0.0`, disabling Redis protected mode) and to listen on port 5001.
* The `--dbfilename` argument sets the name of the RDB file used for Raft snapshots.
* The `--loadmodule` argument loads the RedisRaft module and accepts additional RedisRaft configuration directives (in this case, `--raft.log-filename` and `--raft.addr`).
* The module-specific `--raft.log-filename` argument set the name of the RedisRaft log file.
* The module-specific `--raft.addr` argument indicates how RedisRaft should advertise itself to other nodes. This is an optional argument. If not supplied, `--raft.addr` is inferred from the system's network interfaces and the Redis configuration. In this case we use `localhost`, as we're going to run our nodes as local processes for demonstration purposes. In a real production deployment, you want to run all of your nodes on separate machines / racks / availability zones, etc.

### Redis Configuration

RedisRaft is indirectly affected by Redis's own configuration, and there are several limitations and requirements that must be met:

| Redis Config Parameter | Required Value | Notes |
| ---------------------- | -------------- | ----- |
| databases              | 1              | Multiple databases are not supported. |
| save                   | ""             | RedisRaft uses the RDB file as its own snapshot and manages the BGSAVE operation, so Redis needs to be configured in a way that does not interfere. |
| dbfilename             | user defined   | You are free to configure the filename as desired. |
| replicaof              | <none>         | RedisRaft implements its own replication mechanism; traditional Redis replication may not be enabled. |
| requirepass            | <none>         | RedisRaft cluster nodes exchange messages on the same Redis port that the client uses; however, RedisRaft does not implement authentication. |
| maxmemory-policy       | noeviction     | Eviction must be disabled, as it is not compatible with RedisRaft's consistency guarantees. |
| appendonly             | no             | Append-only file should not be used. |
| cluster-enabled        | no             | RedisRaft is not compatible with Redis Cluster. |

### Creating the RedisRaft Cluster

A RedisRaft cluster is a logical entity which is associated with one or more cluster nodes.

#### First Node

Before creating the cluster, the first node must be launched. Initially, a node launched without an existing Raft log and snapshot will start in an `uninitialized` state, as it does not belong to any cluster.

We can validate our node is indeed `uninitialized` as follows:

    $ redis-cli -p 5001 INFO raft
    # Raft
    raft_node_id:0
    raft_state:uninitialized
    [...]

You can see the `state` indicates the node is uninitialized. Notice also that its `node_id` is `0`.

To create the cluster, run the `RAFT.CLUSTER INIT` command:

    $ redis-cli -p 5001 RAFT.CLUSTER INIT
    OK a8c5ffb374268551fd4ad0188fedcf93

The `OK` response indicates that the cluster has been created.

The response after the `OK` is the ID of the cluster (in this case, `a8c5ffb374268551fd4ad0188fedcf93`). Every RedisRaft cluster has a unique ID, which helps to prevent configuration mishaps such as reading the Raft log of the wrong cluster.

#### Additional Nodes

A single-node RedisRaft cluster is not very useful. Here's how to add another node.

The command to launch a new node should look familiar. Since we're running this second node on the same host as the first node, we need to ensure that the filenames and ports are unique:

    redis-server \
        --bind 0.0.0.0 --port 5002 --dbfilename raft2.rdb \
        --loadmodule <path-to>/redisraft.so \
        --raft.log-filename raftlog2.db \
        --raft.addr 127.0.0.1:5002

As before, we can confirm the new node has also started in `uninitialized` state and is waiting to become part of a cluster.

Since the cluster already exists, we don't need to run `RAFT.CLUSTER INIT`. Instead, we run `RAFT.CLUSTER JOIN` to join the new node to the existing cluster:

    $ redis-cli -p 5002 RAFT.CLUSTER JOIN 127.0.0.1:5001
    OK

> :bulb: Notice we use `redis-cli` to communicate with the new node, but we specify the host and port of the existing leader node as the argument to the `RAFT.CLUSTER JOIN` command.

At this point we can run `INFO raft` again to query the status of our cluster. Querying the second node should result in a response similar to this:

    $ redis-cli -p 5002 INFO raft
    # Raft
    raft_node_id:595100767
    raft_state:up
    raft_role:follower
    raft_is_voting:yes
    raft_leader_id:1733428433
    raft_current_term:1
    raft_num_nodes:2
    raft_num_voting_nodes:2
    raft_node1:id=1733428433,state=connected,voting=yes,addr=localhost,port=5001,last_conn_secs=5,conn_errors=0,conn_oks=1

Some things to notice:
* When joining the cluster, our node has been assigned a unique `node_id`.
* Its `state` has transitioned from `uninitialized` to `up`.
* It is a `follower` node because it joined the cluster when the first node was already designated as a leader, and there was no reason for re-election to take place.

We can now proceed to add additional nodes. While an even number of nodes is generally not recommended for real-world production systems, we now have a bona-fide RedisRaft cluster.

Cluster Management
------------------

### Monitoring

The primary tool for monitoring the status and health of a RedisRaft cluster is the `INFO raft` command.

RedisRaft nodes communicate with each other over the Redis port, using dedicated commands for the implementation of the Raft RPC. It is important to verify the network configuration is correct. In particular:

* The port Redis listens must not be blocked.
* The address and port advertised by each node must be correct. RedisRaft infers this from the network interface's address and the port configured by Redis. In some
  cases, this inference may be wrong (e.g., when multiple network interfaces are in use, on container network that involves NAT, etc.) In these cases, the `addr` argument can be used to specify the correct address and port.

The `node<n>:` fields describe the various nodes as currently configured, including:

| Field             | Description |
| -----             |------------ |
| state             | The current state of connection between the local and the specified node. |
| voting            | Indicates the remote node is up to date and can participate in voting for a new leader if election is called for. |
| addr              | The address of the node as advertised. |
| port              | The port of the node as advertised. |
| last_conn_secs    | The number of seconds elapsed since the last successful connection was made. |
| conn_errors       | A connection error counter. |
| conn_oks          | A successful connections counter. |

The `last_conn_secs`, `conn_errors`, and `conn_oks`, along with `state`, provide a quick way to identify connectivity issues.

### Removing Nodes

There are a couple of reasons why you might want to remove a node from a RedisRaft cluster:

  1. You're scaling down the cluster.
  2. You need to replace one node with another (e.g., when replacing a node that has permanently lost its state)

To remove a node, run `RAFT.NODE REMOVE <node_id>`. For example:

    $ redis-cli -p 5001 RAFT.NODE REMOVE 595100767
    OK

Once the node is removed, the remaining cluster nodes will drop their connections to the removed node

> :warning: The removed node itself must be terminated manually after being removed. It's especially important to shut down the node if it's still operational, as it may maintain stale state
> about the RedisRaft cluster it belonged to.

Configuration
-------------

RedisRaft has its own set of configuration parameters, which can be set in two different ways:

1. You can pass them in as command line arguments in the form of `--raft.param value` pairs.
2. You can add them to config file in the form of `raft.param value`.
3. You can run `CONFIG SET` and `CONFIG GET` in the form of `CONFIG SET raft.param value`.

RedisRaft supports the following configuration options:

### `id`

A node's unique ID.

This is recommended **only for testing**, where a predictable or easy-to-track ID is required.

*Default*: When not specified, RedisRaft allocates a random, cluster-unique ID when the node joins the cluster.

### `addr`

The address and port on which the node will be advertised. Other nodes must be able to establish connections to the local node using this address and port.

*Example*: 127.0.0.1:5001

*Default*: When not specified, `addr` will be set to the first non-local network interface as its host and will use the value of the Redis `port` for the port.

### `log-filename`

The name of the Raft log file.

RedisRaft uses this as the base name of the Raft log files, and creates additional files including `<filename>.idx`, `<filename>.tmp.idx` and `<filename>.tmp`.

*Default*: `redisraft.db.`

### `periodic-interval`

The number of milliseconds between internal RedisRaft cluster events such as heartbeats, message retransmissions, and re-election announcements.

If the `request-timeout` or `election-timeout` values must be reduced for faster failure detection, you'll also want to reduce this value as well. However, reducing this value will result with more network traffic.

*Default*: 100

### `request-timeout`

The number of milliseconds to wait before sending an AppendEntries request as a heartbeat or a re-transmission of a previously unacknowledged request.

*Default*: 200

### `election-timeout`

The number of milliseconds the cluster will wait for a heartbeat from the leader before assuming it is down and initiating a re-election.

This value should be sufficiently greater than `periodic-interval` and
`request-timeout` to avoid prematurely initiating an election, which will result in cluster instability.

*Default*: 1000

### `connection-timeout`

The number of milliseconds the cluster will wait for connections to other nodes to succeed before timing out.

*Defaults*: 3000

### `join-timeout`

The number of milliseconds the node will continue to try and connect (for join and shard group link operations) to the cluster using the provided and discovered nodes, looping through them until a connection is made, or the timeout is reached.

*Defaults*: 120000

### `reconnect-interval`

The number of milliseconds to wait to reconnect to a node when a node connection attempt fails.

*Default*: 100

### `proxy-response-timeout`

The number of milliseconds to wait for a response to a proxy request sent to a leader, before giving up and dropping the connection.

*Default*: 10000

### `response-timeout`

The number of milliseconds to wait for a response to a Raft message exchanged between nodes, before giving up and dropping the connection.

*Default*: 1000

### `follower-proxy`

Whether to enable Follower Proxy mode, as described in the [Follower Proxy Mode](Development.md#follower-proxy-mode) section. Valid values for this setting are *yes* and *no*.

If enabled, a follower node will attempt to proxy client commands to the leader node. This has the benefit of allowing clients to perform operations on all cluster nodes.

Keep in mind that not all Redis commands are supported in this mode.

If disabled, commands issued against a follower node will reply with a redirect message.

*Default*: no

### `log-max-file-size`

The maximum desired Raft log file size (in bytes). Once the file has grown beyond this size, the cluster will initiate local compaction.

*Default*: 64000000 (64MB)

### `log-max-cache-size`

The memory limit for the in-memory Raft log cache.

RedisRaft keeps an in-memory cache of the most recent Raft log entries. Once the in-memory log cache reaches the specified limit, the cluster evicts older entries from the in-memory log (since these entries also exist in the Raft log file).

*Default*: 8000000 (8MB)

### `log-fsync`

Determines if Raft log file writes must be synced. See [FSync Control](#fsync-control) for more information.

Valid values for this setting are *yes* and *no*.

*Default: yes*

### `quorum-reads`

Determines if quorum reads are used to prevent stale reads, trading off performance for consistency. See [Quorum Reads](Using.md#quorum-reads) for more information.

Valid values for this setting are *yes* and *no*.

*Default: yes*

### `sharding`

If enabled, RedisRaft handles dataset sharding in a way that is similar to Redis Cluster.

See [Sharding](Sharding.md) for more information on clustering and sharding.

Valid values for this setting are *yes* and *no*.

*Default: no*

### `slot-config`

A comma delimited list of slot ranges. 

Each element is either <slot> or <start slot>:<end slot>.

Multiple slot ranges can be defined by specifying multiple slot range elements, such as <slot>,<start_slot>:<end_slot>

See [Sharding](Sharding.md) for more information on sharding.

Valid values for this setting are 0-16383.

*Default: 0:16383*

### `shardgroup-update-interval`

The interval (in milliseconds) between attempts to refresh shardgroup configuration
of foreign shardgroup clusters.

*Default: 5000*

### `ignored-commands`

A comma separated list of additional commands that RedisRaft should not intercept, and therefore not append to the Raft log before executing.

In general this is useful when used with other modules that don't want some or all of their commands handled via raft.

*Example*: command1,command2

By default, this configuration option will be empty and no additional commands will be ignored beyond those RedisRaft is hard coded to ignore.
