RedisRaft Sharding Support
==========================

## Introduction

This describes a work-in-progress design that adds sharding support to
RedisRaft.

The goal is to allow sharding of the dataset across multiple RedisRaft
clusters, such that every cluster is assigned specific hash slots.

The main design guideline we follow is to provide an alternative, RedisRaft
based implementation of the Redis Cluster Protocol.

This should allow existing clients with cluster support to seamlessly
interoperate with a RedisRaft cluster and gain the above fail-over and sharding
capabilities.

### Terminology

*Redis Cluster* is the native open-source Redis cluster implementation, which
relies on its own gossip protocol and consensus mechanism over a cluster bus
protocol.

*Redis Cluster Protocol* is the Redis protocol Redis Cluster exposes to clients
that support it, which includes the `CLUSTER SLOTS` command, `-MOVED` responses,
etc.

*RedisRaft Cluster* is a group of Redis servers that execute RedisRaft and form
a single reliable cluster based on the Raft algorithm. Currently, all nodes on a
RedisRaft Cluster share the same dataset and have no sharding support.

### Scope & Limitations

This design follows a strict agile approach and attempts to incrementally
improve the initial non-clustered RedisRaft implementation.

As such, it has several limitations which are out of scope and not addressed at
this point (but are likely to be mandatory as we move on):

* Configuration processes are currently manual with minimal to no tooling.

* Slot range assignment will be static, so no re-sharding or migration of slots
  is currently possible. This is a major limitation which will clearly have to
  be addressed as a next step.

### RedisRaft Configuration

We assume that RedisRaft is configured as follows:

* Follower proxy is disabled.
* Nodes have their `addr` configuration set with an IP address and not a host
  name, to be compliant with Redis Cluster addressing.
* The new `sharding` parameter is set to `yes`.

## Design

### Redis Cluster Protocol and Fail-over

> :warning: This part should move elsewhere, failover and the use of CLUSTER SLOTS
should be considered an integral part of any client and has nothing to do with
sharding.

To support the basic client-managed failover scenario, RedisRaft needs to
respond to `CLUSTER SLOTS` commands.

For a single RedisRaft cluster with no sharding, the response describes a single
shard with the full slot range (0-16383) and information about all RedisRaft
nodes. The leader is listed first (as the Redis Cluster Protocol describes
master nodes) followed by all followers. For example:

```
127.0.0.1:5001> CLUSTER SLOTS
1) 1) (integer) 0
   2) (integer) 16383
   3) 1) "127.0.0.1"
      2) (integer) 5001
      3) "29f800f827b920b54d9fcd13784dd42f68a53323"
   4) 1) "127.0.0.1"
      2) (integer) 5002
      3) "29f800f827b920b54d9fcd13784dd42f4575494c"
   5) 1) "127.0.0.1"
      2) (integer) 5003
      3) "29f800f827b920b54d9fcd13784dd42f565c0a06"
```

This configuration is actually invalid in a Redis Cluster, which requires at
least three slot ranges assigned to three cluster nodes, but it is not likely
that clients will enforce it and should not be difficult to fix if they do.

Cluster-compatible clients are expected to cache this information and, if a
master node becomes unavailable, attempt to reconnect to a different random
node.

We now have two fail-over mechanisms that complement each other:

* A client that has lost a connection and is unable to reconnect uses the cached
  information about other nodes to attempt to re-establish a connection.

* A client that attempts to communicate with a follower gets a `-MOVED <slot>
  <host>:<port>` response and redirects to the leader. This may be a result of a
  leader that has been demoted, or a client that has re-connected to a random
  node and ended up hitting a follower. 

### Sharding and shardgroups

To support sharding, we set up multiple independent RedisRaft clusters.

RedisRaft clusters are configured with the hash slot ranges that are assigned to
them. For now we assume this is a static and manual configuration which must be
done correctly at the time of setup, so every slot is mapped to exactly one
cluster.

Every RedisRaft cluster needs to be able to provide clients with information
about hash slots and cluster nodes of the other RedisRaft clusters. This happens
in two occasions:

* An attempt to access a key that does not belong to the local RedisRaft cluster
  needs to result with a `-MOVED` response that points at another RedisRaft
  cluster.

* The reply to `CLUSTER SLOTS` includes information about all RedisRaft
  clusters.

To support that, we introduce a new configuration element that describes an
external RedisRaft cluster along with its hash slots and nodes. We refer to this
as a *shardgroup*. So, in a sharding topology that consists of three RedisRaft
clusters, every cluster refers to the other two clusters as two shardgroups.

We assume that the mapping of hash slots to RedisRaft clusters is static.
However, we cannot assume that node configuration is static as nodes may be
added or removed. In particular, the leader node may get re-elected at any time.

To address that, we need a mechanism where RedisRaft clusters probe each other
periodically in order to obtain updated topology information: the list of nodes
and identity of the leader node.

Such an update mechanism is limited and means clusters will not always have
up-to-date information about the nodes and leadership status of other clusters:

* A RedisRaft cluster may provide a `CLUSTER SLOTS` reply that:
  1. Incorrectly indicates a follower node is a leader.
  2. Lists nodes that have been recently removed.
  3. Omits nodes that have been recently added.

* The above may also apply to information returned by a `-MOVED` reply.

* Different RedisRaft clusters may provide a different `CLUSTER SLOTS` reply.

We suggest that while ideally we'd want RedisRaft clusters to implement
consensus to maintain configuration, the downside of not doing so may not be as
dramatic as it may first seem:

* A client that ends up communicating with a follower rather than a leader will
  receive a `-MOVED` response anyway. This introduces an additional redirection
  hop, but RedisRaft ensures the response in this case is valid.

* Stale node configuration is not a problem as long as the client is still able
  to communicate with at least one node in every RedisRaft cluster.

So, the only real risk here involves cluster configuration that diverges too
much without being propagated between clusters. This can happen if configuration
changes are more rapid than propagation (very unlikely), or because the
propagation mechanism fails. This can be addressed in different ways:

* Setting up an administrative mechanism that alerts and, possibly, eventually
  shuts down a cluster when RedisRaft clusters fail to exchange updates.

* Require that all administrative operations (node addition and removal) are
  done in a way that ensures all RedisRaft clusters remain up to date.

It is likely that future designs that will also address dynamic re-sharding and
migration of slots will be forced to rely on consensus between RedisRaft
clusters, so this design may require radical changes at that point.

However, as we maintain a strict agile approach and need to demonstrate
incremental progress we consider this an acceptable trade-off.

## Configuration Guide

### Redis Cluster Mode without Sharding

Before using Redis Cluster mode, make sure your Redis client supports the Redis
Cluster protocol.

Use the following RedisRaft configuration:

* The `addr` parameter should be supplied as an IP address and not a hostname,
  to be compliant with the Redis Cluster protocol.
* The `sharding` parameter should be set to `yes`.
* The `slot-config` parameter should be set to their default values, which 
  indicate all slots are managed by the local cluster (no sharding).
* The `follower-proxy` parameter should be set to `no` (default).

### Redis Cluster Mode with Sharding

To use RedisRaft with sharding, you will first need to decide on the number of
shards you are going to use. Every shard is essentially a RedisRaft cluster
which consists of N instances of Redis.

Start by setting up the different RedisRaft clusters individually. You will need
to use the Redis Cluster Mode configuration as described above.

The `slot-config` parameter should be assigned individually for every RedisRaft 
cluster and indicate slot assignment. All nodes of the same RedisRaft cluster 
should have the same configuration.

For example, if you're creating three equal shards you may use:

* `slot-config 0:5460` for RedisRaft cluster 1.
* `slot-config 5461:10921` for RedisRaft cluster 2.
* `slot-config 10922:16383` for RedisRaft cluster 3.

After creating the three clusters, you will need to configure each cluster to
recognize the other two clusters as external shardgroups. We refer to this
operation as *shardgroup linking*.

For every cluster, you need to issue RAFT.SHARDGROUP LINK with the address of
a node in the foreign cluster. For example:

    redis-cli -h <cluster-1-node> -p <cluster-1-port> RAFT.SHARDGROUP LINK <cluster-2-node>:<cluster-2-port>
    redis-cli -h <cluster-1-node> -p <cluster-1-port> RAFT.SHARDGROUP LINK <cluster-3-node>:<cluster-3-port>

This should be repeated for all clusters.

## Getting started with create-shard-groups

The `utils/create-shard-groups` script can be used to simplify and automate setup
of local sharding configurations **for testing or development purposes**.

To get started, change into its directory:

    cd utils/create-shard-groups

Next, create a `config.sh` that can hold any custom configuration.

By default, `create-shard-groups` creates 3 shards. We will instead use a
configuration of 5 shards and also disable `fsync` to favor performance over
safety. Our `config.sh` will look like so:

    NUM_GROUPS=5
    ADDITIONAL_OPTIONS="--raft.log-fsync no"

Next, we execute `create-shard-groups` to set up our environment:

    ./create-shard-groups setup

This will result with a directory created for every shard (RedisRaft cluster),
along with a local configuration file that the `create-cluster` script can use
to locally set up the cluster.

Next, we execute `create-shard-groups start` to start up the Redis processes for
every RedisRaft cluster:

    ./create-shard-groups start

At this point the Redis processes are up but the clusters and sharding are not
configured yet. To do that, we run:

    ./create-shard-groups create

This will set up the different RedisRaft clusters by executing `RAFT.CLUSTER
INIT` and `RAFT.CLUSTER JOIN` on all clusters. Once the clusters are set up, the
script will perform shardgroup linking using `RAFT.SHARDGROUP LINK`.

When this is done, our setup is ready. We can attempt to connect to random
clusters and nodes:

    redis-cli -p 5101 cluster slots     # Cluster 1, Node 1
    redis-cli -p 5302 cluster slots     # Cluster 3, node 2

If we later want to tear it down, we can use these commands:

    ./create-shard-groups stop

This will stop all Redis processes (which can be re-started of course). To
remove all data files, use:

    ./create-shard-groups clean

Additional information on support commands is available by running:

    ./create-shard-groups -h
