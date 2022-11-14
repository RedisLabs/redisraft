RedisRaft External Sharding Support
===================================

## Introduction

Initially, RedisRaft's [sharding mechanism](Sharding.md) used inter-cluster communication to learn about node configuration changes that occurred on its peer clusters containing other shards.
While, much of the terminology hasn't changed, this mechanism for communicating between RedisRaft clusters was only effective at transmitting node changes.
If one wanted to reconfigure which RedisRaft Clusters own which shards, one wasn't able to do it.

In practice, we are moving to a mechanism that allows an external orchestrator to setup individual distinct RedisRaft clusters and dynamically configure them with which redis slots should be owned by them. 
In addition, this configuration process would also inform the cluster of the other RedisRaft clusters that provide other slot ranges, and the nodes that belong to them.
In this mechanism, no communication would happen between RedisRaft clusters during normal stable operations

## Usage

Setting up a RedisRaft cluster for use with external sharding follows the same pattern as setting up a standalone RedisRaft Cluster.
The only addition module argument is `external-sharding yes`.

In order to reconfigure the global cluster configuration, one would issue the same RAFT.SHARDGROUP REPLACE command to every cluster.

The basic format of RAFT.SHARDGROUP REPLACE is

```
RAFT.SHARDGROUP REPLACE \
<N number of RedisRaft Clusters> \
<CLUSTER CONFIGURATION 1> \
...
<CLUSTER CONFIGURATION N>
```

where CLUSTER_CONFIGURATION is defined in the same manner as RAFT.SHARDGROUP ADD

```
<CLUSTER_ID> \
<A num slot ranges> <B num cluster nodes> \
<SLOT RANGE CONFIGURATION 1> \
...
<SLOT RANGE CONFIGURATION A> \
<NODE CONFIGURATION 1> \
...
<NODE CONFIGURATION B>
```

SLOT_RANGE_CONFIGURATION defined as

```
<START SLOT #> <END SLOT #> <SLOT TYPE>
```

START_SLOT and END_SLOT can be 0 to 16383 and are defined inclusively.  While SLOT_TYPE can be 1, 2, or 3 which correspond to RedisCluster's STABLE, Migrating, and Importing definitions respectively.

Currently, RedisRaft requires that the SLOT_TYPE just be 1 (STABLE).  In the future, when slot migration is supported between clusters, administrators will be able to set them to migrating and importing types.

NODE_CONFIGURATION is defined as

```
<Node ID> <Node Address:Port>
```

RedisRaft will validate the full configuration passed to a RAFT.SHARDGROUP REPLACE command to ensure that its internally consistent.
Namely, that every defined slot is in a consistent configuration.  Only one cluster can be defined with a slot if its stable, and while two clusters can be defined to be importing and migrating, respectively, there can be only one cluster of each.