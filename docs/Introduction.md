Introduction
============

RedisRaft is a Redis module that allows you to create strongly-consistent clustered Redis deployments. This module provides strong consistency for Redis using the [Raft Consensus
Algorithm](https://raft.github.io/).

## Background

Redis has traditionally offered a couple of distributed deployment options:

1. A single master instance with one or more read-only replicas (optionally employing the Redis Sentinel service for better automated failover)
2. A highly-available, clustered deployment using hash-based partitioning, known as Redis Cluster

These deployments do not offer a strong consistency guarantee, since they trade it for better performance and availability. To be sure, there are certain controls, such as the `WAIT` command, that provide better consistency assurances, but these controls aren't designed to elevate traditional Redis deployments to the level of strong consistency.

Strong consistency implies that acknowledged writes will be immediately visible to all readers and that writes will not be lost once acknowledged.

Traditional Redis deployments trade strong consistency guarantees for improved performance and greater availability. RedisRaft was created for those occasions where strong consistency is required.

## What can RedisRaft do?

RedisRaft allows you to create a strongly-consistent cluster of Redis instances.

The cluster elects a leader, which handles all reads and writes (since Raft follows the principle of a strong leader).

You can then issue Redis commands to perform reads and writes in a strongly-consistent manner. Acknowledged writes will not be lost so long as a majority of nodes in the RedisRaft cluster remain online. All reads will be consistent with the latest writes as long as those reads are quorum reads (see the discussion of quorum reads in [Using RedisRaft](Using.md)).

A RedisRaft cluster also supports configuration changes, such as adding and removing nodes. Just like all writes to the cluster, these configuration changes are propagated in a strongly-consistent manner (i.e., they will never violate consensus).

The cluster uses heartbeat messages to detect failed nodes. In the event of a failed leader, the cluster will elect a new one.

A RedisRaft cluster will remain available (i.e., able to process reads and writes) as long as the majority of its nodes remains online and able to communicate with each other. If this is not the case, the cluster becomes unavailable.

The RedisRaft consensus algorithm enforces the following invariants:

  1. A node must never lose the data it has acknowledged (i.e., written in the log)
  2. A cluster may lose up to (N/2)-1 nodes while continuing to remain available. A cluster becomes unavailable when it loses a majority (>= 50%) of its nodes.

So long as a node persists its log data to disk, it will survive crashes, OS restarts, etc. This is true as long as the files are still present and can be loaded when the node goes back online.

However, if a node's log files are lost or corrupted, then that node needs to be removed from the cluster and re-joined.

Every cluster node is identified by a unique, logical ID. So re-joining a node involves logically removing the old ID and re-joining the node with a new ID.

## What are RedisRaft's limitations?

RedisRaft does not aim to be a drop-in replacement for Redis or Redis Cluster deployments. There are a couple of reasons for this.

First, RedisRaft does not support every Redis command and capability.

Second, to achieve strong consistency, RedisRaft sacrifices some of the performance you get in traditional Redis and Redis Cluster deployments. This is because:

1. The RedisRaft consensus algorithm requires several network round trips between nodes, which increases response times.
2.  RedisRaft records changes to disk immediately using its own persistence mechanism. This mechanism's impact on performance is comparable to the Redis AOF `always` persistence strategy.
