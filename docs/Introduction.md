Introduction
============

Redis offers several deployment options including:

* A sharded, highly available Redis Cluster
* A single master with one or more replicas, with or without Sentinel service
  for automated failovers.

These deployments do not offer a strong consistency guarantee, as they
generally trade it for better performance or availability.

There are certain controls that can be used to provide better consistency for
specific operations (e.g. `WAIT`), but this does not make the system strongly
consistent overall.

RedisRaft is a Redis module extension that introduces a new form of strongly
consistent deployment, based on the [Raft Consensus
Algorithm](https://raft.github.io/).

## What can it do?

Using RedisRaft, you can create a cluster of Redis nodes that serve as true
replicas of the same dataset.

The cluster will handle the process of electing a leader, which is responsible
for handling all reads and writes (Raft follows the principle of a strong
leader).

You can use Redis commands (with some exceptions) to perform reads and writes in
a strongly consistent manner: an acknowledged write will not be lost, and will
be available for all consequent readers (as long as they're quorum reads, more
on that later).

The cluster supports configuration changes: it is possible to add or remove
nodes from the cluster. Configuration changes are also handled in a way that
does not violate consensus.

The cluster uses heartbeat messages to detect failed nodes, and in the event of
a failed leader will elect a new one.

RedisRaft clusters are available as long as the majority of the configured nodes
is up and able to communicate with each other. If that is not the case, the
cluster becomes unavailable until.

RedisRaft nodes may persist data to disk for better durability, or operate
in-memory only. If an in-memory node goes down (i.e. process stopped or crashed,
host OS restarted) it can no longer join the cluster and should be removed and
replaced by a new node; this is the equivalent of a disk-based node losing its
files.

## What CAN'T it do?

RedisRaft does not aim to be a drop-in replacement for Redis or Redis Cluster
deployments.

Not all Redis commands and capabilities are supported, or even possible in this
mode of operation.

The typical performance profile of RedisRaft is significantly different (and
lower) than Redis:

* Consensus mandates network round-robin trips between nodes, so latency is
  significantly higher.
* When persisting to disk, additional latency can be expected (same as using a
  Redis Append-Only File with an `always` policy).


