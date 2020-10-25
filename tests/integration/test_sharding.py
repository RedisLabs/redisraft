"""
This file is part of RedisRaft.

Copyright (c) 2020 Redis Labs

RedisRaft is dual licensed under the GNU Affero General Public License version 3
(AGPLv3) or the Redis Source Available License (RSAL).
"""

from redis import ResponseError
from pytest import raises


def test_cross_slot_violation(cluster):
    cluster.create(3, raft_args={
        'cluster-mode': 'yes',
        'raftize-all-commands': 'yes'})

    c = cluster.node(1).client

    # -CROSSSLOT on multi-key cross slot violation
    with raises(ResponseError, match='CROSSSLOT'):
        c.mset({'key1': 'val1', 'key2': 'val2'})

    # With tags it should succeed
    assert c.mset({'{tag1}key1': 'val1', '{tag1}key2': 'val2'})

    # MULTI/EXEC with cross slot between commands
    txn = cluster.node(1).client.pipeline(transaction=True)
    txn.set('key1', 'val1')
    txn.set('key2', 'val2')
    with raises(ResponseError, match='CROSSSLOT'):
        txn.execute()


def test_shard_group_sanity(cluster):
    # Create a cluster with just a single slot
    cluster.create(3, raft_args={
        'cluster-mode': 'yes',
        'raftize-all-commands': 'yes',
        'cluster-start-hslot': '0',
        'cluster-end-hslot': '0'})

    c = cluster.node(1).client

    # Operations on unmapped slots should fail
    with raises(ResponseError, match='CLUSTERDOWN'):
        c.set('key', 'value')

    # Add a fake shardgroup to get complete coverage
    assert c.execute_command(
        'RAFT.SHARDGROUP', 'ADD',
        '1', '16383',
        '1234567890123456789012345678901234567890',
        '1.1.1.1:1111') == b'OK'
    with raises(ResponseError, match='MOVED [0-9]+ 1.1.1.1:111'):
        c.set('key', 'value')


def test_shard_group_validation(cluster):
    cluster.create(3, raft_args={
        'cluster-mode': 'yes',
        'raftize-all-commands': 'yes',
        'cluster-start-hslot': '0',
        'cluster-end-hslot': '1000'})

    c = cluster.node(1).client

    # Invalid range
    with raises(ResponseError, match='invalid'):
        c.execute_command(
            'RAFT.SHARDGROUP', 'ADD',
            '1001', '20000',
            '1234567890123456789012345678901234567890',
            '1.1.1.1:1111') == b'OK'

    # Conflict
    with raises(ResponseError, match='invalid'):
        c.execute_command(
            'RAFT.SHARDGROUP', 'ADD',
            '1000', '1001',
            '1234567890123456789012345678901234567890',
            '1.1.1.1:1111') == b'OK'


def test_shard_group_propagation(cluster):
    # Create a cluster with just a single slot
    cluster.create(3, raft_args={
        'cluster-mode': 'yes',
        'raftize-all-commands': 'yes',
        'cluster-start-hslot': '0',
        'cluster-end-hslot': '1000'})

    c = cluster.node(1).client
    assert c.execute_command(
        'RAFT.SHARDGROUP', 'ADD',
        '1001', '16383',
        '1234567890123456789012345678901234567890',
        '1.1.1.1:1111') == b'OK'

    cluster.wait_for_unanimity()
    cluster.node(3).wait_for_log_applied()

    cluster_slots = cluster.node(3).client.execute_command('CLUSTER', 'SLOTS')
    assert len(cluster_slots) == 2


def test_shard_group_snapshot_propagation(cluster):
    # Create a cluster with just a single slot
    cluster.create(1, raft_args={
        'cluster-mode': 'yes',
        'raftize-all-commands': 'yes',
        'cluster-start-hslot': '0',
        'cluster-end-hslot': '1000'})

    c = cluster.node(1).client
    assert c.execute_command(
        'RAFT.SHARDGROUP', 'ADD',
        '1001', '16383',
        '1234567890123456789012345678901234567890',
        '1.1.1.1:1111') == b'OK'

    assert c.execute_command('RAFT.DEBUG', 'COMPACT') == b'OK'

    # Add a new node. Since we don't have the shardgroup entries
    # in the log anymore, we'll rely on snapshot delivery.
    n2 = cluster.add_node(use_cluster_args=True)
    n2.wait_for_node_voting()

    assert (c.execute_command('CLUSTER', 'SLOTS') ==
            n2.client.execute_command('CLUSTER', 'SLOTS'))


def test_shard_group_persistence(cluster):
    cluster.create(1, raft_args={
        'cluster-mode': 'yes',
        'raftize-all-commands': 'yes',
        'cluster-start-hslot': '0',
        'cluster-end-hslot': '1000'})

    n1 = cluster.node(1)
    assert n1.client.execute_command(
        'RAFT.SHARDGROUP', 'ADD',
        '1001', '16383',
        '1234567890123456789012345678901234567890',
        '1.1.1.1:1111') == b'OK'

    cluster_slots = n1.client.execute_command('CLUSTER', 'SLOTS')

    # Make sure received cluster slots is sane
    assert len(cluster_slots) == 2
    assert cluster_slots[1][0] == 1001
    assert cluster_slots[1][1] == 16383

    # Restart and make sure cluster slots persisted
    n1.terminate()
    n1.start()
    n1.wait_for_node_voting()

    assert n1.client.execute_command('CLUSTER', 'SLOTS') == cluster_slots

    # Compact log, restart and make sure cluster slots persisted
    n1.client.execute_command('RAFT.DEBUG', 'COMPACT')

    n1.terminate()
    n1.start()
    n1.wait_for_node_voting()

    assert n1.client.execute_command('CLUSTER', 'SLOTS') == cluster_slots
