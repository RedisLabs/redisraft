"""
This file is part of RedisRaft.

Copyright (c) 2020 Redis Labs

RedisRaft is dual licensed under the GNU Affero General Public License version 3
(AGPLv3) or the Redis Source Available License (RSAL).
"""

import time
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


def test_shard_group_linking(cluster_factory):
    cluster1 = cluster_factory().create(3, raft_args={
        'cluster-mode': 'yes',
        'raftize-all-commands': 'yes',
        'cluster-start-hslot': '0',
        'cluster-end-hslot': '1',
        'shardgroup-update-interval': 500})
    cluster2 = cluster_factory().create(3, raft_args={
        'cluster-mode': 'yes',
        'raftize-all-commands': 'yes',
        'cluster-start-hslot': '2',
        'cluster-end-hslot': '16383',
        'shardgroup-update-interval': 500})

    # Not expected to have coverage
    with raises(ResponseError, match='CLUSTERDOWN'):
        cluster1.node(1).client.set('key', 'value')

    # Link cluster1 -> cluster2
    assert cluster1.node(1).client.execute_command(
        'RAFT.SHARDGROUP', 'LINK',
        'localhost:%s' % cluster2.node(1).port) == b'OK'
    with raises(ResponseError, match='MOVED'):
        cluster1.node(1).client.set('key', 'value')

    # Test cluster2 -> cluster1 linking with redirect, i.e. provide a
    # non-leader address
    assert cluster2.node(1).client.execute_command(
        'RAFT.SHARDGROUP', 'LINK',
        'localhost:%s' % cluster1.node(2).port) == b'OK'

    # Verify CLUSTER SLOTS look good
    cs = cluster1.node(1).client.execute_command('CLUSTER', 'SLOTS')
    assert len(cs) == 2
    assert cs[1][0] == 2
    assert cs[1][1] == 16383
    assert cs[1][2][1] == cluster2.leader_node().port  # first node is leader

    # Terminate leader on cluster 2, wait for re-election and confirm
    # propagation.
    assert cluster2.leader == 1
    cluster2.leader_node().terminate()
    cluster2.node(2).wait_for_election()

    # Wait for shardgroup update interval, 500ms
    time.sleep(1)
    cs = cluster1.node(1).client.execute_command('CLUSTER', 'SLOTS')
    assert len(cs) == 2
    assert cs[1][0] == 2
    assert cs[1][1] == 16383
    assert cs[1][2][1] == cluster2.leader_node().port  # first node is leader


def test_shard_group_linking_checks(cluster_factory):
    # Create clusters with overlapping hash slots,
    # linking should fail.
    cluster1 = cluster_factory().create(3, raft_args={
        'cluster-mode': 'yes',
        'raftize-all-commands': 'yes',
        'cluster-start-hslot': '0',
        'cluster-end-hslot': '1'})
    cluster2 = cluster_factory().create(3, raft_args={
        'cluster-mode': 'yes',
        'raftize-all-commands': 'yes',
        'cluster-start-hslot': '1',
        'cluster-end-hslot': '16383'})

    # Link
    with raises(ResponseError, match='failed to link'):
        cluster1.node(1).client.execute_command(
            'RAFT.SHARDGROUP', 'LINK',
            'localhost:%s' % cluster2.node(1).port)
