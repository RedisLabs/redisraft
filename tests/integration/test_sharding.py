"""
This file is part of RedisRaft.

Copyright (c) 2020-2021 Redis Ltd.

RedisRaft is licensed under the Redis Source Available License (RSAL).
"""

import time
from redis import ResponseError
from pytest import raises
from .sandbox import assert_after

def test_cross_slot_violation(cluster):
    cluster.create(3, raft_args={'sharding': 'yes'})
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
        'sharding': 'yes',
        'slot-config': '0'})

    c = cluster.node(1).client

    # Operations on unmapped slots should fail
    with raises(ResponseError, match='CLUSTERDOWN'):
        c.set('key', 'value')

    # Add a fake shardgroup to get complete coverage
    assert c.execute_command(
        'RAFT.SHARDGROUP', 'ADD',
        '12345678901234567890123456789012',
        '1', '1',
        '1', '16383', '1',
        '1234567890123456789012345678901234567890', '1.1.1.1:1111') == b'OK'
    with raises(ResponseError, match='MOVED [0-9]+ 1.1.1.1:111'):
        c.set('key', 'value')

    # Follower redirect straight to remote shardgroup to
    # avoid another redirect hop.
    cluster.wait_for_unanimity()
    cluster.node(2).wait_for_log_applied()  # to get shardgroup
    with raises(ResponseError, match='MOVED [0-9]+ 1.1.1.1:111'):
        cluster.node(2).client.set('key', 'value')

    cluster_slots = cluster.node(3).client.execute_command('CLUSTER', 'SLOTS')

#    assert c.execute_command(
#        'RAFT.SHARDGROUP', 'UPDATE',
#        '12345678901234567890123456789012',
#        '1', '1',
#        '1', '16383', '1',
#        '1234567890123456789012345678901234567890', '2.2.2.2:2222') == b'OK'
#    with raises(ResponseError, match='MOVED [0-9]+ 2.2.2.2:2222'):
#        c.set('key', 'value')

    # tests whole sale replacement, while ignoring shardgroup that corresponds to local sg
    assert c.execute_command(
        'RAFT.SHARDGROUP', 'REPLACE',
        '3',
        '12345678901234567890123456789012',
        '1', '1',
        '1', '1', '1',
        '1234567890123456789012345678901234567890', '2.2.2.2:2222',
        '12345678901234567890123456789013',
        '1', '1',
        '2', '16383', '1',
        '1234567890123456789012345678901334567890', '3.3.3.3:3333',
        # this should be ignored, testing below
        cluster.leader_node().raft_info()['dbid'],
        '1', '1',
        '5', '7', '1',
        '1234567890123456789012345678901234567890', '2.2.2.2:2222',
    ) == b'OK'
    with raises(ResponseError, match='MOVED [0-9]+ 3.3.3.3:3333'):
        c.set('key', 'value')
    cluster.wait_for_unanimity()
    cluster.node(2).wait_for_log_applied()  # to get shardgroup

    cluster_slots = cluster.node(3).client.execute_command('CLUSTER', 'SLOTS')
    assert len(cluster_slots) == 3
    assert cluster_slots[0][0] == 0
    assert cluster_slots[0][1] == 0
    assert cluster_slots[1][0] == 1
    assert cluster_slots[1][1] == 1
    assert cluster_slots[2][0] == 2
    assert cluster_slots[2][1] == 16383


def test_shard_group_validation(cluster):
    cluster.create(3, raft_args={
        'sharding': 'yes',
        'slot-config': '0:1000'})

    c = cluster.node(1).client

    # Invalid range
    with raises(ResponseError, match='invalid'):
        c.execute_command(
            'RAFT.SHARDGROUP', 'ADD',
            '12345678901234567890123456789012',
            '1', '1',
            '1001', '20000', '1',
            '1234567890123456789012345678901234567890', '1.1.1.1:1111') == b'OK'

    # Conflict
    with raises(ResponseError, match='invalid'):
        c.execute_command(
            'RAFT.SHARDGROUP', 'ADD',
            '12345678901234567890123456789012',
            '1', '1',
            '1000', '1001', '1',
            '1234567890123456789012345678901234567890', '1.1.1.1:1111') == b'OK'


def test_shard_group_propagation(cluster):
    # Create a cluster with just a single slot
    cluster.create(3, raft_args={
        'sharding': 'yes',
        'slot-config': '0:1000'})

    c = cluster.node(1).client
    assert c.execute_command(
        'RAFT.SHARDGROUP', 'ADD',
        '100',
        '1', '1',
        '1001', '16383', '1',
        '1234567890123456789012345678901234567890', '1.1.1.1:1111') == b'OK'

    cluster.wait_for_unanimity()
    cluster.node(3).wait_for_log_applied()

    cluster_slots = cluster.node(3).client.execute_command('CLUSTER', 'SLOTS')
    assert len(cluster_slots) == 2

def test_shard_group_snapshot_propagation(cluster):
    # Create a cluster with just a single slot
    cluster.create(1, raft_args={
        'sharding': 'yes',
        'slot-config': '0:1000'})

    c = cluster.node(1).client
    assert c.execute_command(
        'RAFT.SHARDGROUP', 'ADD',
        '12345678901234567890123456789012',
        '1', '1',
        '1001', '16383', '1',
        '1234567890123456789012345678901234567890', '1.1.1.1:1111') == b'OK'

    assert c.execute_command('RAFT.DEBUG', 'COMPACT') == b'OK'

    # Add a new node. Since we don't have the shardgroup entries
    # in the log anymore, we'll rely on snapshot delivery.
    n2 = cluster.add_node(use_cluster_args=True)
    n2.wait_for_node_voting()

    assert (c.execute_command('CLUSTER', 'SLOTS') ==
            n2.client.execute_command('CLUSTER', 'SLOTS'))


def test_shard_group_persistence(cluster):
    cluster.create(1, raft_args={
        'sharding': 'yes',
        'slot-config': '0:1000',
        'external-sharding': 'yes',
    })

    n1 = cluster.node(1)
    assert n1.client.execute_command(
        'RAFT.SHARDGROUP', 'ADD',
        '12345678901234567890123456789012',
        '1', '1',
        '1001', '16383', '1',
        '1234567890123456789012345678901234567890', '1.1.1.1:1111') == b'OK'

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
        'sharding': 'yes',
        'slot-config': '0:1',
        'shardgroup-update-interval': 500})
    cluster2 = cluster_factory().create(3, raft_args={
        'sharding': 'yes',
        'slot-config': '2:16383',
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

    # Verify CLUSTER NODES looks good
    cluster_nodes_raw = cluster1.node(1).client.execute_command('CLUSTER', 'NODES')
    cluster_nodes = cluster_nodes_raw.split(b"\r\n")
    assert len(cluster_nodes) == 7  # blank entry at the end
    node0 = cluster_nodes[0].split(b' ')
    assert node0[2] == b"myself"
    assert node0[3] == b"master"
    assert node0[8] == b"0-1"
    node1 = cluster_nodes[1].split(b' ')
    assert node1[2] == b"noflags"
    assert node1[3] == b"slave"
    assert node1[8] == b"0-1"
    node3 = cluster_nodes[3].split(b' ')
    assert node3[2] == b"noflags"
    assert node3[3] == b"master"
    assert node3[8] == b"2-16383"
    node4 = cluster_nodes[4].split(b' ')
    assert node4[2] == b"noflags"
    assert node4[3] == b"slave"
    assert node4[8] == b"2-16383"
    assert cluster_nodes[6] == b"";  # empty entry after final "\r\n" from split

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
        'sharding': 'yes',
        'slot-config': '0:1'})
    cluster2 = cluster_factory().create(3, raft_args={
        'sharding': 'yes',
        'slot-config': '1:16383'})

    # Link
    with raises(ResponseError, match='failed to link'):
        cluster1.node(1).client.execute_command(
            'RAFT.SHARDGROUP', 'LINK',
            'localhost:%s' % cluster2.node(1).port)


def test_shard_group_refresh(cluster_factory):
    # Confirm that topology changes are eventually propagated through the
    # shardgroup refresh mechanism.

    cluster1 = cluster_factory().create(3, raft_args={
        'sharding': 'yes',
        'slot-config': '0:8191'})
    cluster2 = cluster_factory().create(3, raft_args={
        'sharding': 'yes',
        'slot-config': '8192:16383'})

    assert cluster1.node(1).client.execute_command(
        'RAFT.SHARDGROUP', 'LINK',
        'localhost:%s' % cluster2.node(1).port) == b'OK'
    assert cluster2.node(1).client.execute_command(
        'RAFT.SHARDGROUP', 'LINK',
        'localhost:%s' % cluster1.node(1).port) == b'OK'

    # Sanity: confirm initial shardgroup was propagated
    slots = cluster2.node(1).client.execute_command('CLUSTER', 'SLOTS')
    assert slots[1][0:2] == [0, 8191]
    assert slots[1][2][0] == b'localhost'
    assert slots[1][2][1] == 5001

    # Terminate cluster1/node1 and wait for election
    cluster1.node(1).terminate()
    cluster1.node(2).wait_for_election()

    # Make sure the new leader is propagated to cluster 2
    def check_slots():
        slots = cluster2.node(1).client.execute_command('CLUSTER', 'SLOTS')
        assert slots[1][0:2] == [0, 8191]
        assert slots[1][2][0] == b'localhost'
        assert slots[1][2][1] != 5001

    assert_after(check_slots, 10)
