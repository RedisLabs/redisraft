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
        '1', '16382', '1',
        '1234567890123456789012345678901234567890', '1.1.1.1:1111') == b'OK'
    with raises(ResponseError, match='MOVED [0-9]+ 1.1.1.1:111'):
        c.set('key', 'value')

    cluster_slots = cluster.node(3).client.execute_command('CLUSTER', 'SLOTS')

    # Test by adding another fake shardgroup with same shardgroup id, should fail
    with raises(ResponseError, match='Invalid ShardGroup Update'):
        c.execute_command(
            'RAFT.SHARDGROUP', 'ADD',
            '12345678901234567890123456789012',
            '1', '1',
            '16383', '16383', '1',
            '1234567890123456789012345678901234567890', '   1.1.1.1:1111')


def test_shard_group_replace(cluster):
    # Create a cluster with just a single slot
    cluster.create(3, raft_args={
        'sharding': 'yes',
        'slot-config': '0',
        'external-sharding': 'yes'})

    c = cluster.node(1).client

    # tests wholesale replacement, while ignoring shardgroup that corresponds to local sg
    assert c.execute_command(
        'RAFT.SHARDGROUP', 'REPLACE',
        '3',
        '12345678901234567890123456789012',
        '1', '1',
        '6', '7', '1',
        '1234567890123456789012345678901234567890', '2.2.2.2:2222',
        '12345678901234567890123456789013',
        '1', '1',
        '8', '16383', '1',
        '1234567890123456789012345678901334567890', '3.3.3.3:3333',
        cluster.leader_node().info()['raft_dbid'],
        '1', '1',
        '0', '5', '1',
        '1234567890123456789012345678901234567890', '2.2.2.2:2222',
    ) == b'OK'
    with raises(ResponseError, match='MOVED [0-9]+ 3.3.3.3:3333'):
        c.set('key', 'value')
    cluster.wait_for_unanimity()
    cluster.node(2).wait_for_log_applied()  # to get shardgroup

    def validate_slots(cluster_slots):
        assert len(cluster_slots) == 3
        for i in range(len(cluster_slots)):
            if cluster_slots[i][2][2] == "{}00000001".format(
                    cluster.leader_node().info()['raft_dbid']).encode():
                assert cluster_slots[i][0] == 0, cluster_slots
                assert cluster_slots[i][1] == 5, cluster_slots
            elif cluster_slots[i][2][2] == b"1234567890123456789012345678901234567890":
                assert cluster_slots[i][0] == 6, cluster_slots
                assert cluster_slots[i][1] == 7, cluster_slots
            elif cluster_slots[i][2][2] == b"1234567890123456789012345678901334567890":
                assert cluster_slots[i][0] == 8, cluster_slots
                assert cluster_slots[i][1] == 16383, cluster_slots
            else:
                assert False, "failed to match id {}".format(cluster_slots[i][2][2])

    validate_slots(cluster.node(3).client.execute_command('CLUSTER', 'SLOTS'))


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

    # Make sure received cluster slots is sane
    def validate_slots(cluster_slots):
        assert len(cluster_slots) == 2
        local_id = "{}00000001".format(
            cluster.leader_node().info()['raft_dbid']).encode()
        for i in range(len(cluster_slots)):
            if cluster_slots[i][2][2] == local_id:
                assert cluster_slots[i][0] == 0, cluster_slots
                assert cluster_slots[i][1] == 1000, cluster_slots
            elif cluster_slots[i][2][2] == b"1234567890123456789012345678901234567890":
                assert cluster_slots[i][0] == 1001, cluster_slots
                assert cluster_slots[i][1] == 16383, cluster_slots
            else:
                assert False, "failed to match id {}".format(cluster_slots[i][2][2])

    validate_slots(n1.client.execute_command('CLUSTER', 'SLOTS'))

    # Restart and make sure cluster slots persisted
    n1.terminate()
    n1.start()
    n1.wait_for_node_voting()

    validate_slots(n1.client.execute_command('CLUSTER', 'SLOTS'))

    # Compact log, restart and make sure cluster slots persisted
    n1.client.execute_command('RAFT.DEBUG', 'COMPACT')

    n1.terminate()
    n1.start()
    n1.wait_for_node_voting()

    validate_slots(n1.client.execute_command('CLUSTER', 'SLOTS'))


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
    def validate_slots(cluster_slots):
        assert len(cluster_slots) == 2
        for i in range(len(cluster_slots)):
            if cluster_slots[i][2][1] == cluster1.leader_node().port:
                assert cluster_slots[i][0] == 0, cluster_slots
                assert cluster_slots[i][1] == 1, cluster_slots
            elif cluster_slots[i][2][1] == cluster2.leader_node().port:
                assert cluster_slots[i][0] == 2, cluster_slots
                assert cluster_slots[i][1] == 16383, cluster_slots
            else:
                assert False, "failed to match id {}".format(cluster_slots[i][2][1])

    validate_slots(cluster1.node(1).client.execute_command('CLUSTER', 'SLOTS'))

    # Verify CLUSTER NODES looks good
    def validate_nodes(cluster_nodes):
        assert len(cluster_nodes) == 7  # blank entry at the end
        for i in [0, 1, 3, 4]:
            node = cluster_nodes[i].split(b' ')
            if node[0] == "{}00000001".format(
                    cluster1.leader_node().info()['raft_dbid']).encode():
                assert node[2] == b"myself"
                assert node[3] == b"master"
                assert node[8] == b"0-1"
            elif node[0] == "{}00000002".format(
                    cluster1.leader_node().info()['raft_dbid']).encode():
                assert node[2] == b"noflags"
                assert node[3] == b"slave"
                assert node[8] == b"0-1"
            elif node[0] == "{}00000001".format(
                    cluster2.leader_node().info()['raft_dbid']).encode():
                assert node[2] == b"noflags"
                assert node[3] == b"master"
                assert node[8] == b"2-16383"
            elif node[0] == "{}00000002".format(
                    cluster2.leader_node().info()['raft_dbid']).encode():
                assert node[2] == b"noflags"
                assert node[3] == b"slave"
                assert node[8] == b"2-16383"
            else:
                assert False, node

        assert cluster_nodes[6] == b"";  # empty entry after final "\r\n" from split

    validate_nodes(cluster1.node(1).client.execute_command('CLUSTER', 'NODES').split(b"\r\n"))

    # Terminate leader on cluster 2, wait for re-election and confirm
    # propagation.
    assert cluster2.leader == 1
    cluster2.leader_node().terminate()
    cluster2.update_leader()

    # Wait for shardgroup update interval, 500ms
    time.sleep(2)
    validate_slots(cluster1.node(1).client.execute_command('CLUSTER', 'SLOTS'))


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
    with raises(ResponseError, match='failed to connect to cluster for link'):
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
    def validate_slots(slots):
        assert len(slots) == 2
        for i in range(2):
            if slots[i][2][1] < 5100:
                assert slots[i][0:2] == [0, 8191], slots
                assert slots[i][2][0] == b'localhost', slots
                assert slots[i][2][1] == 5001, slots

    validate_slots(cluster2.node(1).client.execute_command('CLUSTER', 'SLOTS'))

    # Terminate cluster1/node1 and wait for election
    cluster1.node(1).terminate()
    time.sleep(2)
    cluster1.node(2).wait_for_election()

    # Make sure the new leader is propagated to cluster 2
    def check_slots():
        slots = cluster2.node(1).client.execute_command('CLUSTER', 'SLOTS')
        assert len(slots) == 2
        for i in range(2):
            if slots[i][2][1] < 5100:
                assert slots[i][0:2] == [0, 8191], slots
                assert slots[i][2][0] == b'localhost', slots
                assert slots[i][2][1] != 5001, slots

    assert_after(check_slots, 10)



def test_shard_group_no_slots(cluster):
    cluster.create(3, raft_args={
        'sharding': 'yes',
        'slot-config': ''
    })

    c = cluster.node(1).client
    results = c.execute_command('CLUSTER', 'NODES').split(b"\r\n")

    splits = results[0].split(b" ")
    assert len(splits) == 9
    assert splits[8] == b""


def test_shard_group_reshard_to_migrate(cluster):
    cluster.create(3, raft_args={
        'sharding': 'yes',
        'external-sharding': 'yes'
    })

    cluster.leader_node().client.set("key", "value");

    assert cluster.leader_node().client.execute_command(
        'RAFT.SHARDGROUP', 'REPLACE',
        '2',
        '12345678901234567890123456789013',
        '1', '1',
        '0', '16383', '2',
        '1234567890123456789012345678901334567890', '3.3.3.3:3333',
        cluster.leader_node().info()["raft_dbid"],
        '1', '1',
        '0', '16383', '3',
        '1234567890123456789012345678901234567890', '2.2.2.2:2222',
    ) == b'OK'

    assert cluster.leader_node().client.get("key") == b'value'

    with raises(ResponseError, match="ASK 9189 3.3.3.3:3333"):
        cluster.leader_node().client.set("key1", "value1")

    conn = cluster.leader_node().client.connection_pool.get_connection('deferred')
    conn.send_command('MULTI')
    assert conn.read_response() == b'OK'
    conn.send_command('set', 'key', 'newvalue')
    assert conn.read_response() == b'QUEUED'
    conn.send_command('set', '{key}key1', 'newvalue')
    assert conn.read_response() == b'QUEUED'
    conn.send_command('EXEC')
    with raises(ResponseError, match="TRYAGAIN"):
        conn.read_response()

    assert cluster.leader_node().client.execute_command("del", "key") == 1

    with raises(ResponseError, match="ASK 12539 3.3.3.3:3333"):
        cluster.leader_node().client.get("key")


def test_shard_group_reshard_to_import(cluster):
    cluster.create(3, raft_args={
        'sharding': 'yes',
        'external-sharding': 'yes'
    })

    cluster.leader_node().client.set("key", "value");

    assert cluster.leader_node().client.execute_command(
        'RAFT.SHARDGROUP', 'REPLACE',
        '2',
        '12345678901234567890123456789013',
        '1', '1',
        '0', '16383', '3',
        '1234567890123456789012345678901334567890', '3.3.3.3:3333',
        cluster.leader_node().info()["raft_dbid"],
        '1', '1',
        '0', '16383', '2',
        '1234567890123456789012345678901234567890', '2.2.2.2:2222',
    ) == b'OK'

    with raises(ResponseError, match="MOVED 12539 3.3.3.3:3333"):
        cluster.leader_node().client.get("key")

    assert cluster.leader_node().client.execute_command("asking", "get", "key") == b'value'

    with raises(ResponseError, match="TRYAGAIN"):
        cluster.leader_node().client.execute_command("asking", "get", "key1")

    assert cluster.leader_node().client.execute_command("asking", "del", "key") == 1

    with raises(ResponseError, match="TRYAGAIN"):
        cluster.leader_node().client.execute_command("asking", "get", "key1")
