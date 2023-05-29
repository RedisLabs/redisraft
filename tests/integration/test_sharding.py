"""
Copyright Redis Ltd. 2020 - present
Licensed under your choice of the Redis Source Available License 2.0 (RSALv2)
or the Server Side Public License v1 (SSPLv1).
"""

import time

from redis import ResponseError
from pytest import raises
from .sandbox import assert_after, RawConnection, SlotRangeType


def test_invalid_shardgroup_replace(cluster):
    cluster.create(3, raft_args={'sharding': 'yes'})
    c = cluster.node(1).client

    msg = "wrong number of arguments for 'raft.shardgroup"

    # not enough entries 1 shard
    with raises(ResponseError, match=msg):
        c.execute_command(
            'RAFT.SHARDGROUP', 'REPLACE',
            '1',
            cluster.leader_node().info()['raft_dbid'],
            '1', '1',
            '0', '16383', SlotRangeType.STABLE,
            '1234567890123456789012345678901234567890', '2.2.2.2:2222',
        )

    # not enough entries 2 shard
    with raises(ResponseError, match=msg):
        c.execute_command(
            'RAFT.SHARDGROUP', 'REPLACE',
            '2',
            '12345678901234567890123456789012',
            '0', '1',
            '1234567890123456789012345678901234567890', '2.2.2.2:2222',
            cluster.leader_node().info()['raft_dbid'],
            '1', '1',
            '0', '16383', SlotRangeType.STABLE,
            '1234567890123456789012345678901234567890', '2.2.2.2:2222',
        )


def test_cross_slot_violation(cluster):
    cluster.create(3, raft_args={'sharding': 'yes'})
    c = cluster.node(1).client

    assert c.execute_command(
        'RAFT.SHARDGROUP', 'REPLACE',
        '2',
        '12345678901234567890123456789012',
        '0', '1',
        '1234567890123456789012345678901234567890', '2.2.2.2:2222',
        cluster.leader_node().info()['raft_dbid'],
        '1', '1',
        '0', '16383', SlotRangeType.STABLE, '0',
        '1234567890123456789012345678901234567890', '2.2.2.2:2222',
    ) == b'OK'

    # -CROSSSLOT on multi-key cross slot violation
    with raises(ResponseError, match='CROSSSLOT'):
        c.mset({'key1': 'val1', 'key2': 'val2'})

    # With tags, it should succeed
    assert c.mset({'{tag1}key1': 'val1', '{tag1}key2': 'val2'})

    # MULTI/EXEC with cross slot between commands
    txn = cluster.node(1).client.pipeline(transaction=True)
    txn.set('key1', 'val1')
    txn.set('key2', 'val2')
    with raises(ResponseError, match='CROSSSLOT'):
        txn.execute()

    # Wait followers just to be sure crossslot command does not cause a problem
    cluster.wait_for_unanimity()


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
        '1', '16382', SlotRangeType.STABLE, '0',
        '1234567890123456789012345678901234567890', '1.1.1.1:1111') == b'OK'
    with raises(ResponseError, match='MOVED [0-9]+ 1.1.1.1:111'):
        c.set('key', 'value')

    cluster.node(3).client.execute_command('CLUSTER', 'SLOTS')

    # Test by adding another fake shardgroup with same shardgroup id
    # should fail
    with raises(ResponseError, match='Invalid ShardGroup Update'):
        c.execute_command(
            'RAFT.SHARDGROUP', 'ADD',
            '12345678901234567890123456789012',
            '1', '1',
            '16383', '16383', SlotRangeType.STABLE, '0',
            '1234567890123456789012345678901234567890', '   1.1.1.1:1111')


def test_shard_group_replace(cluster):
    # Create a cluster with just a single slot
    cluster.create(3, raft_args={
        'sharding': 'yes',
        'slot-config': '0',
        'external-sharding': 'yes'})

    c = cluster.node(1).client

    cluster_dbid = cluster.leader_node().info()['raft_dbid']

    # Tests wholesale replacement, while ignoring shardgroup
    # that corresponds to local sg
    assert c.execute_command(
        'RAFT.SHARDGROUP', 'REPLACE',
        '3',
        '12345678901234567890123456789012',
        '1', '1',
        '6', '7', SlotRangeType.STABLE, '0',
        '2' * 40, '2.2.2.2:2222',

        '12345678901234567890123456789013',
        '1', '1',
        '8', '16383', SlotRangeType.STABLE, '0',
        '3' * 40, '3.3.3.3:3333',

        cluster_dbid,
        '1', '3',
        '0', '5', SlotRangeType.STABLE, '0',
        "{}00000001".format(cluster_dbid).encode(), cluster.node(1).address,
        "{}00000002".format(cluster_dbid).encode(), cluster.node(2).address,
        "{}00000003".format(cluster_dbid).encode(), cluster.node(3).address,
    ) == b'OK'

    with raises(ResponseError, match='MOVED [0-9]+ 3.3.3.3:3333'):
        c.set('key', 'value')

    cluster.wait_for_unanimity()
    cluster.node(3).wait_for_log_applied()  # to get shardgroup

    def validate_slots(cluster_slots):
        assert len(cluster_slots) == 3
        leader = cluster.leader_node()
        local_id = "{}00000001".format(leader.info()['raft_dbid']).encode()

        for i in range(len(cluster_slots)):
            node_id = cluster_slots[i][2][2]
            start_slot = cluster_slots[i][0]
            end_slot = cluster_slots[i][1]

            if node_id == local_id:
                assert start_slot == 0, cluster_slots
                assert end_slot == 5, cluster_slots
            elif node_id == b'2' * 40:
                assert start_slot == 6, cluster_slots
                assert end_slot == 7, cluster_slots
            elif node_id == b'3' * 40:
                assert start_slot == 8, cluster_slots
                assert end_slot == 16383, cluster_slots
            else:
                assert False, "failed to match id {}".format(node_id)

    validate_slots(cluster.node(3).client.execute_command('CLUSTER', 'SLOTS'))

    def validate_nodes(cluster_nodes):
        assert len(cluster_nodes) == 5
        for i in [0, 1, 3, 4]:
            node = cluster_nodes[i].split(b' ')
            if node[0] == "{}00000001".format(
                    cluster.leader_node().info()['raft_dbid']).encode():
                assert node[2] == b"myself,master"
                assert node[3] == b"-"
                assert node[8] == b"0-5"
            elif node[0] == "{}00000002".format(
                    cluster.leader_node().info()['raft_dbid']).encode():
                assert node[2] == b"slave"
                assert node[3] == "{}00000001".format(
                    cluster.leader_node().info()['raft_dbid']).encode()
                assert node[8] == b"0-5"
            elif node[0] == "{}00000003".format(
                    cluster.leader_node().info()['raft_dbid']).encode():
                assert node[2] == b"slave"
                assert node[3] == "{}00000001".format(
                    cluster.leader_node().info()['raft_dbid']).encode()
                assert node[8] == b"0-5"
            elif node[0] == b'2' * 40:
                assert node[2] == b"master"
                assert node[3] == b"-"
                assert node[8] == b"6-7"
            elif node[0] == b'3' * 40:
                assert node[2] == b"master"
                assert node[3] == b"-"
                assert node[8] == b"8-16383"
            else:
                assert False, node

    validate_nodes(cluster.node(1).execute('CLUSTER', 'NODES').splitlines())


def test_shard_group_validation(cluster):
    cluster.create(3, raft_args={
        'sharding': 'yes',
        'slot-config': '0:1000'})

    c = cluster.node(1).client

    # Invalid range
    with raises(ResponseError, match='failed to parse slot range'):
        c.execute_command(
            'RAFT.SHARDGROUP', 'ADD',
            '12345678901234567890123456789012',
            '1', '1',
            '1001', '20000', SlotRangeType.STABLE, '0',
            '1234567890123456789012345678901234567890', '1.1.1.1:1111')

    # Conflict
    with raises(ResponseError, match='invalid'):
        c.execute_command(
            'RAFT.SHARDGROUP', 'ADD',
            '12345678901234567890123456789012',
            '1', '1',
            '1000', '1001', SlotRangeType.STABLE, '0',
            '1234567890123456789012345678901234567890', '1.1.1.1:1111')


def test_shard_group_propagation(cluster):
    # Create a cluster with just a single slot
    cluster.create(3, raft_args={
        'sharding': 'yes',
        'slot-config': '0:1000'})

    c = cluster.node(1).client
    assert c.execute_command(
        'RAFT.SHARDGROUP', 'ADD',
        '1' * 32,
        '1', '1',
        '1001', '16383', SlotRangeType.STABLE, '0',
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
        '1001', '16383', SlotRangeType.STABLE, '0',
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
        '1001', '16383', SlotRangeType.STABLE, '0',
        '1234567890123456789012345678901234567890', '1.1.1.1:1111') == b'OK'

    # Make sure received cluster slots is sane
    def validate_slots(cluster_slots):
        assert len(cluster_slots) == 2
        local_id = "{}00000001".format(
            cluster.leader_node().info()['raft_dbid']).encode()
        for i in range(len(cluster_slots)):
            shargroup_id = cluster_slots[i][2][2]
            start_slot = cluster_slots[i][0]
            end_slot = cluster_slots[i][1]

            if shargroup_id == local_id:
                assert start_slot == 0, cluster_slots
                assert end_slot == 1000, cluster_slots
            elif shargroup_id == b"1234567890123456789012345678901234567890":
                assert start_slot == 1001, cluster_slots
                assert end_slot == 16383, cluster_slots
            else:
                assert False, "failed to match id {}".format(shargroup_id)

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
        cluster2.node(1).address) == b'OK'
    with raises(ResponseError, match='MOVED'):
        cluster1.node(1).client.set('key', 'value')

    # Test cluster2 -> cluster1 linking with redirect, i.e. provide a
    # non-leader address
    assert cluster2.node(1).client.execute_command(
        'RAFT.SHARDGROUP', 'LINK',
        cluster1.node(2).address,) == b'OK'

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
                assert False, "failed to match id {}".format(
                    cluster_slots[i][2][1])

    validate_slots(cluster1.node(1).client.execute_command('CLUSTER', 'SLOTS'))

    # Verify CLUSTER NODES looks good
    def validate_nodes(cluster_nodes):
        assert len(cluster_nodes) == 6
        for i in [0, 1, 3, 4]:
            node = cluster_nodes[i].split(b' ')
            if node[0] == "{}00000001".format(
                    cluster1.leader_node().info()['raft_dbid']).encode():
                assert node[2] == b"myself,master"
                assert node[3] == b"-"
                assert node[8] == b"0-1"
            elif node[0] == "{}00000002".format(
                    cluster1.leader_node().info()['raft_dbid']).encode():
                assert node[2] == b"slave"
                assert node[3] == "{}00000001".format(
                    cluster1.leader_node().info()['raft_dbid']).encode()
                assert node[8] == b"0-1"
            elif node[0] == "{}00000001".format(
                    cluster2.leader_node().info()['raft_dbid']).encode():
                assert node[2] == b"master"
                assert node[3] == b"-"
                assert node[8] == b"2-16383"
            elif node[0] == "{}00000002".format(
                    cluster2.leader_node().info()['raft_dbid']).encode():
                assert node[2] == b"slave"
                assert node[3] == b"-"
                assert node[8] == b"2-16383"
            else:
                assert False, node

    validate_nodes(cluster1.node(1).execute('CLUSTER', 'NODES').splitlines())

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
            cluster2.node(1).address)


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
        cluster2.node(1).address) == b'OK'
    assert cluster2.node(1).client.execute_command(
        'RAFT.SHARDGROUP', 'LINK',
        cluster1.node(1).address,) == b'OK'

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

    def validate_nodes(cluster_nodes):
        assert len(cluster_nodes) == 3

        for i in range(len(cluster_nodes)):
            node_data = cluster_nodes[i].split(b' ')
            assert len(node_data) == 9
            assert node_data[8] == b""

    validate_nodes(cluster.node(1).execute('CLUSTER', 'NODES').splitlines())


def test_shard_group_reshard_to_migrate(cluster):
    cluster.create(1, raft_args={
        'sharding': 'yes',
        'external-sharding': 'yes'
    })

    cluster_dbid = cluster.leader_node().info()["raft_dbid"]

    cluster.execute("set", "key", "value")

    assert cluster.execute(
        'RAFT.SHARDGROUP', 'REPLACE',
        '2',

        '12345678901234567890123456789013',
        '1', '1',
        '0', '16383', SlotRangeType.IMPORTING, '0',
        '3' * 40, '3.3.3.3:3333',

        cluster_dbid,
        '1', '1',
        '0', '16383', SlotRangeType.MIGRATING, '0',
        "{}00000001".format(cluster_dbid).encode(), cluster.node(1).address,
    ) == b'OK'

    assert cluster.execute("get", "key") == b'value'

    with raises(ResponseError, match="ASK 9189 3.3.3.3:3333"):
        cluster.execute("set", "key1", "value1")

    conn = RawConnection(cluster.leader_node().client)
    assert conn.execute('multi') == b'OK'
    assert conn.execute('set', 'key', 'newvalue') == b'QUEUED'
    assert conn.execute('set', '{key}key1', 'newvalue') == b'QUEUED'

    with raises(ResponseError, match="TRYAGAIN"):
        conn.execute('exec')

    assert cluster.execute("del", "key") == 1

    with raises(ResponseError, match="ASK 12539 3.3.3.3:3333"):
        cluster.execute("get", "key")


def test_shard_group_reshard_to_import(cluster):
    cluster.create(1, raft_args={
        'sharding': 'yes',
        'external-sharding': 'yes'
    })

    cluster_dbid = cluster.leader_node().info()["raft_dbid"]

    cluster.execute("set", "key", "value")

    assert cluster.execute(
        'RAFT.SHARDGROUP', 'REPLACE',
        '2',

        '12345678901234567890123456789013',
        '1', '1',
        '0', '16383', SlotRangeType.MIGRATING, '456',
        '1234567890123456789012345678901334567890', '3.3.3.3:3333',

        cluster_dbid,
        '1', '1',
        '0', '16383', SlotRangeType.IMPORTING, '456',
        "{}00000001".format(cluster_dbid).encode(), cluster.node(1).address,
    ) == b'OK'

    with raises(ResponseError, match="MOVED 12539 3.3.3.3:3333"):
        cluster.leader_node().client.get("key")

    conn = RawConnection(cluster.leader_node().client)
    assert conn.execute('asking') == b'OK'

    assert conn.execute('get', 'key') == b'value'
    assert conn.execute('asking') == b'OK'

    with raises(ResponseError, match="TRYAGAIN"):
        assert conn.execute('get', 'key1')

    assert conn.execute('asking') == b'OK'
    assert conn.execute("del", "key") == 1

    assert conn.execute('asking') == b'OK'
    with raises(ResponseError, match="TRYAGAIN"):
        conn.execute('get', 'key')


def test_asking_follower(cluster):
    cluster.create(3, raft_args={
        'sharding': 'yes',
        'external-sharding': 'yes'
    })

    cluster_dbid = cluster.leader_node().info()["raft_dbid"]

    cluster.execute("set", "key", "value")

    assert cluster.execute(
        'RAFT.SHARDGROUP', 'REPLACE',
        1,
        cluster.leader_node().info()["raft_dbid"],
        '1', '3',
        '0', '16383', SlotRangeType.IMPORTING, '123',
        "{}00000001".format(cluster_dbid).encode(), cluster.node(1).address,
        "{}00000002".format(cluster_dbid).encode(), cluster.node(2).address,
        "{}00000003".format(cluster_dbid).encode(), cluster.node(3).address,
    ) == b'OK'

    cluster.wait_for_unanimity()
    cluster.node(2).wait_for_log_applied()

    conn = cluster.node(2).client.connection_pool.get_connection('deferred')
    conn.send_command('ASKING')
    assert conn.read_response() == b'OK'

    conn.send_command('get', 'key')
    with raises(ResponseError, match="ASK"):
        conn.read_response()


def test_cluster_slots_for_empty_slot_sg(cluster):
    cluster.create(3, raft_args={
        'sharding': 'yes',
        'external-sharding': 'yes'})

    cluster_shardgroup_id = "1" * 32

    assert cluster.execute(
        'RAFT.SHARDGROUP', 'REPLACE',
        '1',

        cluster_shardgroup_id,
        '0', '1',
        '%s00000001' % cluster_shardgroup_id, '1.1.1.1:1111',
        ) == b'OK'

    def validate_slots(cluster_slots):
        assert len(cluster_slots) == 0

    validate_slots(cluster.node(1).client.execute_command('CLUSTER', 'SLOTS'))


def test_cluster_slots_for_single_slot_sg(cluster):
    cluster.create(1, raft_args={
        'sharding': 'yes',
        'external-sharding': 'yes'})

    stable_shardgroup_id = cluster.leader_node().info()["raft_dbid"]
    importing_shardgroup_id = "2" * 32
    migrating_shardgroup_id = "3" * 32

    assert cluster.execute(
        'RAFT.SHARDGROUP', 'REPLACE',
        '3',

        stable_shardgroup_id,
        '1', '1',
        '0', '0', SlotRangeType.STABLE, '123',
        '%s00000001' % stable_shardgroup_id, cluster.node(1).address,

        importing_shardgroup_id,
        '1', '1',
        '501', '501', SlotRangeType.IMPORTING, '123',
        '%s00000001' % importing_shardgroup_id, '2.2.2.2:2222',

        migrating_shardgroup_id,
        '1', '1',
        '501', '501', SlotRangeType.MIGRATING, '123',
        '%s00000001' % migrating_shardgroup_id, '3.3.3.3:3333',
        ) == b'OK'

    def validate_slots(cluster_slots):
        assert len(cluster_slots) == 2

        stable_node_id = "{}00000001".format(stable_shardgroup_id).encode()
        migrate_node_id = "{}00000001".format(migrating_shardgroup_id).encode()

        for i in range(len(cluster_slots)):
            node_id = cluster_slots[i][2][2]
            start_slot = cluster_slots[i][0]
            end_slot = cluster_slots[i][1]

            if node_id == stable_node_id:
                assert start_slot == 0, cluster_slots
                assert end_slot == 0, cluster_slots
            elif node_id == migrate_node_id:
                assert start_slot == 501, cluster_slots
                assert end_slot == 501, cluster_slots
            else:
                assert False, "failed to match id {}".format(node_id)

    validate_slots(cluster.node(1).client.execute_command('CLUSTER', 'SLOTS'))


def test_cluster_slots_for_single_slot_range_sg(cluster):
    cluster.create(1, raft_args={
        'sharding': 'yes',
        'external-sharding': 'yes'})

    stable_shardgroup_id = cluster.leader_node().info()["raft_dbid"]
    importing_shardgroup_id = "2" * 32
    migrating_shardgroup_id = "3" * 32

    assert cluster.execute(
        'RAFT.SHARDGROUP', 'REPLACE',
        '3',

        stable_shardgroup_id,
        '1', '1',
        '0', '500', SlotRangeType.STABLE, '123',
        '%s00000001' % stable_shardgroup_id, cluster.node(1).address,

        importing_shardgroup_id,
        '1', '1',
        '501', '16383', SlotRangeType.IMPORTING, '123',
        '%s00000001' % importing_shardgroup_id, '2.2.2.2:2222',

        migrating_shardgroup_id,
        '1', '1',
        '501', '16383', SlotRangeType.MIGRATING, '123',
        '%s00000001' % migrating_shardgroup_id, '3.3.3.3:3333',
        ) == b'OK'

    def validate_slots(cluster_slots):
        assert len(cluster_slots) == 2

        stable_node_id = "{}00000001".format(stable_shardgroup_id).encode()
        migrate_node_id = "{}00000001".format(migrating_shardgroup_id).encode()

        for i in range(len(cluster_slots)):
            node_id = cluster_slots[i][2][2]
            start_slot = cluster_slots[i][0]
            end_slot = cluster_slots[i][1]

            if node_id == stable_node_id:
                assert start_slot == 0, cluster_slots
                assert end_slot == 500, cluster_slots
            elif node_id == migrate_node_id:
                assert start_slot == 501, cluster_slots
                assert end_slot == 16383, cluster_slots
            else:
                assert False, "failed to match id {}".format(node_id)

    validate_slots(cluster.node(1).client.execute_command('CLUSTER', 'SLOTS'))


def test_cluster_slots_for_multiple_slots_range_sg(cluster):
    cluster.create(1, raft_args={
        'sharding': 'yes',
        'external-sharding': 'yes'})

    shardgroup_id_1 = cluster.leader_node().info()["raft_dbid"]
    shardgroup_id_2 = "2" * 32
    shardgroup_id_3 = "3" * 32

    assert cluster.execute(
        'RAFT.SHARDGROUP', 'REPLACE',
        '3',

        shardgroup_id_1,
        '3', '1',
        '0', '500', SlotRangeType.STABLE, '123',
        '600', '700', SlotRangeType.IMPORTING, '123',
        '800', '1000', SlotRangeType.MIGRATING, '123',
        '%s00000001' % shardgroup_id_1, cluster.node(1).address,

        shardgroup_id_2,
        '2', '1',
        '1001', '16383', SlotRangeType.IMPORTING, '123',
        '600', '700', SlotRangeType.MIGRATING, '123',
        '%s00000001' % shardgroup_id_2, '2.2.2.2:2222',

        shardgroup_id_3,
        '2', '1',
        '800', '1000', SlotRangeType.IMPORTING, '123',
        '1001', '16383', SlotRangeType.MIGRATING, '123',
        '%s00000001' % shardgroup_id_3, '3.3.3.3:3333',
        ) == b'OK'

    def validate_slots(cluster_slots):
        assert len(cluster_slots) == 4

        node_id_1 = "{}00000001".format(shardgroup_id_1).encode()
        node_id_2 = "{}00000001".format(shardgroup_id_2).encode()
        node_id_3 = "{}00000001".format(shardgroup_id_3).encode()

        for i in range(len(cluster_slots)):
            node_id = cluster_slots[i][2][2]
            start_slot = cluster_slots[i][0]
            end_slot = cluster_slots[i][1]

            if start_slot == 0 and end_slot == 500:
                assert node_id == node_id_1
            elif start_slot == 800 and end_slot == 1000:
                assert node_id == node_id_1
            elif start_slot == 600 and end_slot == 700:
                assert node_id == node_id_2
            elif start_slot == 1001 and end_slot == 16383:
                assert node_id == node_id_3
            else:
                assert False, "failed to match slot {}-{}"\
                    .format(start_slot, end_slot)

    validate_slots(cluster.node(1).client.execute_command('CLUSTER', 'SLOTS'))


def test_cluster_nodes_for_empty_slot_sg(cluster):
    cluster.create(3, raft_args={
        'sharding': 'yes',
        'external-sharding': 'yes'})

    cluster_shardgroup_id = "1" * 32

    assert cluster.execute(
        'RAFT.SHARDGROUP', 'REPLACE',
        '1',

        cluster_shardgroup_id,
        '0', '1',
        '%s00000001' % cluster_shardgroup_id, '1.1.1.1:1111',
        ) == b'OK'

    def validate_nodes(cluster_nodes):
        assert len(cluster_nodes) == 1
        cluster_dbid = cluster.leader_node().info()["raft_dbid"]
        node_id = "{}00000001".format(cluster_shardgroup_id).encode()
        local_node_id = "{}00000001".format(cluster_dbid).encode()

        node_data = cluster_nodes[0].split(b' ')

        if local_node_id == node_data[0]:
            assert node_data[2] == b"myself,master"
        else:
            assert node_data[2] == b"master"

        assert node_data[3] == b"-"

        assert node_data[0] == node_id
        assert node_data[8] == b""

    validate_nodes(cluster.node(1).execute('CLUSTER', 'NODES').splitlines())


def test_cluster_nodes_for_single_slot_range_sg(cluster):
    cluster.create(1, raft_args={
        'sharding': 'yes',
        'external-sharding': 'yes'})

    dbid = cluster.leader_node().info()["raft_dbid"]
    stable_shardgroup_id = "1" * 32
    importing_shardgroup_id = "2" * 32
    migrating_shardgroup_id = dbid

    assert cluster.execute(
        'RAFT.SHARDGROUP', 'REPLACE',
        '3',

        stable_shardgroup_id,
        '1', '1',
        '0', '500', SlotRangeType.STABLE, '123',
        '%s00000001' % stable_shardgroup_id, '1.1.1.1:1111',

        importing_shardgroup_id,
        '1', '1',
        '501', '16383', SlotRangeType.IMPORTING, '123',
        '%s00000001' % importing_shardgroup_id, '2.2.2.2:2222',

        migrating_shardgroup_id,
        '1', '1',
        '501', '16383', SlotRangeType.MIGRATING, '123',
        '%s00000001' % migrating_shardgroup_id, cluster.node(1).address,
        ) == b'OK'

    def validate_nodes(cluster_nodes):
        assert len(cluster_nodes) == 3
        stable_node_id = "{}00000001".format(stable_shardgroup_id).encode()
        migrate_node_id = "{}00000001".format(migrating_shardgroup_id).encode()
        import_node_id = "{}00000001".format(importing_shardgroup_id).encode()
        local_node_id = "{}00000001".format(dbid).encode()

        for i in range(len(cluster_nodes)):
            node_data = cluster_nodes[i].split(b' ')
            if local_node_id == node_data[0]:
                assert node_data[2] == b"myself,master"
            else:
                assert node_data[2] == b"master"

            assert node_data[3] == b"-"

            if node_data[0] == stable_node_id:
                assert node_data[8] == b"0-500"
            elif node_data[0] == migrate_node_id:
                assert node_data[8] == b"501-16383"
                assert len(node_data) == 15892
                for j in range(501, 16383):
                    node_str = f"[{j}->-{import_node_id.decode()}]".encode()
                    assert node_data[j-492] == node_str

            elif node_data[0] == import_node_id:
                assert node_data[8] == b""
            else:
                assert False, "failed to match id {}".format(node_data[0])

    validate_nodes(cluster.node(1).execute('CLUSTER', 'NODES').splitlines())


def test_cluster_nodes_for_single_slot_sg(cluster):
    cluster.create(1, raft_args={
        'sharding': 'yes',
        'external-sharding': 'yes'})

    dbid = cluster.leader_node().info()["raft_dbid"]
    stable_shardgroup_id = dbid
    importing_shardgroup_id = "2" * 32
    migrating_shardgroup_id = "3" * 32

    assert cluster.execute(
        'RAFT.SHARDGROUP', 'REPLACE',
        '3',

        stable_shardgroup_id,
        '1', '1',
        '0', '0', SlotRangeType.STABLE, '123',
        '%s00000001' % stable_shardgroup_id, cluster.node(1).address,

        importing_shardgroup_id,
        '1', '1',
        '501', '501', SlotRangeType.IMPORTING, '123',
        '%s00000001' % importing_shardgroup_id, '2.2.2.2:2222',

        migrating_shardgroup_id,
        '1', '1',
        '501', '501', SlotRangeType.MIGRATING, '123',
        '%s00000001' % migrating_shardgroup_id, '3.3.3.3:3333',
        ) == b'OK'

    def validate_nodes(cluster_nodes):
        assert len(cluster_nodes) == 3
        stable_node_id = "{}00000001".format(stable_shardgroup_id).encode()
        migrate_node_id = "{}00000001".format(migrating_shardgroup_id).encode()
        import_node_id = "{}00000001".format(importing_shardgroup_id).encode()
        local_node_id = "{}00000001".format(dbid).encode()

        for i in range(len(cluster_nodes)):
            node_data = cluster_nodes[i].split(b' ')

            if local_node_id == node_data[0]:
                assert node_data[2] == b"myself,master"
            else:
                assert node_data[2] == b"master"

            assert node_data[3] == b"-"

            if node_data[0] == stable_node_id:
                assert node_data[8] == b"0"
            elif node_data[0] == migrate_node_id:
                assert node_data[8] == b"501"
            elif node_data[0] == import_node_id:
                assert node_data[8] == b""
            else:
                assert False, "failed to match id {}".format(node_data[0])

    validate_nodes(cluster.node(1).execute('CLUSTER', 'NODES').splitlines())


def test_cluster_nodes_for_multiple_slots_range_sg(cluster):
    cluster.create(1, raft_args={
        'sharding': 'yes',
        'external-sharding': 'yes'})

    cluster_dbid = cluster.leader_node().info()["raft_dbid"]
    shardgroup_id_1 = cluster_dbid
    shardgroup_id_2 = "2" * 32
    shardgroup_id_3 = "3" * 32

    assert cluster.execute(
        'RAFT.SHARDGROUP', 'REPLACE',
        '3',

        shardgroup_id_1,
        '3', '1',
        '0', '500', SlotRangeType.STABLE, '123',
        '600', '700', SlotRangeType.IMPORTING, '123',
        '800', '1000', SlotRangeType.MIGRATING, '123',
        '%s00000001' % shardgroup_id_1, cluster.node(1).address,

        shardgroup_id_2,
        '2', '1',
        '1001', '16383', SlotRangeType.IMPORTING, '123',
        '600', '700', SlotRangeType.MIGRATING, '123',
        '%s00000001' % shardgroup_id_2, '2.2.2.2:2222',

        shardgroup_id_3,
        '2', '1',
        '800', '1000', SlotRangeType.IMPORTING, '123',
        '1001', '16383', SlotRangeType.MIGRATING, '123',
        '%s00000001' % shardgroup_id_3, '3.3.3.3:3333',
        ) == b'OK'

    def validate_nodes(cluster_nodes):
        # many "special" lines now
        assert len(cluster_nodes) == 3
        node_id_1 = "{}00000001".format(shardgroup_id_1).encode()
        node_id_2 = "{}00000001".format(shardgroup_id_2).encode()
        node_id_3 = "{}00000001".format(shardgroup_id_3).encode()
        local_node_id = "{}00000001".format(cluster_dbid).encode()

        for i in range(len(cluster_nodes)):
            node_data = cluster_nodes[i].split(b' ')
            slot_data = cluster_nodes[i].split(b'-')

            if len(node_data) > 1:
                if local_node_id == node_data[0]:
                    assert node_data[2] == b"myself,master"
                else:
                    assert node_data[2] == b"master"

                assert node_data[3] == b"-"

                if node_data[0] == node_id_1:
                    assert node_data[8] == b"0-500"
                    assert node_data[9] == b"800-1000"
                    for j in range(600, 700):
                        node_str = f"[{j}-<-{node_id_2.decode()}]".encode()
                        assert node_data[j-590] == node_str
                    for j in range(800, 1000):
                        node_str = f"[{j}->-{node_id_3.decode()}]".encode()
                        assert node_data[j-689] == node_str
                elif node_data[0] == node_id_2:
                    assert node_data[8] == b"600-700"
                elif node_data[0] == node_id_3:
                    assert node_data[8] == b"1001-16383"
                else:
                    assert False, "failed to match id {}".format(node_data[0])
            else:
                assert (chr(slot_data[0][0]) == '[')

                if int(slot_data[0][1:]) <= 700:
                    assert slot_data[1] == b"<"
                    assert slot_data[2] == f"{node_id_2.decode()}]".encode()
                elif int(slot_data[0][1:]) <= 1000:
                    assert slot_data[1] == b">"
                    assert slot_data[2] == f"{node_id_3.decode()}]".encode()
                else:
                    assert False, "failed to match id {}".format(slot_data[0])

    validate_nodes(cluster.node(1).execute('CLUSTER', 'NODES').splitlines())


def test_cluster_shards_for_empty_slot_sg(cluster, pytestconfig):
    cluster.create(3, raft_args={
        'sharding': 'yes',
        'external-sharding': 'yes'})

    cluster_shardgroup_id = "1" * 32

    assert cluster.execute(
        'RAFT.SHARDGROUP', 'REPLACE',
        '1',

        cluster_shardgroup_id,
        '0', '1',
        '%s00000001' % cluster_shardgroup_id, '1.1.1.1:1111',
        ) == b'OK'

    def validate_shards(cluster_shards):
        node_id_1 = "{}00000001".format(cluster_shardgroup_id).encode()

        assert len(cluster_shards) == 1
        assert cluster_shards[0][0] == b'slots'
        assert cluster_shards[0][1] == []
        assert cluster_shards[0][2] == b'nodes'
        assert len(cluster_shards[0][3]) == 1
        assert len(cluster_shards[0][3][0]) == 14
        assert cluster_shards[0][3][0][0] == b'id'
        assert cluster_shards[0][3][0][1] == node_id_1
        if pytestconfig.getoption('tls'):
            assert cluster_shards[0][3][0][2] == b'tls-port'
        else:
            assert cluster_shards[0][3][0][2] == b'port'
        assert cluster_shards[0][3][0][3] == 1111
        assert cluster_shards[0][3][0][4] == b'ip'
        assert cluster_shards[0][3][0][5] == b'1.1.1.1'
        assert cluster_shards[0][3][0][6] == b'endpoint'
        assert cluster_shards[0][3][0][7] == b'1.1.1.1'

    validate_shards(cluster.node(1).execute('CLUSTER', 'SHARDS'))


def test_cluster_shards_for_single_slot_range_sg_multiple_nodes(cluster):
    cluster.create(3, raft_args={
        'sharding': 'yes',
        'external-sharding': 'yes'})

    cluster_shardgroup_id = "1" * 32
    node_id_1 = "{}00000001".format(cluster_shardgroup_id).encode()
    node_id_2 = "{}00000002".format(cluster_shardgroup_id).encode()
    node_id_3 = "{}00000003".format(cluster_shardgroup_id).encode()

    assert cluster.execute(
        'RAFT.SHARDGROUP', 'REPLACE',
        1,

        cluster_shardgroup_id,
        1, 3,
        1000, 2000, SlotRangeType.STABLE, 123,
        node_id_1, '1.1.1.1:1111',
        node_id_2, '2.2.2.2:2222',
        node_id_3, '3.3.3.3:3333'
    ) == b'OK'

    def validate_shards(cluster_shards):
        assert len(cluster_shards) == 1
        assert cluster_shards[0][0] == b'slots'
        assert len(cluster_shards[0][1]) == 2
        assert cluster_shards[0][1][0] == 1000
        assert cluster_shards[0][1][1] == 2000
        assert cluster_shards[0][2] == b'nodes'
        assert len(cluster_shards[0][3]) == 3
        assert len(cluster_shards[0][3][0]) == 14
        for i in range(len(cluster_shards[0][3])):
            if cluster_shards[0][3][i][1] == node_id_1:
                assert cluster_shards[0][3][i][3] == 1111
                assert cluster_shards[0][3][i][7] == b'1.1.1.1'
            elif cluster_shards[0][3][i][1] == node_id_2:
                assert cluster_shards[0][3][i][3] == 2222
                assert cluster_shards[0][3][i][7] == b'2.2.2.2'
            elif cluster_shards[0][3][i][1] == node_id_3:
                assert cluster_shards[0][3][i][3] == 3333
                assert cluster_shards[0][3][i][7] == b'3.3.3.3'
            else:
                assert False, "didn't match %s" % cluster_shards

    validate_shards(cluster.node(1).execute('CLUSTER', 'SHARDS'))


def test_cluster_shards_for_single_slot_range_sg(cluster, pytestconfig):
    cluster.create(3, raft_args={
        'sharding': 'yes',
        'external-sharding': 'yes'})

    stable_shardgroup_id = "1" * 32
    stable_node_id = "{}00000001".format(stable_shardgroup_id).encode()
    importing_shardgroup_id = "2" * 32
    importing_node_id = "{}00000001".format(importing_shardgroup_id).encode()
    migrating_shardgroup_id = "3" * 32
    migrating_node_id = "{}00000001".format(migrating_shardgroup_id).encode()

    assert cluster.execute(
        'RAFT.SHARDGROUP', 'REPLACE',
        '3',

        stable_shardgroup_id,
        '1', '1',
        '0', '500', SlotRangeType.STABLE, '123',
        stable_node_id, '1.1.1.1:1111',

        importing_shardgroup_id,
        '1', '1',
        '501', '16383', SlotRangeType.IMPORTING, '123',
        importing_node_id, '2.2.2.2:2222',

        migrating_shardgroup_id,
        '1', '1',
        '501', '16383', SlotRangeType.MIGRATING, '123',
        migrating_node_id, '3.3.3.3:3333',
        ) == b'OK'

    def validate_shards(cluster_shards):
        def validate_shard(shard):
            assert shard[0] == b'slots'
            assert shard[2] == b'nodes'
            if len(shard[1]) == 2:
                if shard[1][0] == 0:
                    assert shard[1][1] == 500
                    assert len(shard[3]) == 1
                    assert len(shard[3][0]) == 14
                    assert shard[3][0][0] == b'id'
                    assert shard[3][0][1] == stable_node_id
                    if pytestconfig.getoption('tls'):
                        assert cluster_shards[0][3][0][2] == b'tls-port'
                    else:
                        assert cluster_shards[0][3][0][2] == b'port'
                    assert shard[3][0][3] == 1111
                    assert shard[3][0][7] == b'1.1.1.1'
                elif shard[1][0] == 501:
                    assert len(shard[1]) == 2
                    assert shard[1][1] == 16383
                    assert len(shard[3]) == 1
                    assert len(shard[3][0]) == 14
                    assert shard[3][0][0] == b'id'
                    assert shard[3][0][1] == migrating_node_id
                    if pytestconfig.getoption('tls'):
                        assert cluster_shards[0][3][0][2] == b'tls-port'
                    else:
                        assert cluster_shards[0][3][0][2] == b'port'
                    assert shard[3][0][3] == 3333
                    assert shard[3][0][7] == b'3.3.3.3'
            elif len(shard[1]) == 0:
                assert len(shard[3]) == 1
                assert len(shard[3][0]) == 14
                assert shard[3][0][1] == importing_node_id
                assert shard[3][0][3] == 2222
                assert shard[3][0][7] == b'2.2.2.2'
            else:
                assert False, "didn't match %s" % shard

        assert len(cluster_shards) == 3
        for i in range(len(cluster_shards)):
            validate_shard(cluster_shards[i])

    validate_shards(cluster.node(1).execute('CLUSTER', 'SHARDS'))


def test_cluster_shards_for_multiple_slots_range_sg(cluster):
    cluster.create(3, raft_args={
        'sharding': 'yes',
        'external-sharding': 'yes'})

    shardgroup_id_1 = "1" * 32
    node_id_1 = "{}00000001".format(shardgroup_id_1).encode()
    shardgroup_id_2 = "2" * 32
    node_id_2 = "{}00000001".format(shardgroup_id_2).encode()
    shardgroup_id_3 = "3" * 32
    node_id_3 = "{}00000001".format(shardgroup_id_3).encode()

    assert cluster.execute(
        'RAFT.SHARDGROUP', 'REPLACE',
        '3',

        shardgroup_id_1,
        '3', '1',
        '0', '500', SlotRangeType.STABLE, '123',
        '600', '700', SlotRangeType.IMPORTING, '123',
        '800', '1000', SlotRangeType.MIGRATING, '123',
        node_id_1, '1.1.1.1:1111',

        shardgroup_id_2,
        '2', '1',
        '1001', '16383', SlotRangeType.IMPORTING, '123',
        '600', '700', SlotRangeType.MIGRATING, '123',
        node_id_2, '2.2.2.2:2222',

        shardgroup_id_3,
        '2', '1',
        '800', '1000', SlotRangeType.IMPORTING, '123',
        '1001', '16383', SlotRangeType.MIGRATING, '123',
        node_id_3, '3.3.3.3:3333',
        ) == b'OK'

    def validate_shards(cluster_shards):
        assert len(cluster_shards) == 3

        for shard in cluster_shards:
            assert len(shard[3]) == 1
            if shard[3][0][1] == node_id_1:
                assert len(shard[3]) == 1
                assert len(shard[1]) == 4
                for i in [0, 2]:
                    if shard[1][i] == 0:
                        assert shard[1][i+1] == 500
                    elif shard[1][i] == 800:
                        assert shard[1][i+1] == 1000
                    else:
                        assert False, "didn't match %s" % shard
            elif shard[3][0][1] == node_id_2:
                assert len(shard[3]) == 1
                assert len(shard[1]) == 2
                assert shard[1][0] == 600 and shard[1][1] == 700
            elif shard[3][0][1] == node_id_3:
                assert len(shard[3]) == 1
                assert len(shard[1]) == 2
                assert shard[1][0] == 1001 and shard[1][1] == 16383
            else:
                assert False, "didn't match %s" % shard

    validate_shards(cluster.node(1).execute('CLUSTER', 'SHARDS'))
