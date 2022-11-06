"""
This file is part of RedisRaft.

Copyright (c) 2020-2021 Redis Ltd.

RedisRaft is licensed under the Redis Source Available License (RSAL).
"""
import time
import pytest as pytest

from .sandbox import ElleWorker, Elle, key_hash_slot


@pytest.mark.skipif("not config.getoption('elle_threads')")
@pytest.mark.elle_test()
def test_elle_sanity(cluster_factory):
    cluster1 = cluster_factory().create(3, raft_args={
        'sharding': 'yes',
        'external-sharding': 'yes'})
    time.sleep(5)


#@pytest.mark.skipif("not config.getoption('elle_threads')")
#@pytest.mark.elle_test()
def test_elle_migrating_manual(elle, cluster_factory):
    cluster1 = cluster_factory().create(3, raft_args={
        'sharding': 'yes',
        'external-sharding': 'yes'})
    cluster2 = cluster_factory().create(3, raft_args={
        'sharding': 'yes',
        'external-sharding': 'yes'})

    cluster1_dbid = cluster1.leader_node().info()["raft_dbid"]
    cluster2_dbid = cluster2.leader_node().info()["raft_dbid"]
    slot = key_hash_slot("test")

    worker = ElleWorker(elle, [cluster1, cluster2])

    ops = worker.generate_ops(["{test}key1"])
    worker.do_ops(ops)

    assert cluster1.execute(
        'RAFT.SHARDGROUP', 'REPLACE',
        '2',

        cluster1_dbid,
        3, 3,
        0, slot-1, 1,  0,
        slot, slot, 3, 0,
        slot+1, 16383, 1, 0,
        "{}00000001".format(cluster1_dbid).encode(), cluster1.node(1).address,
        "{}00000002".format(cluster1_dbid).encode(), cluster1.node(2).address,
        "{}00000003".format(cluster1_dbid).encode(), cluster1.node(3).address,

        cluster2_dbid,
        1, 3,
        slot, slot, 2, 0,
        "{}00000001".format(cluster2_dbid).encode(), cluster2.node(1).address,
        "{}00000002".format(cluster2_dbid).encode(), cluster2.node(2).address,
        "{}00000003".format(cluster2_dbid).encode(), cluster2.node(3).address,
    ) == b'OK'

    assert cluster2.execute(
        'RAFT.SHARDGROUP', 'REPLACE',
        '2',

        cluster1_dbid,
        3, 3,
        0, slot-1, 1,  0,
        slot, slot, 3, 0,
        slot+1, 16383, 1, 0,
        "{}00000001".format(cluster1_dbid).encode(), cluster1.node(1).address,
        "{}00000002".format(cluster1_dbid).encode(), cluster1.node(2).address,
        "{}00000003".format(cluster1_dbid).encode(), cluster1.node(3).address,

        cluster2_dbid,
        1, 3,
        slot, slot, 2, 0,
        "{}00000001".format(cluster2_dbid).encode(), cluster2.node(1).address,
        "{}00000002".format(cluster2_dbid).encode(), cluster2.node(2).address,
        "{}00000003".format(cluster2_dbid).encode(), cluster2.node(3).address,
    ) == b'OK'

    cluster1.wait_for_unanimity()
    cluster2.wait_for_unanimity()

    ops = worker.generate_ops(["{test}key1"])
    worker.do_ops(ops)

    assert cluster1.execute(
        'migrate', '', '', '', '', '', 'keys',
        "{test}key1", "{test}key2", "{test}key3", "{test}key4",
    ) == b'OK'

    cluster1.wait_for_unanimity()
    cluster2.wait_for_unanimity()

    ops = worker.generate_ops(["{test}key1"])
    worker.do_ops(ops)

    assert cluster1.execute(
        'RAFT.SHARDGROUP', 'REPLACE',
        '2',

        cluster1_dbid,
        2, 3,
        0, slot-1, 1,  0,
        slot+1, 16383, 1, 0,
        "{}00000001".format(cluster1_dbid).encode(), cluster1.node(1).address,
        "{}00000002".format(cluster1_dbid).encode(), cluster1.node(2).address,
        "{}00000003".format(cluster1_dbid).encode(), cluster1.node(3).address,

        cluster2_dbid,
        1, 3,
        slot, slot, 1, 0,
        "{}00000001".format(cluster2_dbid).encode(), cluster2.node(1).address,
        "{}00000002".format(cluster2_dbid).encode(), cluster2.node(2).address,
        "{}00000003".format(cluster2_dbid).encode(), cluster2.node(3).address,
    ) == b'OK'

    assert cluster2.execute(
        'RAFT.SHARDGROUP', 'REPLACE',
        '2',

        cluster1_dbid,
        2, 3,
        0, slot-1, 1,  0,
        slot+1, 16383, 1, 0,
        "{}00000001".format(cluster1_dbid).encode(), cluster1.node(1).address,
        "{}00000002".format(cluster1_dbid).encode(), cluster1.node(2).address,
        "{}00000003".format(cluster1_dbid).encode(), cluster1.node(3).address,

        cluster2_dbid,
        1, 3,
        slot, slot, 1, 0,
        "{}00000001".format(cluster2_dbid).encode(), cluster2.node(1).address,
        "{}00000002".format(cluster2_dbid).encode(), cluster2.node(2).address,
        "{}00000003".format(cluster2_dbid).encode(), cluster2.node(3).address,
    ) == b'OK'

    cluster1.wait_for_unanimity()
    cluster2.wait_for_unanimity()

    ops = worker.generate_ops(["{test}key1"])
    worker.do_ops(ops)

    assert cluster1.leader_node().raft_debug_exec("lrange", "{test}key1", 0, -1) == []
    val = cluster2.leader_node().raft_debug_exec("lrange", "{test}key1", 0, -1)
    assert type(val) is list
    assert len(val) == 4


@pytest.mark.key_hash_tag("test")
@pytest.mark.num_elle_keys(5)
@pytest.mark.elle_test()
def test_elle_migrating(request, cluster_factory):
    cluster1 = cluster_factory().create(3, raft_args={
        'sharding': 'yes',
        'external-sharding': 'yes'})
    cluster2 = cluster_factory().create(3, raft_args={
        'sharding': 'yes',
        'external-sharding': 'yes'})

    cluster1_dbid = cluster1.leader_node().info()["raft_dbid"]
    cluster2_dbid = cluster2.leader_node().info()["raft_dbid"]

    slot = -1
    marker = request.node.get_closest_marker("key_hash_tag")
    if marker is not None:
        slot = key_hash_slot(marker.args[0])
    assert slot != -1

    time.sleep(0.25)

    assert cluster1.execute(
        'RAFT.SHARDGROUP', 'REPLACE',
        '2',

        cluster1_dbid,
        3, 3,
        0, slot-1, 1,  0,
        slot, slot, 3, 0,
        slot+1, 16383, 1, 0,
        "{}00000001".format(cluster1_dbid).encode(), cluster1.node(1).address,
        "{}00000002".format(cluster1_dbid).encode(), cluster1.node(2).address,
        "{}00000003".format(cluster1_dbid).encode(), cluster1.node(3).address,

        cluster2_dbid,
        1, 3,
        slot, slot, 2, 0,
        "{}00000001".format(cluster2_dbid).encode(), cluster2.node(1).address,
        "{}00000002".format(cluster2_dbid).encode(), cluster2.node(2).address,
        "{}00000003".format(cluster2_dbid).encode(), cluster2.node(3).address,
    ) == b'OK'

    assert cluster2.execute(
        'RAFT.SHARDGROUP', 'REPLACE',
        '2',

        cluster1_dbid,
        3, 3,
        0, slot-1, 1,  0,
        slot, slot, 3, 0,
        slot+1, 16383, 1, 0,
        "{}00000001".format(cluster1_dbid).encode(), cluster1.node(1).address,
        "{}00000002".format(cluster1_dbid).encode(), cluster1.node(2).address,
        "{}00000003".format(cluster1_dbid).encode(), cluster1.node(3).address,

        cluster2_dbid,
        1, 3,
        slot, slot, 2, 0,
        "{}00000001".format(cluster2_dbid).encode(), cluster2.node(1).address,
        "{}00000002".format(cluster2_dbid).encode(), cluster2.node(2).address,
        "{}00000003".format(cluster2_dbid).encode(), cluster2.node(3).address,
    ) == b'OK'

    time.sleep(0.25)

    cursor = 0
    while True:
        reply = cluster1.execute('raft.scan', cursor, slot)

        cursor = int(reply[0])
        keys = reply[1]

        if len(keys) != 0:
            key_names = []
            for key in keys:
                key_names.append(key[0].decode('utf-8'))

            assert cluster1.execute('migrate', '', '', '', '', '', 'keys',
                                    *key_names) == b'OK'

        # If cursor is zero, we've moved all the keys
        if cursor == 0:
            break

    time.sleep(0.25)

    assert cluster1.execute(
        'RAFT.SHARDGROUP', 'REPLACE',
        '2',

        cluster1_dbid,
        2, 3,
        0, slot-1, 1,  0,
        slot+1, 16383, 1, 0,
        "{}00000001".format(cluster1_dbid).encode(), cluster1.node(1).address,
        "{}00000002".format(cluster1_dbid).encode(), cluster1.node(2).address,
        "{}00000003".format(cluster1_dbid).encode(), cluster1.node(3).address,

        cluster2_dbid,
        1, 3,
        slot, slot, 1, 0,
        "{}00000001".format(cluster2_dbid).encode(), cluster2.node(1).address,
        "{}00000002".format(cluster2_dbid).encode(), cluster2.node(2).address,
        "{}00000003".format(cluster2_dbid).encode(), cluster2.node(3).address,
    ) == b'OK'

    assert cluster2.execute(
        'RAFT.SHARDGROUP', 'REPLACE',
        '2',

        cluster1_dbid,
        2, 3,
        0, slot-1, 1,  0,
        slot+1, 16383, 1, 0,
        "{}00000001".format(cluster1_dbid).encode(), cluster1.node(1).address,
        "{}00000002".format(cluster1_dbid).encode(), cluster1.node(2).address,
        "{}00000003".format(cluster1_dbid).encode(), cluster1.node(3).address,

        cluster2_dbid,
        1, 3,
        slot, slot, 1, 0,
        "{}00000001".format(cluster2_dbid).encode(), cluster2.node(1).address,
        "{}00000002".format(cluster2_dbid).encode(), cluster2.node(2).address,
        "{}00000003".format(cluster2_dbid).encode(), cluster2.node(3).address,
    ) == b'OK'

    count = 0
    while True:
        reply = cluster2.execute('raft.scan', cursor, slot)
        cursor = int(reply[0])
        keys = reply[1]

        count += len(keys)

        if cursor == 0:
            break

    expected_count = 0
    if cluster1.config.elle_threads != 0:
        marker = request.node.get_closest_marker("num_elle_keys")
        if marker is not None:
            expected_count += marker.args[0]

    assert count == expected_count
