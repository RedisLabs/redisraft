"""
Copyright Redis Ltd. 2020 - present
Licensed under your choice of the Redis Source Available License 2.0 (RSALv2)
or the Server Side Public License v1 (SSPLv1).
"""

import time
import pytest as pytest

from .sandbox import ElleWorker, key_hash_slot, SlotRangeType


@pytest.mark.elle_test()
def test_elle_sanity(cluster_factory):
    cluster_factory().create(3, raft_args={
        'sharding': 'yes',
        'external-sharding': 'yes'})
    time.sleep(1)


@pytest.mark.skipif("not config.getoption('elle_threads')")
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
        0, slot-1, SlotRangeType.STABLE,  0,
        slot, slot, SlotRangeType.MIGRATING, 0,
        slot+1, 16383, SlotRangeType.STABLE, 0,
        "{}00000001".format(cluster1_dbid).encode(), cluster1.node(1).address,
        "{}00000002".format(cluster1_dbid).encode(), cluster1.node(2).address,
        "{}00000003".format(cluster1_dbid).encode(), cluster1.node(3).address,

        cluster2_dbid,
        1, 3,
        slot, slot, SlotRangeType.IMPORTING, 0,
        "{}00000001".format(cluster2_dbid).encode(), cluster2.node(1).address,
        "{}00000002".format(cluster2_dbid).encode(), cluster2.node(2).address,
        "{}00000003".format(cluster2_dbid).encode(), cluster2.node(3).address,
    ) == b'OK'

    assert cluster2.execute(
        'RAFT.SHARDGROUP', 'REPLACE',
        '2',

        cluster1_dbid,
        3, 3,
        0, slot-1, SlotRangeType.STABLE,  0,
        slot, slot, SlotRangeType.MIGRATING, 0,
        slot+1, 16383, SlotRangeType.STABLE, 0,
        "{}00000001".format(cluster1_dbid).encode(), cluster1.node(1).address,
        "{}00000002".format(cluster1_dbid).encode(), cluster1.node(2).address,
        "{}00000003".format(cluster1_dbid).encode(), cluster1.node(3).address,

        cluster2_dbid,
        1, 3,
        slot, slot, SlotRangeType.IMPORTING, 0,
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
        0, slot-1, SlotRangeType.STABLE,  0,
        slot+1, 16383, SlotRangeType.STABLE, 0,
        "{}00000001".format(cluster1_dbid).encode(), cluster1.node(1).address,
        "{}00000002".format(cluster1_dbid).encode(), cluster1.node(2).address,
        "{}00000003".format(cluster1_dbid).encode(), cluster1.node(3).address,

        cluster2_dbid,
        1, 3,
        slot, slot, SlotRangeType.STABLE, 0,
        "{}00000001".format(cluster2_dbid).encode(), cluster2.node(1).address,
        "{}00000002".format(cluster2_dbid).encode(), cluster2.node(2).address,
        "{}00000003".format(cluster2_dbid).encode(), cluster2.node(3).address,
    ) == b'OK'

    assert cluster2.execute(
        'RAFT.SHARDGROUP', 'REPLACE',
        '2',

        cluster1_dbid,
        2, 3,
        0, slot-1, SlotRangeType.STABLE,  0,
        slot+1, 16383, SlotRangeType.STABLE, 0,
        "{}00000001".format(cluster1_dbid).encode(), cluster1.node(1).address,
        "{}00000002".format(cluster1_dbid).encode(), cluster1.node(2).address,
        "{}00000003".format(cluster1_dbid).encode(), cluster1.node(3).address,

        cluster2_dbid,
        1, 3,
        slot, slot, SlotRangeType.STABLE, 0,
        "{}00000001".format(cluster2_dbid).encode(), cluster2.node(1).address,
        "{}00000002".format(cluster2_dbid).encode(), cluster2.node(2).address,
        "{}00000003".format(cluster2_dbid).encode(), cluster2.node(3).address,
    ) == b'OK'

    cluster1.wait_for_unanimity()
    cluster2.wait_for_unanimity()

    ops = worker.generate_ops(["{test}key1"])
    worker.do_ops(ops)

    v1 = cluster1.leader_node().raft_debug_exec("lrange", "{test}key1", 0, -1)
    assert v1 == []

    v2 = cluster2.leader_node().raft_debug_exec("lrange", "{test}key1", 0, -1)
    assert type(v2) is list
    assert len(v2) == 4


@pytest.mark.key_hash_tag("test")
@pytest.mark.num_elle_keys(5)
@pytest.mark.elle_test()
def test_elle_migrating(cluster_factory):
    cluster1 = cluster_factory().create(3, raft_args={
        'sharding': 'yes',
        'external-sharding': 'yes'})
    cluster2 = cluster_factory().create(3, raft_args={
        'sharding': 'yes',
        'external-sharding': 'yes'})

    cluster1_dbid = cluster1.leader_node().info()["raft_dbid"]
    cluster2_dbid = cluster2.leader_node().info()["raft_dbid"]

    # must correspond to @pytest.mark.key_hash_tag("test") above
    key_name = "test"
    slot = key_hash_slot(key_name)

    for i in range(5):
        cluster1.execute('set', f"{{{key_name}}}key" + str(i), 'hello')

    time.sleep(0.25)

    assert cluster1.execute(
        'RAFT.SHARDGROUP', 'REPLACE',
        '2',

        cluster1_dbid,
        3, 3,
        0, slot-1, SlotRangeType.STABLE,  0,
        slot, slot, SlotRangeType.MIGRATING, 0,
        slot+1, 16383, SlotRangeType.STABLE, 0,
        "{}00000001".format(cluster1_dbid).encode(), cluster1.node(1).address,
        "{}00000002".format(cluster1_dbid).encode(), cluster1.node(2).address,
        "{}00000003".format(cluster1_dbid).encode(), cluster1.node(3).address,

        cluster2_dbid,
        1, 3,
        slot, slot, SlotRangeType.IMPORTING, 0,
        "{}00000001".format(cluster2_dbid).encode(), cluster2.node(1).address,
        "{}00000002".format(cluster2_dbid).encode(), cluster2.node(2).address,
        "{}00000003".format(cluster2_dbid).encode(), cluster2.node(3).address,
    ) == b'OK'

    assert cluster2.execute(
        'RAFT.SHARDGROUP', 'REPLACE',
        '2',

        cluster1_dbid,
        3, 3,
        0, slot-1, SlotRangeType.STABLE,  0,
        slot, slot, SlotRangeType.MIGRATING, 0,
        slot+1, 16383, SlotRangeType.STABLE, 0,
        "{}00000001".format(cluster1_dbid).encode(), cluster1.node(1).address,
        "{}00000002".format(cluster1_dbid).encode(), cluster1.node(2).address,
        "{}00000003".format(cluster1_dbid).encode(), cluster1.node(3).address,

        cluster2_dbid,
        1, 3,
        slot, slot, SlotRangeType.IMPORTING, 0,
        "{}00000001".format(cluster2_dbid).encode(), cluster2.node(1).address,
        "{}00000002".format(cluster2_dbid).encode(), cluster2.node(2).address,
        "{}00000003".format(cluster2_dbid).encode(), cluster2.node(3).address,
    ) == b'OK'

    for i in range(5):
        cluster1.execute('set', f"{{{key_name}}}key" + str(i), 'val' + str(i))

    time.sleep(0.25)

    # migrate
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
        0, slot-1, SlotRangeType.STABLE,  0,
        slot+1, 16383, SlotRangeType.STABLE, 0,
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
        0, slot-1, SlotRangeType.STABLE,  0,
        slot+1, 16383, SlotRangeType.STABLE, 0,
        "{}00000001".format(cluster1_dbid).encode(), cluster1.node(1).address,
        "{}00000002".format(cluster1_dbid).encode(), cluster1.node(2).address,
        "{}00000003".format(cluster1_dbid).encode(), cluster1.node(3).address,

        cluster2_dbid,
        1, 3,
        slot, slot, SlotRangeType.STABLE, 0,
        "{}00000001".format(cluster2_dbid).encode(), cluster2.node(1).address,
        "{}00000002".format(cluster2_dbid).encode(), cluster2.node(2).address,
        "{}00000003".format(cluster2_dbid).encode(), cluster2.node(3).address,
    ) == b'OK'

    # validate that cluster1 has no more entries in slot
    count = 0
    while True:
        reply = cluster1.execute('raft.scan', cursor, slot)
        cursor = int(reply[0])
        keys = reply[1]

        count += len(keys)

        if cursor == 0:
            break
    assert count == 0

    # validate key(s) created by this test are on new cluster
    for i in range(5):
        key = f"{{{key_name}}}key" + str(i)
        val = f"val{str(i)}".encode()
        assert cluster2.execute('get', key) == val
