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
