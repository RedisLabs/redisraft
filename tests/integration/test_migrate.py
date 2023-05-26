"""
Copyright Redis Ltd. 2020 - present
Licensed under your choice of the Redis Source Available License 2.0 (RSALv2)
or the Server Side Public License v1 (SSPLv1).
"""
import logging

import redis

from _pytest.python_api import raises
from redis import ResponseError

from .sandbox import RawConnection, RedisRaft, RedisRaftTimeout, SlotRangeType


def migrate_slots(cluster, slots):
    cursor = 0
    max_retries = 100

    while True:
        leader = cluster.leader_node()

        try:
            reply = leader.execute('raft.scan', str(cursor), slots)
            cursor = int(reply[0])
            keys = reply[1]

            if len(keys) != 0:
                key_names = []
                for key in keys:
                    key_names.append(key[0].decode('utf-8'))

                assert leader.execute('migrate', '', '', '', '', '', 'keys',
                                      *key_names) == b'OK'

            # If cursor is zero, we've moved all the keys
            if cursor == 0:
                break

        except (redis.ConnectionError, redis.ResponseError,
                redis.TimeoutError) as e:
            logging.info('raft.scan command failed with error: ' + str(e))

            max_retries -= 1
            if max_retries == 0:
                raise RedisRaftTimeout("migrate command was not successful")

            # In case of error, we just update the leader node and start from
            # beginning. If leader has changed, our current 'cursor' value
            # is not valid on the new leader.
            cursor = 0
            cluster.update_leader()
            continue


def test_migration_basic(cluster_factory):
    """
    Simulates full migration process. Test generates keys in a cluster and
    moves half of the keys to another cluster.
    """

    cluster1 = cluster_factory().create(3, raft_args={
        'sharding': 'yes',
        'external-sharding': 'yes'})
    cluster2 = cluster_factory().create(3, raft_args={
        'sharding': 'yes',
        'external-sharding': 'yes'})

    key_count = 11500

    # Fill two specific slots with keys. One of these slots will be migrated.
    for i in range(key_count):
        cluster1.execute('set', '{key}' + str(i), 'value')   # slot 12539
        cluster1.execute('set', '{key2}' + str(i), 'value')  # slot 4998

    cluster1_dbid = cluster1.leader_node().info()['raft_dbid']
    cluster2_dbid = cluster2.leader_node().info()['raft_dbid']

    # Set slot range 8001-16383 as migrating from cluster1 to cluster2.
    assert cluster1.execute(
        'RAFT.SHARDGROUP', 'REPLACE',
        '2',
        cluster1_dbid,
        '2', '3',
        '0', '8000', SlotRangeType.STABLE, '123',
        '8001', '16383', SlotRangeType.MIGRATING, '123',
        '%s00000001' % cluster1_dbid, 'localhost:%s' % cluster1.node(1).port,
        '%s00000002' % cluster1_dbid, 'localhost:%s' % cluster1.node(2).port,
        '%s00000003' % cluster1_dbid, 'localhost:%s' % cluster1.node(3).port,
        cluster2_dbid,
        '1', '3',
        '8001', '16383', SlotRangeType.IMPORTING, '123',
        '%s00000001' % cluster2_dbid, 'localhost:%s' % cluster2.node(1).port,
        '%s00000002' % cluster2_dbid, 'localhost:%s' % cluster2.node(2).port,
        '%s00000003' % cluster2_dbid, 'localhost:%s' % cluster2.node(3).port,
        ) == b'OK'

    assert cluster2.execute(
        'RAFT.SHARDGROUP', 'REPLACE',
        '2',
        cluster1_dbid,
        '2', '3',
        '0', '8000', SlotRangeType.STABLE, '123',
        '8001', '16383',  SlotRangeType.MIGRATING, '123',
        '%s00000001' % cluster1_dbid, 'localhost:%s' % cluster1.node(1).port,
        '%s00000002' % cluster1_dbid, 'localhost:%s' % cluster1.node(2).port,
        '%s00000003' % cluster1_dbid, 'localhost:%s' % cluster1.node(3).port,
        cluster2_dbid,
        '1', '3',
        '8001', '16383', SlotRangeType.IMPORTING, '123',
        '%s00000001' % cluster2_dbid, 'localhost:%s' % cluster2.node(1).port,
        '%s00000002' % cluster2_dbid, 'localhost:%s' % cluster2.node(2).port,
        '%s00000003' % cluster2_dbid, 'localhost:%s' % cluster2.node(3).port,
        ) == b'OK'

    migrate_slots(cluster1, '8001-16383')

    # Finalize migration
    assert cluster1.execute(
        'RAFT.SHARDGROUP', 'REPLACE',
        '2',
        cluster1_dbid,
        '1', '3',
        '0', '8000', SlotRangeType.STABLE, '123',
        '%s00000001' % cluster1_dbid, 'localhost:%s' % cluster1.node(1).port,
        '%s00000002' % cluster1_dbid, 'localhost:%s' % cluster1.node(2).port,
        '%s00000003' % cluster1_dbid, 'localhost:%s' % cluster1.node(3).port,
        cluster2_dbid,
        '1', '3',
        '8001', '16383', SlotRangeType.STABLE, '123',
        '%s00000001' % cluster2_dbid, 'localhost:%s' % cluster2.node(1).port,
        '%s00000002' % cluster2_dbid, 'localhost:%s' % cluster2.node(2).port,
        '%s00000003' % cluster2_dbid, 'localhost:%s' % cluster2.node(3).port,
        ) == b'OK'

    assert cluster2.execute(
        'RAFT.SHARDGROUP', 'REPLACE',
        '2',
        cluster1_dbid,
        '1', '3',
        '0', '8000', SlotRangeType.STABLE, '123',
        '%s00000001' % cluster1_dbid, 'localhost:%s' % cluster1.node(1).port,
        '%s00000002' % cluster1_dbid, 'localhost:%s' % cluster1.node(2).port,
        '%s00000003' % cluster1_dbid, 'localhost:%s' % cluster1.node(3).port,
        cluster2_dbid,
        '1', '3',
        '8001', '16383', SlotRangeType.STABLE, '123',
        '%s00000001' % cluster2_dbid, 'localhost:%s' % cluster2.node(1).port,
        '%s00000002' % cluster2_dbid, 'localhost:%s' % cluster2.node(2).port,
        '%s00000003' % cluster2_dbid, 'localhost:%s' % cluster2.node(3).port,
        ) == b'OK'

    # Sanity check

    # cluster-1 has slots 0-8000
    assert cluster1.execute('dbsize') == key_count
    assert cluster1.node(1).execute('get', '{key2}1') == b'value'
    with raises(ResponseError, match='MOVED 12539'):
        cluster1.node(1).execute('get', '{key}1')

    # cluster-2 has slots 8001-16383
    assert cluster2.execute('dbsize') == key_count
    assert cluster2.node(1).execute('get', '{key}1') == b'value'
    with raises(ResponseError, match='MOVED 4998'):
        cluster2.node(1).execute('get', '{key2}1')


def test_migration_basic_on_oom(cluster_factory):
    """
    Simulates full migration process while cluster is OOM. Test generates keys
    in a cluster and moves half of the keys to another cluster.
    """

    cluster1 = cluster_factory().create(3, raft_args={
        'sharding': 'yes',
        'external-sharding': 'yes'})
    cluster2 = cluster_factory().create(3, raft_args={
        'sharding': 'yes',
        'external-sharding': 'yes'})

    key_count = 11500

    # Fill two specific slots with keys. One of these slots will be migrated.
    for i in range(key_count):
        cluster1.execute('set', '{key}' + str(i), 'value')   # slot 12539
        cluster1.execute('set', '{key2}' + str(i), 'value')  # slot 4998

    cluster1.wait_for_unanimity()
    cluster2.wait_for_unanimity()

    cluster1.config_set('maxmemory', '1')
    cluster2.config_set('maxmemory', '1')

    cluster1_dbid = cluster1.leader_node().info()['raft_dbid']
    cluster2_dbid = cluster2.leader_node().info()['raft_dbid']

    # Set slot range 8001-16383 as migrating from cluster1 to cluster2.
    assert cluster1.execute(
        'RAFT.SHARDGROUP', 'REPLACE',
        '2',
        cluster1_dbid,
        '2', '3',
        '0', '8000', SlotRangeType.STABLE, '123',
        '8001', '16383', SlotRangeType.MIGRATING, '123',
        '%s00000001' % cluster1_dbid, 'localhost:%s' % cluster1.node(1).port,
        '%s00000002' % cluster1_dbid, 'localhost:%s' % cluster1.node(2).port,
        '%s00000003' % cluster1_dbid, 'localhost:%s' % cluster1.node(3).port,
        cluster2_dbid,
        '1', '3',
        '8001', '16383', SlotRangeType.IMPORTING, '123',
        '%s00000001' % cluster2_dbid, 'localhost:%s' % cluster2.node(1).port,
        '%s00000002' % cluster2_dbid, 'localhost:%s' % cluster2.node(2).port,
        '%s00000003' % cluster2_dbid, 'localhost:%s' % cluster2.node(3).port,
        ) == b'OK'

    assert cluster2.execute(
        'RAFT.SHARDGROUP', 'REPLACE',
        '2',
        cluster1_dbid,
        '2', '3',
        '0', '8000', SlotRangeType.STABLE, '123',
        '8001', '16383', SlotRangeType.MIGRATING, '123',
        '%s00000001' % cluster1_dbid, 'localhost:%s' % cluster1.node(1).port,
        '%s00000002' % cluster1_dbid, 'localhost:%s' % cluster1.node(2).port,
        '%s00000003' % cluster1_dbid, 'localhost:%s' % cluster1.node(3).port,
        cluster2_dbid,
        '1', '3',
        '8001', '16383', SlotRangeType.IMPORTING, '123',
        '%s00000001' % cluster2_dbid, 'localhost:%s' % cluster2.node(1).port,
        '%s00000002' % cluster2_dbid, 'localhost:%s' % cluster2.node(2).port,
        '%s00000003' % cluster2_dbid, 'localhost:%s' % cluster2.node(3).port,
        ) == b'OK'

    migrate_slots(cluster1, '8001-16383')

    # Finalize migration
    assert cluster1.execute(
        'RAFT.SHARDGROUP', 'REPLACE',
        '2',
        cluster1_dbid,
        '1', '3',
        '0', '8000', SlotRangeType.STABLE, '123',
        '%s00000001' % cluster1_dbid, 'localhost:%s' % cluster1.node(1).port,
        '%s00000002' % cluster1_dbid, 'localhost:%s' % cluster1.node(2).port,
        '%s00000003' % cluster1_dbid, 'localhost:%s' % cluster1.node(3).port,
        cluster2_dbid,
        '1', '3',
        '8001', '16383', SlotRangeType.STABLE, '123',
        '%s00000001' % cluster2_dbid, 'localhost:%s' % cluster2.node(1).port,
        '%s00000002' % cluster2_dbid, 'localhost:%s' % cluster2.node(2).port,
        '%s00000003' % cluster2_dbid, 'localhost:%s' % cluster2.node(3).port,
        ) == b'OK'

    assert cluster2.execute(
        'RAFT.SHARDGROUP', 'REPLACE',
        '2',
        cluster1_dbid,
        '1', '3',
        '0', '8000', SlotRangeType.STABLE, '123',
        '%s00000001' % cluster1_dbid, 'localhost:%s' % cluster1.node(1).port,
        '%s00000002' % cluster1_dbid, 'localhost:%s' % cluster1.node(2).port,
        '%s00000003' % cluster1_dbid, 'localhost:%s' % cluster1.node(3).port,
        cluster2_dbid,
        '1', '3',
        '8001', '16383', SlotRangeType.STABLE, '123',
        '%s00000001' % cluster2_dbid, 'localhost:%s' % cluster2.node(1).port,
        '%s00000002' % cluster2_dbid, 'localhost:%s' % cluster2.node(2).port,
        '%s00000003' % cluster2_dbid, 'localhost:%s' % cluster2.node(3).port,
        ) == b'OK'

    # Sanity check

    # cluster-1 has slots 0-8000
    assert cluster1.execute('dbsize') == key_count
    for i in range(key_count):
        # Verify the keys in slot 4998 stay in cluster1
        assert cluster1.execute('get', '{key2}' + str(i)) == b'value'

        # Verify MOVED response for the keys in slot 12539
        with raises(ResponseError, match='MOVED 12539'):
            cluster1.leader_node().execute('get', '{key}' + str(i))

    # cluster-2 has slots 8001-16383
    assert cluster2.execute('dbsize') == key_count
    for i in range(key_count):
        # Verify the keys in slot 12539 are in cluster2
        assert cluster2.execute('get', '{key}' + str(i)) == b'value'

        # Verify MOVED response for the keys in slot 4998
        with raises(ResponseError, match='MOVED 4998'):
            cluster2.leader_node().execute('get', '{key2}' + str(i))


def test_raft_import(cluster):
    cluster.create(3, raft_args={
        'sharding': 'yes',
        'external-sharding': 'yes'})

    assert cluster.execute('set', 'key', 'value')
    assert cluster.execute('get', 'key') == b'value'

    dump = cluster.execute('dump', 'key')
    assert cluster.execute('del', 'key') == 1

    assert cluster.execute('get', 'key') is None

    assert cluster.execute(
        'RAFT.SHARDGROUP', 'REPLACE',
        '2',

        '12345678901234567890123456789013',
        '1', '1',
        '0', '16383', SlotRangeType.MIGRATING, '123',
        '1234567890123456789012345678901334567890', '3.3.3.3:3333',

        cluster.leader_node().info()["raft_dbid"],
        '1', '1',
        '0', '16383', SlotRangeType.IMPORTING, '123',
        '1234567890123456789012345678901234567890', '2.2.2.2:2222',
    ) == b'OK'

    # initial import
    assert cluster.execute('raft.import', '2', '123', 'key', dump) == b'OK'
    # older term, fails
    with raises(ResponseError, match='invalid term'):
        assert cluster.execute('raft.import', '1', '123', 'key', dump)
    # not matched session migration key
    with raises(ResponseError, match='invalid migration_session_key'):
        assert cluster.execute('raft.import', '2', '10', 'key', dump)
    # repeated with correct values
    assert cluster.execute('raft.import', '2', '123', 'key', dump) == b'OK'
    # repeated with updated term
    assert cluster.execute('raft.import', '3', '123', 'key', dump) == b'OK'
    # again, older, previously valid term
    with raises(ResponseError, match='invalid term'):
        assert cluster.execute('raft.import', '2', '123', 'key', dump)

    conn = RawConnection(cluster.leader_node().client)
    assert conn.execute('asking') == b'OK'
    assert conn.execute('get', 'key') == b'value'


def test_import_with_snapshot(cluster):
    cluster.create(1, raft_args={
        'sharding': 'yes',
        'external-sharding': 'yes'})

    assert cluster.execute('set', 'key', 'value')
    assert cluster.execute('get', 'key') == b'value'

    dump = cluster.execute('dump', 'key')
    assert cluster.execute('del', 'key') == 1

    assert cluster.execute('get', 'key') is None

    assert cluster.execute(
        'RAFT.SHARDGROUP', 'REPLACE',
        '2',
        '12345678901234567890123456789013',
        '1', '1',
        '0', '16383', SlotRangeType.MIGRATING, '123',
        '1234567890123456789012345678901334567890', '3.3.3.3:3333',
        cluster.leader_node().info()["raft_dbid"],
        '1', '1',
        '0', '16383', SlotRangeType.IMPORTING, '123',
        '1234567890123456789012345678901234567890', '2.2.2.2:2222',
    ) == b'OK'

    # initial import
    assert cluster.execute('raft.import', '2', '123', 'key', dump) == b'OK'

    assert cluster.node(1).client.execute_command(
        'RAFT.DEBUG', 'COMPACT') == b'OK'

    cluster.node(1).kill()
    cluster.node(1).start()
    cluster.node(1).wait_for_info_param('raft_state', 'up')

    # incorrect session migration key
    with raises(ResponseError, match="invalid migration_session_key"):
        cluster.execute('raft.import', '2', '0', 'key', dump)

    # older term should not be accepted
    with raises(ResponseError, match="invalid term"):
        cluster.execute('raft.import', '1', '123', 'key', dump)

    # same term should still work after snapshot load
    assert cluster.execute('raft.import', '2', '123', 'key', dump) == b'OK'


def test_happy_migrate(cluster_factory):
    cluster1 = cluster_factory().create(1, raft_args={
        'sharding': 'yes',
        'external-sharding': 'yes'})
    cluster2 = cluster_factory().create(1, raft_args={
        'sharding': 'yes',
        'external-sharding': 'yes'})

    cluster1_dbid = cluster1.leader_node().info()["raft_dbid"]
    cluster2_dbid = cluster2.leader_node().info()["raft_dbid"]

    assert cluster1.execute('set', 'key', 'value')
    assert cluster1.execute('get', 'key') == b'value'
    assert cluster1.execute('set', '{key}key1', 'value1')
    assert cluster1.execute('get', '{key}key1') == b'value1'

    assert cluster1.execute(
        'RAFT.SHARDGROUP', 'REPLACE',
        '2',
        cluster2_dbid,
        '1', '1',
        '0', '16383', SlotRangeType.IMPORTING, '123',
        '%s00000001' % cluster2_dbid, 'localhost:%s' % cluster2.node(1).port,
        cluster1_dbid,
        '1', '1',
        '0', '16383', SlotRangeType.MIGRATING, '123',
        '%s00000001' % cluster1_dbid, 'localhost:%s' % cluster1.node(1).port,
        ) == b'OK'

    assert cluster2.execute(
        'RAFT.SHARDGROUP', 'REPLACE',
        '2',
        cluster2_dbid,
        '1', '1',
        '0', '16383', SlotRangeType.IMPORTING, '123',
        '%s00000001' % cluster2_dbid, 'localhost:%s' % cluster2.node(1).port,
        cluster1_dbid,
        '1', '1',
        '0', '16383', SlotRangeType.MIGRATING, '123',
        '%s00000001' % cluster1_dbid, 'localhost:%s' % cluster1.node(1).port,
        ) == b'OK'

    assert cluster1.execute("migrate", "", "", "", "", "", "keys", "key",
                            "{key}key1") == b'OK'

    with raises(ResponseError, match="ASK 12539 localhost"):
        cluster1.execute("get", "key")

    with raises(ResponseError, match="ASK 12539 localhost"):
        cluster1.execute("get", "{key}key1")

    with raises(ResponseError, match="MOVED 12539 localhost"):
        cluster2.leader_node().client.get("key")

    conn = RawConnection(cluster2.leader_node().client)
    assert conn.execute('asking') == b'OK'

    assert conn.execute('get', 'key') == b'value'

    with raises(ResponseError, match="MOVED 12539 localhost:5001"):
        conn.execute('get', '{key}key1')

    assert conn.execute('asking') == b'OK'
    assert conn.execute('get', '{key}key1') == b'value1'

    with raises(ResponseError, match="MOVED 9189 localhost:5001"):
        conn.execute('get', 'key1')

    assert conn.execute('asking') == b'OK'
    with raises(ResponseError, match="TRYAGAIN"):
        conn.execute('get', 'key1')

    assert cluster2.execute(
        'RAFT.SHARDGROUP', 'REPLACE',
        '1',
        cluster2_dbid,
        '1', '1',
        '0', '16383', SlotRangeType.STABLE, "123",
        '%s00000001' % cluster2_dbid, 'localhost:%s' % cluster2.node(1).port,
        ) == b'OK'

    assert cluster2.execute("get", "key") == b'value'
    assert cluster2.execute("get", "{key}key1") == b'value1'
    assert cluster2.execute("get", "key1") is None


def test_sad_path_migrate(cluster_factory):
    cluster1 = cluster_factory().create(1, raft_args={
        'sharding': 'yes',
        'external-sharding': 'yes'})
    cluster2 = cluster_factory().create(1, raft_args={
        'sharding': 'yes',
        'external-sharding': 'yes'})

    cluster1_dbid = cluster1.leader_node().info()["raft_dbid"]
    cluster2_dbid = cluster2.leader_node().info()["raft_dbid"]

    assert cluster1.execute('set', 'key1', 'value1')
    assert cluster1.execute('get', 'key1') == b'value1'
    assert cluster1.execute('set', 'key2', 'value2')
    assert cluster1.execute('get', 'key2') == b'value2'
    assert cluster1.execute('set', 'key3', 'value3')
    assert cluster1.execute('get', 'key3') == b'value3'

    assert cluster1.execute(
        'RAFT.SHARDGROUP', 'REPLACE',
        '2',
        cluster2_dbid,
        '1', '1',
        '0', '16383', SlotRangeType.IMPORTING, '123',
        '%s00000001' % cluster2_dbid, 'localhost:%s' % cluster2.node(1).port,
        cluster1_dbid,
        '1', '1',
        '0', '16383', SlotRangeType.MIGRATING, '123',
        '%s00000001' % cluster1_dbid, 'localhost:%s' % cluster1.node(1).port,
        ) == b'OK'

    assert cluster2.execute(
        'RAFT.SHARDGROUP', 'REPLACE',
        '2',
        cluster2_dbid,
        '1', '1',
        '0', '16383', SlotRangeType.IMPORTING, '123',
        '%s00000001' % cluster2_dbid, 'localhost:%s' % cluster2.node(1).port,
        cluster1_dbid,
        '1', '1',
        '0', '16383', SlotRangeType.MIGRATING, '123',
        '%s00000001' % cluster1_dbid, 'localhost:%s' % cluster1.node(1).port,
        ) == b'OK'

    def verify_migration_fails(key_name, value, slot, err_string):
        # first pass, should error out
        with raises(ResponseError, match=err_string):
            cluster1.execute("migrate", "", "", "", "", "", "keys", key_name)

        # validate state
        with raises(ResponseError, match="TRYAGAIN"):
            cluster1.execute("get", key_name)
        with raises(ResponseError, match=f"MOVED {slot} localhost"):
            cluster2.leader_node().client.get(key_name)

        # remove injected error, should pass
        cluster1.config_set("raft.migration-debug", "none")
        assert cluster1.execute("migrate", "", "", "", "", "", "keys",
                                key_name) == b'OK'

        # validate state
        with raises(ResponseError, match=f"ASK {slot} localhost"):
            cluster1.execute("get", key_name)
        with raises(ResponseError, match=f"MOVED {slot} localhost"):
            cluster2.leader_node().client.get(key_name)

        conn = RawConnection(cluster2.leader_node().client)
        assert conn.execute('asking') == b'OK'
        assert conn.execute('get', key_name) == value

    cluster1.config_set("raft.migration-debug", "fail-connect")
    verify_migration_fails("key1", b'value1', 9189,
                           "failed to connect to import cluster, try again")

    cluster1.config_set("raft.migration-debug", "fail-import")
    verify_migration_fails("key2", b'value2', 4998,
                           "failed to submit RAFT.IMPORT command, try again")

    cluster1.config_set("raft.migration-debug", "fail-unlock")
    verify_migration_fails("key3", b'value3', 935,
                           "Unable to unlock/delete migrated keys, try again")


def test_redirect_asking_to_leader(cluster):
    """
    Followers redirect asking mode requests to the leader with an ASK reply.
    """
    cluster.create(3)

    # Leader is node-2
    cluster.leader_node().transfer_leader(2)

    # Send a request to node-3
    follower = cluster.node(3)
    follower.execute('ASKING')

    # Expect redirection to node-2
    with raises(ResponseError, match="ASK 12539 localhost:5002"):
        follower.execute('get', 'key')


def test_asking_multi(cluster):
    cluster.create(3, raft_args={
        'sharding': 'yes',
        'external-sharding': 'yes'
    })

    cluster_dbid = cluster.leader_node().info()["raft_dbid"]

    cluster.execute("set", "key", "value")

    assert cluster.execute(
        'RAFT.SHARDGROUP', 'REPLACE',
        2,
        '12345678901234567890123456789013',
        '1', '1',
        '0', '16383', SlotRangeType.MIGRATING, '123',
        '1234567890123456789012345678901334567890', '3.3.3.3:3333',
        cluster.leader_node().info()["raft_dbid"],
        '1', '3',
        '0', '16383', SlotRangeType.IMPORTING, '123',
        "{}00000001".format(cluster_dbid).encode(), cluster.node(1).address,
        "{}00000002".format(cluster_dbid).encode(), cluster.node(2).address,
        "{}00000003".format(cluster_dbid).encode(), cluster.node(3).address,
    ) == b'OK'

    cluster.wait_for_unanimity()

    conn = RawConnection(cluster.leader_node().client)

    # 1- Verify 'asking' with 'multi'
    conn.execute('ASKING')
    conn.execute('MULTI')
    conn.execute('GET', 'key')
    conn.execute('GET', 'key')
    conn.execute('GET', 'key')
    assert conn.execute('EXEC') == [b'value', b'value', b'value']

    # 2- Verify 'EXEC' clears 'asking' flag
    conn.execute('ASKING')
    conn.execute('MULTI')
    conn.execute('GET', 'key')
    assert conn.execute('EXEC') == [b'value']

    with raises(ResponseError, match='MOVED [0-9]+ 3.3.3.3:3333'):
        conn.execute('MULTI')
        conn.execute('GET', 'key')
        conn.execute('EXEC')

    with raises(ResponseError, match='MOVED [0-9]+ 3.3.3.3:3333'):
        conn.execute('GET', 'key')

    # 3- Verify 'DISCARD' clears 'asking' flag
    conn.execute('ASKING')
    conn.execute('MULTI')
    conn.execute('GET', 'key')
    conn.execute('DISCARD')

    with raises(ResponseError, match='MOVED [0-9]+ 3.3.3.3:3333'):
        conn.execute('MULTI')
        conn.execute('GET', 'key')
        conn.execute('EXEC')

    with raises(ResponseError, match='MOVED [0-9]+ 3.3.3.3:3333'):
        conn.execute('GET', 'key')


def test_migrate_fail_if_not_ready(cluster):
    cluster1 = None
    cluster2 = None

    try:
        cluster1 = RedisRaft(1, cluster.base_port, cluster.config,
                             raft_args={
                                 'sharding': 'yes',
                                 'external-sharding': 'yes'
                             })
        cluster1.start()
        assert cluster1.info()["raft_state"] == "uninitialized"

        cluster2 = RedisRaft(1, cluster.base_port+1, cluster.config,
                             raft_args={
                                 'sharding': 'yes',
                                 'external-sharding': 'yes'
                             })
        cluster2.init()

        cluster2.execute("set", "key", "value")
        cluster2_dbid = cluster2.info()["raft_dbid"]
        assert cluster2.execute(
            'RAFT.SHARDGROUP', 'REPLACE',
            2,
            '12345678901234567890123456789013',
            '1', '1',
            '0', '16383', SlotRangeType.IMPORTING, '123',
            '1234567890123456789012345678901334567890', cluster1.address,
            cluster2_dbid.encode(),
            '1', '1',
            '0', '16383', SlotRangeType.MIGRATING, '123',
            "{}00000001".format(cluster2_dbid).encode(), cluster2.address,
        ) == b'OK'

        with raises(ResponseError,
                    match="RAFT.IMPORT failed: NOCLUSTER No Raft Cluster"):
            cluster2.execute('migrate', '', '', '', '', '', 'keys', "key")
    finally:
        if cluster1 is not None:
            cluster1.kill()
        if cluster2 is not None:
            cluster2.kill()


def test_migrate_auth(cluster_factory):
    """
    Test AUTH2 parameter of MIGRATE command
    """

    cluster1 = cluster_factory().create(1, raft_args={
        'sharding': 'yes',
        'external-sharding': 'yes'})
    cluster2 = cluster_factory().create(1, raft_args={
        'sharding': 'yes',
        'external-sharding': 'yes'})

    cluster1_dbid = cluster1.leader_node().info()["raft_dbid"]
    cluster2_dbid = cluster2.leader_node().info()["raft_dbid"]

    assert cluster1.execute('set', 'key', 'value')
    assert cluster1.execute('set', '{key}key1', 'value1')

    assert cluster1.execute(
        'RAFT.SHARDGROUP', 'REPLACE',
        '2',
        cluster2_dbid,
        '1', '1',
        '0', '16383', SlotRangeType.IMPORTING, '123',
        '%s00000001' % cluster2_dbid, 'localhost:%s' % cluster2.node(1).port,
        cluster1_dbid,
        '1', '1',
        '0', '16383', SlotRangeType.MIGRATING, '123',
        '%s00000001' % cluster1_dbid, 'localhost:%s' % cluster1.node(1).port,
        ) == b'OK'

    assert cluster2.execute(
        'RAFT.SHARDGROUP', 'REPLACE',
        '2',
        cluster2_dbid,
        '1', '1',
        '0', '16383', SlotRangeType.IMPORTING, '123',
        '%s00000001' % cluster2_dbid, 'localhost:%s' % cluster2.node(1).port,
        cluster1_dbid,
        '1', '1',
        '0', '16383', SlotRangeType.MIGRATING, '123',
        '%s00000001' % cluster1_dbid, 'localhost:%s' % cluster1.node(1).port,
        ) == b'OK'

    cluster2.execute("ACL", "SETUSER", "user", "on", ">pass", "allcommands")
    assert cluster1.execute("migrate", "", "", "", "", "",
                            "AUTH2", "user", "pass",
                            "keys", "key", "{key}key1") == b'OK'

    assert cluster2.execute(
        'RAFT.SHARDGROUP', 'REPLACE',
        '1',
        cluster2_dbid,
        '1', '1',
        '0', '16383', SlotRangeType.STABLE, "123",
        '%s00000001' % cluster2_dbid, 'localhost:%s' % cluster2.node(1).port,
        ) == b'OK'

    assert cluster2.execute("get", "key") == b'value'
    assert cluster2.execute("get", "{key}key1") == b'value1'
