"""
This file is part of RedisRaft.

Copyright (c) 2020-2021 Redis Ltd.

RedisRaft is licensed under the Redis Source Available License (RSAL).
"""
import socket

import time
from redis import ResponseError
from pytest import raises, skip
from .sandbox import RedisRaft, RedisRaftFailedToStart


def test_info(cluster):
    r1 = cluster.add_node()
    data = r1.client.execute_command('info')
    assert data["cluster_enabled"] == 1


def test_set_cluster_id(cluster):
    id = "12345678901234567890123456789012"
    cluster.create(3, cluster_id=id)
    assert str(cluster.leader_node().raft_info()["dbid"]) == id


def test_fail_cluster_enabled(cluster):
    with raises(RedisRaftFailedToStart):
        r1 = cluster.add_node(redis_args=["--cluster_enabled", "yes"])


def test_add_node_as_a_single_leader(cluster):
    """
    Single node becomes a leader
    """
    # Do some basic sanity
    r1 = cluster.add_node()
    assert r1.client.set('key', 'value')
    assert r1.raft_info()['current_index'] == 2


def test_node_joins_and_gets_data(cluster):
    """
    Node joins and gets data
    """
    r1 = cluster.add_node()
    assert r1.client.set('key', 'value')
    r2 = cluster.add_node()
    r2.wait_for_election()
    assert r2.raft_info().get('leader_id') == 1
    assert r2.raft_debug_exec('get', 'key') == b'value'

    # Also validate -MOVED as expected
    with raises(ResponseError, match='MOVED'):
        r2.client.set('key', 'value')


def test_single_node_log_is_reapplied(cluster):
    """Single node log is reapplied on startup"""
    r1 = cluster.add_node()
    assert r1.client.set('key', 'value')
    r1.restart()
    r1.wait_for_election()
    assert r1.raft_info().get('leader_id') == 1
    r1.wait_for_log_applied()
    assert r1.client.get('key') == b'value'


def test_reelection_basic_flow(cluster):
    """
    Basic reelection flow
    """
    cluster.create(3)
    assert cluster.leader == 1
    assert cluster.leader_node().client.set('key', 'value')
    cluster.node(1).terminate()
    cluster.node(2).wait_for_election()
    assert cluster.execute('set', 'key2', 'value')


def test_resp3(cluster):
    cluster.create(3)

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    host_ip = socket.gethostbyname('localhost')
    s.connect((host_ip, cluster.node(cluster.leader).port))

    s.send("*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n".encode())
    assert s.recv(1024).decode().startswith("%7")
    s.send("*4\r\n$4\r\nhset\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$3\r\nbaz\r\n".encode())
    assert s.recv(1024).decode().startswith(":1")
    s.send("*2\r\n$7\r\nhgetall\r\n$3\r\nfoo\r\n".encode())
    assert s.recv(1024).decode() == '%1\r\n$3\r\nbar\r\n$3\r\nbaz\r\n'


def test_proxying(cluster):
    """
    Command proxying from follower to leader works
    """
    cluster.create(3)
    assert cluster.leader == 1
    with raises(ResponseError, match='MOVED'):
        assert cluster.node(2).client.set('key', 'value')
    assert cluster.node(2).client.execute_command(
        'RAFT.CONFIG', 'SET', 'follower-proxy', 'yes') == b'OK'

    # Basic sanity
    assert cluster.node(2).client.set('key', 'value2')
    assert cluster.leader_node().client.get('key') == b'value2'

    # Numeric values
    assert cluster.node(2).client.sadd('myset', 'a') == 1
    assert cluster.node(2).client.sadd('myset', 'b') == 1

    # Multibulk
    assert set(cluster.node(2).client.smembers('myset')) == set([b'a', b'b'])

    # Nested multibulk
    assert set(cluster.node(2).client.execute_command(
        'EVAL', 'return {{\'a\',\'b\',\'c\'}};', 0)[0]) == set(
            [b'a', b'b', b'c'])
    # Error
    with raises(ResponseError, match='WRONGTYPE'):
        cluster.node(2).client.incr('myset')


def test_readonly_commands(cluster):
    """
    Test read-only command execution, which does not go through the Raft
    log.
    """
    cluster.create(3)
    assert cluster.leader == 1

    # Write something
    assert cluster.node(1).current_index() == 5
    assert cluster.node(1).client.set('key', 'value')
    assert cluster.node(1).current_index() == 6

    # Read something, log should not grow
    assert cluster.node(1).client.get('key') == b'value'
    assert cluster.node(1).current_index() == 6

    # Tear down cluster, reads should hang
    cluster.node(2).terminate()
    cluster.node(3).terminate()
    conn = cluster.node(1).client.connection_pool.get_connection(
        'RAFT', socket_timeout=1)
    conn.send_command('GET', 'key')
    assert not conn.can_read(timeout=1)

    # Now configure non-quorum reads
    cluster.node(1).raft_config_set('quorum-reads', 'no')
    assert cluster.node(1).client.get('key') == b'value'


def test_auto_ids(cluster):
    """
    Test automatic assignment of ids.
    """

    # -- Test auto id on create --
    node1 = cluster.add_node(cluster_setup=False, use_id_arg=False)
    node1.start()

    # No id initially
    assert node1.raft_info()['node_id'] == 0

    # Create cluster, id should be generated
    node1.cluster('init')
    assert node1.raft_info()['node_id'] != 0

    # -- Test auto id on join --
    node2 = cluster.add_node(cluster_setup=False, use_id_arg=False)
    node2.start()

    assert node2.raft_info()['node_id'] == 0
    node2.cluster('join', '127.0.0.1:5001')
    node2.wait_for_node_voting()
    assert node2.raft_info()['node_id'] != 0

    # -- Test recovery of id from log after restart --
    _id = node2.raft_info()['node_id']
    node2.restart()
    assert _id == node2.raft_info()['node_id']


def test_interception_does_not_affect_lua(cluster):
    """
    Make sure command interception does not affect Lua commands.
    """

    r1 = cluster.add_node()
    assert r1.raft_info()['current_index'] == 1
    assert r1.client.execute_command('EVAL', """
redis.call('SET','key1','value1');
redis.call('SET','key2','value2');
redis.call('SET','key3','value3');
return 1234;""", '0') == 1234
    assert r1.raft_info()['current_index'] == 2
    assert r1.client.get('key1') == b'value1'
    assert r1.client.get('key2') == b'value2'
    assert r1.client.get('key3') == b'value3'


def test_proxying_with_interception(cluster):
    """
    Test follower proxy mode together with command interception enabled.

    This is a regression test for issues #13, #14 and basically ensures
    that command filtering does not trigger re-entrancy when processing
    a log entry over a thread safe context.
    """
    cluster.create(3)
    assert cluster.leader == 1

    assert cluster.node(1).client.execute_command(
        'RAFT.CONFIG', 'SET', 'follower-proxy', 'yes') == b'OK'
    assert cluster.node(2).client.execute_command(
        'RAFT.CONFIG', 'SET', 'follower-proxy', 'yes') == b'OK'
    assert cluster.node(3).client.execute_command(
        'RAFT.CONFIG', 'SET', 'follower-proxy', 'yes') == b'OK'

    assert cluster.node(1).client.rpush('list-a', 'x') == 1
    assert cluster.node(1).client.rpush('list-a', 'x') == 2
    assert cluster.node(1).client.rpush('list-a', 'x') == 3

    time.sleep(1)
    assert cluster.node(1).raft_info()['current_index'] == 8


def test_rolled_back_reply(cluster):
    """
    Watch the reply to a write operation that ends up being discarded
    due to election change before commit.
    """

    cluster.create(3)

    cluster.node(2).pause()
    cluster.node(3).pause()

    conn = cluster.node(1).client.connection_pool.get_connection('deferred')
    conn.send_command('INCR', 'key')

    cluster.node(1).pause()

    cluster.node(2).kill()
    cluster.node(3).kill()
    cluster.node(2).start()
    cluster.node(3).start()

    cluster.node(2).wait_for_election()
    cluster.node(1).resume()

    with raises(ResponseError, match='TIMEOUT'):
        conn.read_response()


def test_rolled_back_read_only_reply(cluster):
    """
    Watch the reply to a read-only operation that ends up being discarded
    due to election change before commit.
    """

    cluster.create(3)
    cluster.node(1).client.set('key', 'value')

    cluster.node(2).pause()
    cluster.node(3).pause()

    conn = cluster.node(1).client.connection_pool.get_connection('deferred')
    conn.send_command('GET', 'key')

    cluster.node(1).pause()

    cluster.node(2).kill()
    cluster.node(3).kill()
    cluster.node(2).start()
    cluster.node(3).start()

    cluster.node(2).wait_for_election()
    cluster.node(1).resume()

    with raises(ResponseError, match='TIMEOUT'):
        conn.read_response()


def test_rolled_back_multi_reply(cluster):
    """
    Watch the reply to a MULTI operation that ends up being discarded
    due to election change before commit.
    """

    cluster.create(3)
    cluster.node(1).client.set('key', 'value')

    cluster.node(2).pause()
    cluster.node(3).pause()

    conn = cluster.node(1).client.connection_pool.get_connection('deferred')
    conn.send_command('MULTI')
    assert conn.read_response() == b'OK'
    conn.send_command('INCR', 'key')
    assert conn.read_response() == b'QUEUED'
    conn.send_command('EXEC')

    cluster.node(1).pause()

    cluster.node(2).kill()
    cluster.node(3).kill()
    cluster.node(2).start()
    cluster.node(3).start()

    cluster.node(2).wait_for_election()
    cluster.node(1).resume()

    with raises(ResponseError, match='TIMEOUT'):
        assert conn.read_response() == None


def test_rolled_back_read_only_multi_reply(cluster):
    """
    Watch the reply to a MULTI operation that ends up being discarded
    due to election change before commit.
    """

    cluster.create(3)
    cluster.node(1).client.set('key', 'value')

    cluster.node(2).pause()
    cluster.node(3).pause()

    conn = cluster.node(1).client.connection_pool.get_connection('deferred')
    conn.send_command('MULTI')
    assert conn.read_response() == b'OK'
    conn.send_command('GET', 'key')
    assert conn.read_response() == b'QUEUED'
    conn.send_command('EXEC')

    cluster.node(1).pause()

    cluster.node(2).kill()
    cluster.node(3).kill()
    cluster.node(2).start()
    cluster.node(3).start()

    cluster.node(2).wait_for_election()
    cluster.node(1).resume()

    with raises(ResponseError, match='TIMEOUT'):
        assert conn.read_response() == None
