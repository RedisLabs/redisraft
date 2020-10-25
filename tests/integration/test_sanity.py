"""
This file is part of RedisRaft.

Copyright (c) 2020 Redis Labs

RedisRaft is dual licensed under the GNU Affero General Public License version 3
(AGPLv3) or the Redis Source Available License (RSAL).
"""

import time
from redis import ResponseError
from pytest import raises, skip
from .sandbox import RedisRaft


def test_add_node_as_a_single_leader(cluster):
    """
    Single node becomes a leader
    """
    # Do some basic sanity
    r1 = cluster.add_node()
    assert r1.raft_exec('SET', 'key', 'value')
    assert r1.raft_info()['current_index'] == 2


def test_node_joins_and_gets_data(cluster):
    """
    Node joins and gets data
    """
    r1 = cluster.add_node()
    assert r1.raft_exec('SET', 'key', 'value') == b'OK'
    r2 = cluster.add_node()
    r2.wait_for_election()
    assert r2.raft_info().get('leader_id') == 1
    assert r2.client.get('key') == b'value'

    # Also validate -MOVED as expected
    with raises(ResponseError, match='MOVED'):
        assert r2.raft_exec('SET', 'key', 'value') is None


def test_single_node_log_is_reapplied(cluster):
    """Single node log is reapplied on startup"""
    r1 = cluster.add_node()
    assert r1.raft_exec('SET', 'key', 'value')
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
    assert cluster.raft_exec('SET', 'key', 'value') ==  b'OK'
    cluster.node(1).terminate()
    cluster.node(2).wait_for_election()
    assert cluster.raft_exec('SET', 'key2', 'value2') == b'OK'
    cluster.exec_all('GET', 'key2')


def test_proxying(cluster):
    """
    Command proxying from follower to leader works
    """
    cluster.create(3)
    assert cluster.leader == 1
    with raises(ResponseError, match='MOVED'):
        assert cluster.node(2).raft_exec('SET', 'key', 'value') == b'OK'
    assert cluster.node(2).client.execute_command(
        'RAFT.CONFIG', 'SET', 'follower-proxy', 'yes') == b'OK'

    # Basic sanity
    assert cluster.node(2).raft_exec('SET', 'key', 'value') == b'OK'
    assert cluster.raft_exec('GET', 'key') == b'value'

    # Numeric values
    assert cluster.node(2).raft_exec('SADD', 'myset', 'a') == 1
    assert cluster.node(2).raft_exec('SADD', 'myset', 'b') == 1
    # Multibulk
    assert set(cluster.node(2).raft_exec('SMEMBERS', 'myset')) == set(
        [b'a', b'b'])
    # Nested multibulk
    assert set(cluster.node(2).raft_exec(
        'EVAL', 'return {{\'a\',\'b\',\'c\'}};', 0)[0]) == set(
            [b'a', b'b', b'c'])
    # Error
    with raises(ResponseError, match='WRONGTYPE'):
        cluster.node(2).raft_exec('INCR', 'myset')


def test_readonly_commands(cluster):
    """
    Test read-only command execution, which does not go through the Raft
    log.
    """
    cluster.create(3)
    assert cluster.leader == 1

    # Write something
    assert cluster.node(1).current_index() == 5
    assert cluster.node(1).raft_exec('SET', 'key', 'value') == b'OK'
    assert cluster.node(1).current_index() == 6

    # Read something, log should not grow
    assert cluster.node(1).raft_exec('GET', 'key') == b'value'
    assert cluster.node(1).current_index() == 6

    # Tear down cluster, reads should hang
    cluster.node(2).terminate()
    cluster.node(3).terminate()
    conn = cluster.node(1).client.connection_pool.get_connection(
        'RAFT', socket_timeout=1)
    conn.send_command('RAFT', 'GET', 'key')
    assert not conn.can_read(timeout=1)

    # Now configure non-quorum reads
    cluster.node(1).raft_config_set('quorum-reads', 'no')
    assert cluster.node(1).raft_exec('GET', 'key') == b'value'


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


def test_raftize(cluster):
    """
    Test raftize-all-commands mode.
    """

    r1 = cluster.add_node()
    try:
        assert r1.raft_config_set('raftize-all-commands', 'yes')
    except ResponseError:
        skip('Not supported on this Redis')
    assert r1.raft_info()['current_index'] == 1
    assert r1.client.execute_command('SET', 'key', 'value')
    assert r1.raft_info()['current_index'] == 2


def test_raftize_does_not_affect_lua(cluster):
    """
    Make sure raftize-all-commands does not affect Lua commands.
    """

    r1 = cluster.add_node()
    try:
        assert r1.raft_config_set('raftize-all-commands', 'yes')
    except ResponseError:
        skip('Not supported on this Redis')
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

def test_proxying_with_raftize(cluster):
    """
    Test follower proxy mode together with raftize enabled.

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
    assert cluster.node(1).client.execute_command(
        'RAFT.CONFIG', 'SET', 'raftize-all-commands', 'yes') == b'OK'
    assert cluster.node(2).client.execute_command(
        'RAFT.CONFIG', 'SET', 'raftize-all-commands', 'yes') == b'OK'
    assert cluster.node(3).client.execute_command(
        'RAFT.CONFIG', 'SET', 'raftize-all-commands', 'yes') == b'OK'

    assert cluster.node(1).raft_exec('RPUSH', 'list-a', 'x') == 1
    assert cluster.node(1).raft_exec('RPUSH', 'list-a', 'x') == 2
    assert cluster.node(1).raft_exec('RPUSH', 'list-a', 'x') == 3

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

    conn = cluster.node(1).client.connection_pool.get_connection('RAFT')
    conn.send_command('RAFT', 'INCR', 'key')

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
    cluster.node(1).raft_exec('SET', 'key', 'value')

    cluster.node(2).pause()
    cluster.node(3).pause()

    conn = cluster.node(1).client.connection_pool.get_connection('RAFT')
    conn.send_command('RAFT', 'GET', 'key')

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
    cluster.node(1).raft_exec('SET', 'key', 'value')

    cluster.node(2).pause()
    cluster.node(3).pause()

    conn = cluster.node(1).client.connection_pool.get_connection('RAFT')
    conn.send_command('RAFT', 'MULTI')
    assert conn.read_response() == b'OK'
    conn.send_command('RAFT', 'INCR', 'key')
    assert conn.read_response() == b'QUEUED'
    conn.send_command('RAFT', 'EXEC')

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
    cluster.node(1).raft_exec('SET', 'key', 'value')

    cluster.node(2).pause()
    cluster.node(3).pause()

    conn = cluster.node(1).client.connection_pool.get_connection('RAFT')
    conn.send_command('RAFT', 'MULTI')
    assert conn.read_response() == b'OK'
    conn.send_command('RAFT', 'GET', 'key')
    assert conn.read_response() == b'QUEUED'
    conn.send_command('RAFT', 'EXEC')

    cluster.node(1).pause()

    cluster.node(2).kill()
    cluster.node(3).kill()
    cluster.node(2).start()
    cluster.node(3).start()

    cluster.node(2).wait_for_election()
    cluster.node(1).resume()

    with raises(ResponseError, match='TIMEOUT'):
        assert conn.read_response() == None
