"""
This file is part of RedisRaft.

Copyright (c) 2020 Redis Labs

RedisRaft is dual licensed under the GNU Affero General Public License version 3
(AGPLv3) or the Redis Source Available License (RSAL).
"""

import time
from pytest import raises, skip
from redis import ResponseError


def test_multi_exec_invalid_use(cluster):
    r1 = cluster.add_node()

    # EXEC without MULTI is not supported
    with raises(ResponseError, match='.*EXEC without MULTI'):
        r1.raft_exec('EXEC')

    # DISCARD without MULTI is not supported
    with raises(ResponseError, match='.*DISCARD without MULTI'):
        r1.raft_exec('DISCARD')

    # MULTI cannot be nested
    assert r1.raft_exec('MULTI') == b'OK'
    with raises(ResponseError, match='.*MULTI calls can not be nested'):
        r1.raft_exec('MULTI')


def test_multi_discard(cluster):
    """
    MULTI/DISCARD test
    """

    r1 = cluster.add_node()

    assert r1.raft_exec('MULTI')
    assert r1.raft_exec('INCR', 'key') == b'QUEUED'
    assert r1.raft_exec('INCR', 'key') == b'QUEUED'
    assert r1.raft_exec('DISCARD') == b'OK'

    assert r1.raft_exec('GET', 'key') is None


def test_multi_exec(cluster):
    """
    MULTI/EXEC test
    """
    r1 = cluster.add_node()

    # MULTI does not go itself to the log
    assert r1.raft_exec('MULTI')
    assert r1.raft_info()['current_index'] == 1

    # MULTI cannot be nested
    with raises(ResponseError, match='.*MULTI calls can not be nested'):
        r1.raft_exec('MULTI')

    # Commands are queued
    assert r1.raft_exec('INCR', 'key') == b'QUEUED'
    assert r1.raft_exec('INCR', 'key') == b'QUEUED'
    assert r1.raft_exec('INCR', 'key') == b'QUEUED'

    # More validations
    assert r1.raft_info()['current_index'] == 1
    assert r1.raft_exec('EXEC') == [1, 2, 3]
    assert r1.raft_info()['current_index'] == 2

    assert r1.raft_exec('GET', 'key') == b'3'


def test_multi_exec_state_cleanup(cluster):
    """
    MULTI/EXEC state is cleaned up on client disconnect
    """

    r1 = cluster.add_node()

    # Normal flow, no disconnect
    c1 = r1.client.connection_pool.get_connection('multi')
    c1.send_command('RAFT', 'MULTI')
    assert c1.read_response() == b'OK'

    c2 = r1.client.connection_pool.get_connection('multi')
    c2.send_command('RAFT', 'MULTI')
    assert c2.read_response() == b'OK'

    assert r1.raft_info()['clients_in_multi_state'] == 2

    c1.disconnect()
    c2.disconnect()

    time.sleep(1)   # Not ideal
    assert r1.raft_info()['clients_in_multi_state'] == 0


def test_multi_exec_proxying(cluster):
    """
    Proxy a MULTI/EXEC sequence
    """
    cluster.create(3)
    assert cluster.leader == 1
    assert cluster.node(2).client.execute_command(
        'RAFT.CONFIG', 'SET', 'follower-proxy', 'yes') == b'OK'

    # Basic sanity
    n2 = cluster.node(2)
    assert n2.raft_info()['current_index'] == 5
    assert n2.raft_exec('MULTI')
    assert n2.raft_exec('INCR', 'key') == b'QUEUED'
    assert n2.raft_exec('INCR', 'key') == b'QUEUED'
    assert n2.raft_exec('INCR', 'key') == b'QUEUED'
    assert n2.raft_exec('EXEC') == [1, 2, 3]
    assert n2.raft_info()['current_index'] == 6


def test_multi_exec_raftized(cluster):
    """
    MULTI/EXEC when raftize-all-commands is on.
    """

    r1 = cluster.add_node()
    try:
        assert r1.raft_config_set('raftize-all-commands', 'yes')
    except ResponseError:
        skip('Not supported on this Redis')

    # MULTI does not go itself to the log
    assert r1.client.execute_command('MULTI')
    assert r1.raft_info()['current_index'] == 1

    # Commands are queued
    assert r1.client.execute_command('INCR', 'key') == b'QUEUED'
    assert r1.client.execute_command('INCR', 'key') == b'QUEUED'
    assert r1.client.execute_command('INCR', 'key') == b'QUEUED'

    # More validations
    assert r1.raft_info()['current_index'] == 1
    assert r1.client.execute_command('EXEC') == [1, 2, 3]
    assert r1.raft_info()['current_index'] == 2

    assert r1.client.execute_command('GET', 'key') == b'3'


def test_multi_exec_with_watch(cluster):
    """
    MULTI/EXEC with WATCH
    """

    r1 = cluster.add_node()

    r1.client.execute_command('SET', 'watched-key', '1')

    c1 = r1.client.connection_pool.get_connection('c1')
    c1.send_command('WATCH', 'watched-key')
    assert c1.read_response() == b'OK'

    c2 = r1.client.connection_pool.get_connection('c2')
    c2.send_command('RAFT', 'SET', 'watched-key', '2')
    assert c2.read_response() == b'OK'

    c1.send_command('RAFT', 'MULTI')
    assert c1.read_response() == b'OK'
    c1.send_command('RAFT', 'SET', 'watched-key', '3')
    assert c1.read_response() == b'QUEUED'
    c1.send_command('RAFT', 'EXEC')
    assert c1.read_response() is None

    assert r1.client.execute_command('GET', 'watched-key') == b'2'


def test_multi_exec_with_disconnect(cluster):
    """
    MULTI/EXEC, client drops before EXEC.
    """

    r1 = cluster.add_node()

    c1 = r1.client.connection_pool.get_connection('c1')
    c2 = r1.client.connection_pool.get_connection('c2')

    # We use RAFT.DEBUG COMPACT with delay to make the Raft thread
    # busy and allow us to queue up several RaftReqs and disconnect in
    # time.
    # Note -- for compact to succeed we need at least one key.
    r1.client.execute_command('RAFT', 'SET', 'somekey', 'someval')

    c2.send_command('RAFT.DEBUG', 'COMPACT', '2')
    time.sleep(0.5)

    # While Raft thread is busy, pipeline a first non-MULTI request
    c1.send_command('RAFT', 'SET', 'test-key', '1')

    # Then pipeline a MULTI/EXEC which we expect to fail, because it
    # cannot determine CAS safety.  We also want to be sure no other
    # commands that follow get executed.
    c1.send_command('RAFT', 'MULTI')
    c1.send_command('RAFT', 'SET', 'test-key', '2')
    c1.send_command('RAFT', 'EXEC')
    c1.send_command('RAFT', 'SET', 'test-key', '3')
    c1.disconnect()

    # Wait for RAFT.DEBUG COMPACT
    assert c2.read_response() == b'OK'

    # Make sure SET succeeded and EXEC didn't.
    assert r1.client.execute_command('GET', 'test-key') == b'1'
