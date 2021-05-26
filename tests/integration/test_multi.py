"""
This file is part of RedisRaft.

Copyright (c) 2020 Redis Labs

RedisRaft is dual licensed under the GNU Affero General Public License version 3
(AGPLv3) or the Redis Source Available License (RSAL).
"""

import time
from pytest import raises, skip
from redis.exceptions import ExecAbortError, ResponseError

class RawConnection(object):
    """
    Implement a simply way of executing a Redis command and return the raw
    unprocessed reply (unlike redis-py's execute_command() which applies some
    command-specific parsing.
    """

    def __init__(self, client):
        self._conn = client.connection_pool.get_connection('raw-connection')

    def execute(self, *cmd):
        self._conn.send_command(*cmd)
        return self._conn.read_response()

def test_multi_exec_invalid_use(cluster):
    r1 = cluster.add_node()

    # EXEC without MULTI is not supported
    with raises(ResponseError, match='.*EXEC without MULTI'):
        r1.client.execute_command('EXEC')

    # DISCARD without MULTI is not supported
    with raises(ResponseError, match='.*DISCARD without MULTI'):
        r1.client.execute_command('DISCARD')

    # MULTI cannot be nested
    assert r1.client.execute_command('MULTI') == b'OK'
    with raises(ResponseError, match='.*MULTI calls can not be nested'):
        r1.client.execute_command('MULTI')


def test_multi_discard(cluster):
    """
    MULTI/DISCARD test
    """

    r1 = cluster.add_node()
    conn = RawConnection(r1.client)

    assert conn.execute('MULTI') == b'OK'
    assert conn.execute('INCR', 'key') == b'QUEUED'
    assert conn.execute('INCR', 'key') == b'QUEUED'
    assert conn.execute('DISCARD') == b'OK'
    assert conn.execute('GET', 'key') is None


def test_multi_exec(cluster):
    """
    MULTI/EXEC test
    """
    r1 = cluster.add_node()
    conn = RawConnection(r1.client)

    # MULTI does not go itself to the log
    assert conn.execute('MULTI') == b'OK'
    assert r1.raft_info()['current_index'] == 1

    # MULTI cannot be nested
    with raises(ResponseError, match='.*MULTI calls can not be nested'):
        conn.execute('MULTI')

    # Commands are queued
    assert conn.execute('INCR', 'key') == b'QUEUED'
    assert conn.execute('INCR', 'key') == b'QUEUED'
    assert conn.execute('INCR', 'key') == b'QUEUED'

    # More validations
    assert r1.raft_info()['current_index'] == 1
    assert conn.execute('EXEC') == [1, 2, 3]
    assert r1.raft_info()['current_index'] == 2

    assert conn.execute('GET', 'key') == b'3'


def test_multi_exec_state_cleanup(cluster):
    """
    MULTI/EXEC state is cleaned up on client disconnect
    """

    r1 = cluster.add_node()

    # Normal flow, no disconnect
    c1 = r1.client.connection_pool.get_connection('multi')
    c1.send_command('MULTI')
    assert c1.read_response() == b'OK'

    c2 = r1.client.connection_pool.get_connection('multi')
    c2.send_command('MULTI')
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
    conn = RawConnection(n2.client)

    assert conn.execute('MULTI') == b'OK'
    assert conn.execute('INCR', 'key') == b'QUEUED'
    assert conn.execute('INCR', 'key') == b'QUEUED'
    assert conn.execute('INCR', 'key') == b'QUEUED'
    assert conn.execute('EXEC') == [1, 2, 3]
    assert n2.raft_info()['current_index'] == 6


def test_multi_exec_with_watch(cluster):
    """
    MULTI/EXEC with WATCH
    """

    r1 = cluster.add_node()

    r1.client.set('watched-key', '1')

    c1 = r1.client.connection_pool.get_connection('c1')
    c1.send_command('WATCH', 'watched-key')
    assert c1.read_response() == b'OK'

    c2 = r1.client.connection_pool.get_connection('c2')
    c2.send_command('SET', 'watched-key', '2')
    assert c2.read_response() == b'OK'

    c1.send_command('MULTI')
    assert c1.read_response() == b'OK'
    c1.send_command('SET', 'watched-key', '3')
    assert c1.read_response() == b'QUEUED'
    c1.send_command('EXEC')
    assert c1.read_response() is None

    assert r1.client.get('watched-key') == b'2'


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
    r1.client.set('somekey', 'someval')

    c2.send_command('RAFT.DEBUG', 'COMPACT', '2')
    time.sleep(0.5)

    # While Raft thread is busy, pipeline a first non-MULTI request
    c1.send_command('SET', 'test-key', '1')

    # Then pipeline a MULTI/EXEC which we expect to fail, because it
    # cannot determine CAS safety.  We also want to be sure no other
    # commands that follow get executed.
    c1.send_command('MULTI')
    c1.send_command('SET', 'test-key', '2')
    c1.send_command('EXEC')
    c1.send_command('SET', 'test-key', '3')
    c1.disconnect()

    # Wait for RAFT.DEBUG COMPACT
    assert c2.read_response() == b'OK'

    # Make sure SET succeeded and EXEC didn't.
    assert r1.client.get('test-key') == b'1'


def test_multi_mixed_ro_rw(cluster_factory):
    """
    MULTI/EXEC with mixed read-only and read-write commands.
    """

    cluster = cluster_factory().create(3)
    c1 = cluster.node(1).client

    # Perform a mixed read-only/read-write MULTI/EXEC block
    txn = c1.pipeline(transaction=True)
    txn.set('mykey', 'myval')
    txn.get('mykey')
    result = txn.execute()
    assert result[0]
    assert result[1] == b'myval'

    # Fail over and make sure it was propagated
    cluster.node(1).terminate()
    cluster.node(2).wait_for_election()

    assert cluster.execute('GET', 'mykey') == b'myval'


def test_multi_with_unsupported_commands(cluster_factory):
    """
    MULTI/EXEC with unsupported commands should fail.
    """

    cluster = cluster_factory().create(3)
    c1 = cluster.node(1).client.connection_pool.get_connection('client')

    # Initiate MULTI and send a valid command
    c1.send_command('MULTI')
    assert c1.read_response() == b'OK'
    c1.send_command('SET', 'mykey', 'myval')
    assert c1.read_response() == b'QUEUED'

    # Send an invalid command: should fail immediately, remain in MULTI
    # state but flag the transaction.
    c1.send_command('DEBUG', 'HELP')
    with raises(ResponseError, match='.*not supported.*'):
        c1.read_response()

    # Validate we're still in MULTI state
    c1.send_command('SET', 'myotherkey', 'myotherval')
    assert c1.read_response() == b'QUEUED'

    # Validate transaction is flagged
    c1.send_command('EXEC')
    with raises(ExecAbortError):
        c1.read_response()