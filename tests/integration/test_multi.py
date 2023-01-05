"""
Copyright Redis Ltd. 2020 - present
Licensed under your choice of the Redis Source Available License 2.0 (RSALv2)
or the Server Side Public License v1 (SSPLv1).
"""

from pytest import raises
from redis.exceptions import ExecAbortError, ResponseError

from .sandbox import RawConnection


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
    assert r1.info()['raft_current_index'] == 2

    # MULTI cannot be nested
    with raises(ResponseError, match='.*MULTI calls can not be nested'):
        conn.execute('MULTI')

    # Commands are queued
    assert conn.execute('INCR', 'key') == b'QUEUED'
    assert conn.execute('INCR', 'key') == b'QUEUED'
    assert conn.execute('INCR', 'key') == b'QUEUED'

    # More validations
    assert r1.info()['raft_current_index'] == 2
    assert conn.execute('EXEC') == [1, 2, 3]
    assert r1.info()['raft_current_index'] == 3

    assert conn.execute('GET', 'key') == b'3'


def test_multi_exec_proxying(cluster):
    """
    Proxy a MULTI/EXEC sequence
    """
    cluster.create(3)
    assert cluster.leader == 1
    assert cluster.node(2).client.execute_command(
        'CONFIG', 'SET', 'raft.follower-proxy', 'yes') == b'OK'

    # Basic sanity
    n2 = cluster.node(2)
    assert n2.info()['raft_current_index'] == 6
    conn = RawConnection(n2.client)

    assert conn.execute('MULTI') == b'OK'
    assert conn.execute('INCR', 'key') == b'QUEUED'
    assert conn.execute('INCR', 'key') == b'QUEUED'
    assert conn.execute('INCR', 'key') == b'QUEUED'
    assert conn.execute('EXEC') == [1, 2, 3]
    assert n2.info()['raft_current_index'] == 7


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


def test_multi_with_maxmemory(cluster):
    """
    MULTI/EXEC should fail when used memory is over the 'maxmemory' config.
    """
    cluster.create(3)

    val = '1' * 2000

    node = cluster.leader_node()
    node.execute('set', 'key1', val)
    node.execute('config', 'set', 'maxmemory', 100)

    # Test MULTI - COMMAND - EXEC
    assert node.execute('MULTI') == b'OK'
    with raises(ResponseError, match='OOM command not allowed'):
        node.execute('GET', 'key1')
    with raises(ResponseError, match='Transaction discarded'):
        node.execute('EXEC')

    # Test MULTI - COMMAND - DISCARD
    assert node.execute('MULTI') == b'OK'
    with raises(ResponseError, match='OOM command not allowed'):
        node.execute('GET', 'key1')
    assert node.execute('DISCARD') == b'OK'

    # Clear OOM
    node.execute('config', 'set', 'maxmemory', 100000000)
    assert node.execute('MULTI') == b'OK'
    assert node.execute('GET', 'key1') == b'QUEUED'
    assert node.execute('EXEC') == [val.encode()]


def test_multi_with_acl(cluster):
    """
    MULTI/EXEC should fail when used a command is ACL denied
    """

    cluster.create(3)
    node = cluster.leader_node()
    node.execute('set', 'key1', 1)
    node.execute('acl', 'setuser', 'default', 'resetkeys', '(+set', '~key*)',
                 '(+get', '~key*)')

    conn = RawConnection(cluster.node(1).client)

    assert conn.execute('multi') == b'OK'
    assert conn.execute('get', 'key1') == b'QUEUED'
    assert conn.execute('set', 'key1', 2) == b'QUEUED'
    assert conn.execute('exec') == [b'1', b'OK']

    assert conn.execute('get', 'key1') == b'2'

    assert conn.execute('multi') == b'OK'
    assert conn.execute('set', 'key1', 3) == b'QUEUED'

    with raises(ResponseError, match='No permissions to access a key'):
        conn.execute('set', 'abc', 1)

    assert conn.execute('set', 'key2', 1) == b'QUEUED'

    msg = 'Transaction discarded because of previous errors.'
    with raises(ResponseError, match=msg):
        conn.execute('exec')

    assert conn.execute('multi') == b'OK'
    assert conn.execute('eval', """
        redis.call('set','abc', 3);
        return 1234;
        """, 0) == b'QUEUED'
    ret = conn.execute('exec')

    assert isinstance(ret, list)
    assert len(ret) == 1
    assert isinstance(ret[0], ResponseError)

    msg = str(ret[0])
    assert 'ACL failure in script: No permissions to access a key' in msg


def test_watch_within_multi(cluster):
    """
    MULTI/EXEC doesn't allow WATCH within multi, but EXEC still works
    """
    cluster.create(3)
    node = cluster.leader_node()
    node.execute('set', 'key1', 1)

    conn = RawConnection(cluster.node(1).client)

    assert conn.execute('multi') == b'OK'
    assert conn.execute('get', 'key1') == b'QUEUED'
    with raises(ResponseError, match='WATCH inside MULTI is not allowed'):
        conn.execute('watch', 'x')
    assert conn.execute('get', 'key1') == b'QUEUED'
    assert conn.execute('exec') == [b'1', b'1']
