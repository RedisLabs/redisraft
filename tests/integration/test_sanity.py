import redis
from nose import SkipTest
from nose.tools import (eq_, ok_, assert_raises_regex, assert_not_equal)

from test_tools import with_setup_args
import sandbox


def _setup():
    return [sandbox.Cluster()], {}


def _teardown(c):
    c.destroy()


@with_setup_args(_setup, _teardown)
def test_add_node_as_a_single_leader(c):
    """
    Single node becomes a leader
    """
    # Do some basic sanity
    r1 = c.add_node()
    ok_(r1.raft_exec('SET', 'key', 'value'))
    eq_(r1.raft_info()['current_index'], 2)


@with_setup_args(_setup, _teardown)
def test_node_joins_and_gets_data(c):
    """
    Node joins and gets data
    """
    r1 = c.add_node()
    eq_(r1.raft_exec('SET', 'key', 'value'), b'OK')
    r2 = c.add_node()
    r2.wait_for_election()
    eq_(r2.raft_info().get('leader_id'), 1)
    eq_(r2.client.get('key'), b'value')

    # Also validate -MOVED as expected
    with assert_raises_regex(redis.ResponseError, 'MOVED'):
        eq_(r2.raft_exec('SET', 'key', 'value'), None)


@with_setup_args(_setup, _teardown)
def test_single_node_log_is_reapplied(c):
    """Single node log is reapplied on startup"""
    r1 = c.add_node()
    ok_(r1.raft_exec('SET', 'key', 'value'))
    r1.restart()
    r1.wait_for_election()
    eq_(r1.raft_info().get('leader_id'), 1)
    r1.wait_for_log_applied()
    eq_(r1.client.get('key'), b'value')


@with_setup_args(_setup, _teardown)
def test_reelection_basic_flow(c):
    """
    Basic reelection flow
    """
    c.create(3)
    eq_(c.leader, 1)
    eq_(c.raft_exec('SET', 'key', 'value'), b'OK')
    c.node(1).terminate()
    c.node(2).wait_for_election()
    eq_(c.raft_exec('SET', 'key2', 'value2'), b'OK')
    c.exec_all('GET', 'key2')


@with_setup_args(_setup, _teardown)
def test_proxying(c):
    """
    Command proxying from follower to leader works
    """
    c.create(3)
    eq_(c.leader, 1)
    with assert_raises_regex(redis.ResponseError, 'MOVED'):
        eq_(c.node(2).raft_exec('SET', 'key', 'value'), b'OK')
    eq_(c.node(2).client.execute_command('RAFT.CONFIG', 'SET',
                                         'follower-proxy', 'yes'), b'OK')

    # Basic sanity
    eq_(c.node(2).raft_exec('SET', 'key', 'value'), b'OK')
    eq_(c.raft_exec('GET', 'key'), b'value')

    # Numeric values
    eq_(c.node(2).raft_exec('SADD', 'myset', 'a'), 1)
    eq_(c.node(2).raft_exec('SADD', 'myset', 'b'), 1)
    # Multibulk
    eq_(set(c.node(2).raft_exec('SMEMBERS', 'myset')), set([b'a', b'b']))
    # Nested multibulk
    eq_(set(c.node(2).raft_exec(
        'EVAL', 'return {{\'a\',\'b\',\'c\'}};', 0)[0]),
        set([b'a', b'b', b'c']))
    # Error
    with assert_raises_regex(redis.ResponseError, 'WRONGTYPE'):
        c.node(2).raft_exec('INCR', 'myset')


@with_setup_args(_setup, _teardown)
def test_readonly_commands(c):
    """
    Test read-only command execution, which does not go through the Raft
    log.
    """
    c.create(3)
    eq_(c.leader, 1)

    # Write something
    eq_(c.node(1).current_index(), 5)
    eq_(c.node(1).raft_exec('SET', 'key', 'value'), b'OK')
    eq_(c.node(1).current_index(), 6)

    # Read something, log should not grow
    eq_(c.node(1).raft_exec('GET', 'key'), b'value')
    eq_(c.node(1).current_index(), 6)

    # Tear down cluster, reads should hang
    c.node(2).terminate()
    c.node(3).terminate()
    conn = c.node(1).client.connection_pool.get_connection('RAFT',
                                                           socket_timeout=1)
    conn.send_command('RAFT', 'GET', 'key')
    eq_(conn.can_read(timeout=1), False)

    # Now configure non-quorum reads
    c.node(1).raft_config_set('quorum-reads', 'no')
    eq_(c.node(1).raft_exec('GET', 'key'), b'value')


@with_setup_args(_setup, _teardown)
def test_auto_ids(c):
    """
    Test automatic assignment of ids.
    """

    # -- Test auto id on create --
    node1 = sandbox.RedisRaft(1, 5000, use_id_arg=False)
    node1.start()
    c.add_initialized_node(node1)   # Just to make sure it gets destroyed

    # No id initially
    eq_(node1.raft_info()['node_id'], 0)

    # Create cluster, id should be generated
    node1.cluster('init')
    assert_not_equal(node1.raft_info()['node_id'], 0)

    # -- Test auto id on join --
    node2 = sandbox.RedisRaft(2, 5001, use_id_arg=False)
    node2.start()
    c.add_initialized_node(node2)   # Just to make sure it gets destroyed

    eq_(node2.raft_info()['node_id'], 0)
    node2.cluster('join', '127.0.0.1:5000')
    node2.wait_for_node_voting()
    assert_not_equal(node2.raft_info()['node_id'], 0)

    # -- Test recovery of id from log after restart --
    _id = node2.raft_info()['node_id']
    node2.restart()
    eq_(_id, node2.raft_info()['node_id'])


@with_setup_args(_setup, _teardown)
def test_raftize(c):
    """
    Test raftize-all-commands mode.
    """

    r1 = c.add_node()
    try:
        ok_(r1.raft_config_set('raftize-all-commands', 'yes'))
    except redis.ResponseError:
        raise SkipTest('Not supported on this Redis')
    eq_(r1.raft_info()['current_index'], 1)
    ok_(r1.client.execute_command('SET', 'key', 'value'))
    eq_(r1.raft_info()['current_index'], 2)
