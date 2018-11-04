import sys
import time
import sandbox
import redis
from nose.tools import eq_, ok_, assert_raises_regex
from test_tools import with_setup_args

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
def test_node_join_iterates_all_addrs(c):
    """
    Node join iterates all addresses.
    """
    r1 = c.add_node()
    eq_(r1.raft_exec('SET', 'key', 'value'), b'OK')
    r2 = c.add_node(cluster_setup=False)
    r2.start()
    eq_(r2.cluster('join', 'localhost:1', 'localhost:2',
                        'localhost:{}'.format(c.node_ports()[0])), b'OK')
    r2.wait_for_election()

@with_setup_args(_setup, _teardown)
def test_node_join_redirects_to_leader(c):
    """
    Node join can redirect to leader.
    """
    r1 = c.add_node()
    eq_(r1.raft_exec('SET', 'key', 'value'), b'OK')
    r2 = c.add_node()
    r2.wait_for_election()
    r3 = c.add_node(cluster_setup=False)
    r3.start()
    result = r3.cluster('join', 'localhost:{}'.format(r2.port))
    r3.wait_for_election()

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
    res = c.exec_all('GET', 'key2')

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
    eq_(set(c.node(2).raft_exec('EVAL', 'return {{\'a\',\'b\',\'c\'}};', 0)[0]),
        set([b'a', b'b', b'c']))
    # Error
    with assert_raises_regex(redis.ResponseError, 'WRONGTYPE'):
        c.node(2).raft_exec('INCR', 'myset')
