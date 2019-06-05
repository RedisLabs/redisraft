import time

import redis
from nose import SkipTest
from nose.tools import (ok_, eq_, assert_raises_regex)
from test_tools import with_setup_args

import sandbox


def _setup():
    return [sandbox.Cluster()], {}


def _teardown(c):
    c.destroy()


@with_setup_args(_setup, _teardown)
def test_multi_exec_invalid_use(c):
    r1 = c.add_node()

    # EXEC without MULTI is not supported
    with assert_raises_regex(redis.ResponseError,
                             ''):
        r1.raft_exec('.*EXEC without MULTI')

    # DISCARD without MULTI is not supported
    with assert_raises_regex(redis.ResponseError,
                             ''):
        r1.raft_exec('.*DISCARD without MULTI')

    # MULTI cannot be nested
    eq_(r1.raft_exec('MULTI'), b'OK')
    with assert_raises_regex(redis.ResponseError,
                             '.*MULTI calls can not be nested'):
        r1.raft_exec('MULTI')


@with_setup_args(_setup, _teardown)
def test_multi_discard(c):
    """
    MULTI/DISCARD test
    """

    r1 = c.add_node()

    ok_(r1.raft_exec('MULTI'))
    eq_(r1.raft_exec('INCR', 'key'), b'QUEUED')
    eq_(r1.raft_exec('INCR', 'key'), b'QUEUED')
    eq_(r1.raft_exec('DISCARD'), b'OK')

    eq_(r1.raft_exec('GET', 'key'), None)


@with_setup_args(_setup, _teardown)
def test_multi_exec(c):
    """
    MULTI/EXEC test
    """
    r1 = c.add_node()

    # MULTI does not go itself to the log
    ok_(r1.raft_exec('MULTI'))
    eq_(r1.raft_info()['current_index'], 1)

    # MULTI cannot be nested
    with assert_raises_regex(redis.ResponseError,
                             '.*MULTI calls can not be nested'):
        r1.raft_exec('MULTI')

    # Commands are queued
    eq_(r1.raft_exec('INCR', 'key'), b'QUEUED')
    eq_(r1.raft_exec('INCR', 'key'), b'QUEUED')
    eq_(r1.raft_exec('INCR', 'key'), b'QUEUED')

    # More validations
    eq_(r1.raft_info()['current_index'], 1)
    eq_(r1.raft_exec('EXEC'), [1, 2, 3])
    eq_(r1.raft_info()['current_index'], 2)

    eq_(r1.raft_exec('GET', 'key'), b'3')


@with_setup_args(_setup, _teardown)
def test_multi_exec_state_cleanup(c):
    """
    MULTI/EXEC state is cleaned up on client disconnect
    """

    r1 = c.add_node()

    # Normal flow, no disconnect
    c1 = r1.client.connection_pool.get_connection('multi')
    c1.send_command('RAFT', 'MULTI')
    eq_(c1.read_response(), b'OK')

    c2 = r1.client.connection_pool.get_connection('multi')
    c2.send_command('RAFT', 'MULTI')
    eq_(c2.read_response(), b'OK')

    eq_(r1.raft_info()['clients_in_multi_state'], 2)

    c1.disconnect()
    c2.disconnect()

    time.sleep(1)   # Not ideal
    eq_(r1.raft_info()['clients_in_multi_state'], 0)


@with_setup_args(_setup, _teardown)
def test_multi_exec_proxying(c):
    """
    Proxy a MULTI/EXEC sequence
    """
    c.create(3)
    eq_(c.leader, 1)
    eq_(c.node(2).client.execute_command('RAFT.CONFIG', 'SET',
                                         'follower-proxy', 'yes'), b'OK')

    # Basic sanity
    eq_(c.node(2).raft_info()['current_index'], 5)
    ok_(c.node(2).raft_exec('MULTI'))
    eq_(c.node(2).raft_exec('INCR', 'key'), b'QUEUED')
    eq_(c.node(2).raft_exec('INCR', 'key'), b'QUEUED')
    eq_(c.node(2).raft_exec('INCR', 'key'), b'QUEUED')
    eq_(c.node(2).raft_exec('EXEC'), [1, 2, 3])
    eq_(c.node(2).raft_info()['current_index'], 6)


@with_setup_args(_setup, _teardown)
def test_multi_exec_raftized(c):
    """
    MULTI/EXEC when raftize-all-commands is on.
    """

    r1 = c.add_node()
    try:
        ok_(r1.raft_config_set('raftize-all-commands', 'yes'))
    except redis.ResponseError:
        raise SkipTest('Not supported on this Redis')

    # MULTI does not go itself to the log
    ok_(r1.client.execute_command('MULTI'))
    eq_(r1.raft_info()['current_index'], 1)

    # Commands are queued
    eq_(r1.client.execute_command('INCR', 'key'), b'QUEUED')
    eq_(r1.client.execute_command('INCR', 'key'), b'QUEUED')
    eq_(r1.client.execute_command('INCR', 'key'), b'QUEUED')

    # More validations
    eq_(r1.raft_info()['current_index'], 1)
    eq_(r1.client.execute_command('EXEC'), [1, 2, 3])
    eq_(r1.raft_info()['current_index'], 2)

    eq_(r1.client.execute_command('GET', 'key'), b'3')
