import redis
from nose.tools import (ok_, eq_, assert_raises_regex)
from test_tools import with_setup_args

import sandbox


def _setup():
    return [sandbox.Cluster()], {}


def _teardown(c):
    c.destroy()


@with_setup_args(_setup, _teardown)
def test_multi(c):
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
        ok_(r1.raft_exec('MULTI'))

    # Commands are queued
    eq_(r1.raft_exec('INCR', 'key'), b'QUEUED')
    eq_(r1.raft_exec('INCR', 'key'), b'QUEUED')
    eq_(r1.raft_exec('INCR', 'key'), b'QUEUED')

    # More validations
    eq_(r1.raft_info()['current_index'], 1)
    eq_(r1.raft_exec('EXEC'), [1, 2, 3])
    eq_(r1.raft_info()['current_index'], 2)

    eq_(r1.raft_exec('GET', 'key'), b'3')
