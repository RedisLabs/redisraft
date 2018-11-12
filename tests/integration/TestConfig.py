import sys
import redis
from nose.tools import eq_, ok_, assert_raises_regex, assert_regex
from test_tools import with_setup_args

import sandbox

def _setup():
    return [sandbox.Cluster()], {}

def _teardown(c):
    c.destroy()

@with_setup_args(_setup, _teardown)
def test_config_sanity(c):
    """
    Configuration sanity check.
    """

    r1 = c.add_node()
    r1.raft_config_set('raft-interval', 999)
    eq_(r1.raft_config_get('raft-interval'), {'raft-interval': '999'})

    r1.raft_config_set('request-timeout', 888)
    eq_(r1.raft_config_get('request-timeout'), {'request-timeout': '888'})

    r1.raft_config_set('election-timeout', 777)
    eq_(r1.raft_config_get('election-timeout'), {'election-timeout': '777'})

    r1.raft_config_set('reconnect-interval', 111)
    eq_(r1.raft_config_get('reconnect-interval'), {'reconnect-interval': '111'})

    r1.raft_config_set('max-log-entries', 11111)
    eq_(r1.raft_config_get('max-log-entries'), {'max-log-entries': '11111'})

    r1.raft_config_set('loglevel', 'debug')
    eq_(r1.raft_config_get('loglevel'), {'loglevel': 'debug'})

@with_setup_args(_setup, _teardown)
def test_config_startup_only_params(c):
    """
    Configuration startup-only params.
    """

    r1 = c.add_node()
    with assert_raises_regex(redis.ResponseError,
                             '.*only supported at load time'):
        r1.raft_config_set('id', 2)

    with assert_raises_regex(redis.ResponseError,
                             '.*only supported at load time'):
        r1.raft_config_set('raftlog', 'filename')

@with_setup_args(_setup, _teardown)
def test_invalid_configs(c):
    """
    Invalid configurations.
    """

    r1 = c.add_node()
    with assert_raises_regex(redis.ResponseError,
                             '.*invalid addr'):
        r1.raft_config_set('addr', 'host')

    with assert_raises_regex(redis.ResponseError,
                             '.*invalid addr'):
        r1.raft_config_set('addr', 'host:0')

    with assert_raises_regex(redis.ResponseError,
                             '.*invalid addr'):
        r1.raft_config_set('addr', 'host:99999')

    with assert_raises_regex(redis.ResponseError,
                             '.*invalid .*value'):
        r1.raft_config_set('request-timeout', 'nonint')
