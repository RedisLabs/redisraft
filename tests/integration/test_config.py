"""
This file is part of RedisRaft.

Copyright (c) 2020 Redis Labs

RedisRaft is dual licensed under the GNU Affero General Public License version 3
(AGPLv3) or the Redis Source Available License (RSAL).
"""

from redis import ResponseError
from pytest import raises


def test_config_sanity(cluster):
    """
    Configuration sanity check.
    """

    r1 = cluster.add_node()
    r1.raft_config_set('raft-interval', 999)
    assert r1.raft_config_get('raft-interval') == {'raft-interval': '999'}

    r1.raft_config_set('request-timeout', 888)
    assert r1.raft_config_get('request-timeout') == {'request-timeout': '888'}

    r1.raft_config_set('election-timeout', 777)
    assert (r1.raft_config_get('election-timeout') ==
            {'election-timeout': '777'})

    r1.raft_config_set('reconnect-interval', 111)
    assert (r1.raft_config_get('reconnect-interval') ==
            {'reconnect-interval': '111'})

    r1.raft_config_set('raft-log-max-file-size', '64mb')
    assert (r1.raft_config_get('raft-log-max-file-size') ==
            {'raft-log-max-file-size': '64MB'})

    r1.raft_config_set('loglevel', 'debug')
    assert r1.raft_config_get('loglevel') == {'loglevel': 'debug'}


def test_config_startup_only_params(cluster):
    """
    Configuration startup-only params.
    """

    r1 = cluster.add_node()
    with raises(ResponseError, match='.*only supported at load time'):
        r1.raft_config_set('id', 2)

    with raises(ResponseError, match='.*only supported at load time'):
        r1.raft_config_set('raft-log-filename', 'filename')


def test_invalid_configs(cluster):
    """
    Invalid configurations.
    """

    r1 = cluster.add_node()
    with raises(ResponseError, match='.*invalid.*'):
        r1.raft_config_set('addr', 'host')

    with raises(ResponseError, match='.*invalid.*'):
        r1.raft_config_set('addr', 'host:0')

    with raises(ResponseError, match='.*invalid.*'):
        r1.raft_config_set('addr', 'host:99999')

    with raises(ResponseError, match='.*invalid .*value'):
        r1.raft_config_set('request-timeout', 'nonint')
