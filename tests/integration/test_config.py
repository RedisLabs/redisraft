"""
This file is part of RedisRaft.

Copyright (c) 2020-2021 Redis Ltd.

RedisRaft is licensed under the Redis Source Available License (RSAL).
"""
import logging

from redis import ResponseError
from pytest import raises

from .sandbox import RedisRaft


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
        r1.raft_config_set('external-sharding', 'yes')

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


def test_prevent_config_id_after_init(cluster):
    node = RedisRaft(1, cluster.base_port + 1, cluster.config)
    cluster.nodes[1] = node
    node.cleanup()
    node.start()
    assert node.raft_config_get("id")['id'] == '1'
    node.raft_config_set("id", 123)
    assert node.raft_config_get("id")['id'] == '123'
    node.cluster('init')
    with raises(ResponseError, match='only supported at before cluster init/join'):
        node.raft_config_set("id", "456")


def test_ignored_commands(cluster):
    """
    ignored commands
    """
    # create a non initialized single node cluster
    cluster.nodes = {1: RedisRaft(1, cluster.base_port + 1,
              config=cluster.config,
              raft_args={"ignored-commands": "ignored,mycommand"},
              cluster_id=cluster.cluster_id)}

    cluster.node(1).cleanup()
    cluster.node(1).start()

    # ignored commands should give a non existent command redis error
    with raises(ResponseError, match="unknown command"):
        cluster.node(1).client.execute_command("ignored", "test", "command")
    with raises(ResponseError, match="unknown command"):
        cluster.node(1).client.execute_command("mycommand", "to", "ignore")

    # while not ignored commands should give an uninitialized cluster error
    with raises(ResponseError, match='NOCLUSTER No Raft Cluster'):
        cluster.node(1).client.execute_command("set", "a", "b")