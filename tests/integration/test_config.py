"""
Copyright Redis Ltd. 2020 - present
Licensed under your choice of the Redis Source Available License 2.0 (RSALv2)
or the Server Side Public License v1 (SSPLv1).
"""

import pytest
from redis import ResponseError
from pytest import raises


def test_config_sanity(cluster):
    """
    Configuration sanity check.
    """

    r1 = cluster.add_node()

    def verify(name, value):
        r1.config_set(name, value)
        assert r1.config_get(name) == {name: str(value)}

    verify('raft.periodic-interval', 999)
    verify('raft.request-timeout', 999)
    verify('raft.election-timeout', 999)
    verify('raft.connection-timeout', 999)
    verify('raft.join-timeout', 999)
    verify('raft.response-timeout', 999)
    verify('raft.proxy-response-timeout', 999)
    verify('raft.reconnect-interval', 999)
    verify('raft.shardgroup-update-interval', 999)
    verify('raft.append-req-max-count', 999)
    verify('raft.append-req-max-size', 999)
    verify('raft.snapshot-req-max-count', 999)
    verify('raft.snapshot-req-max-size', 999)
    verify('raft.log-max-cache-size', 999)
    verify('raft.log-max-file-size', 999)
    verify('raft.scan-size', 999)
    verify('raft.log-delay-apply', 999)
    verify('raft.snapshot-delay', 999)

    verify('raft.log-fsync', 'yes')
    verify('raft.log-fsync', 'no')
    verify('raft.follower-proxy', 'yes')
    verify('raft.follower-proxy', 'no')
    verify('raft.quorum-reads', 'yes')
    verify('raft.quorum-reads', 'no')
    verify('raft.sharding', 'yes')
    verify('raft.sharding', 'no')
    verify('raft.tls-enabled', 'no')
    verify('raft.log-disable-apply', 'yes')
    verify('raft.log-disable-apply', 'no')
    verify('raft.snapshot-fail', 'yes')
    verify('raft.snapshot-fail', 'no')
    verify('raft.snapshot-disable', 'yes')
    verify('raft.snapshot-disable', 'no')
    verify('raft.snapshot-disable-load', 'yes')
    verify('raft.snapshot-disable-load', 'no')

    verify('raft.ignored-commands', 'get')
    verify('raft.cluster-user', 'username')
    verify('raft.cluster-password', 'password')

    verify('raft.loglevel', 'debug')
    verify('raft.loglevel', 'verbose')
    verify('raft.loglevel', 'notice')
    verify('raft.loglevel', 'warning')

    verify('raft.migration-debug', 'none')
    verify('raft.migration-debug', 'fail-connect')
    verify('raft.migration-debug', 'fail-import')
    verify('raft.migration-debug', 'fail-unlock')

    verify('raft.trace', 'node conn')


@pytest.mark.skipif("config.getoption('tls')")
def test_config_tls_enabled_without_tls_build(cluster):
    """
    Enabling tls returns error if RedisRaft was built without TLS support
    """

    r1 = cluster.add_node()

    # Disable TLS should work
    r1.config_set('raft.tls-enabled', 'no')

    with raises(ResponseError, match='Build RedisRaft with TLS'):
        r1.config_set('raft.tls-enabled', 'yes')


@pytest.mark.skipif("not config.getoption('tls')")
def test_config_tls_enabled_with_tls_build(cluster):
    """
    Enabling/disabling tls returns ok if RedisRaft was built with TLS support
    """
    cluster.create(3)
    r1 = cluster.add_node()

    def verify(name, value):
        r1.config_set(name, value)
        assert r1.config_get(name) == {name: str(value)}

    # Enable/Disable TLS should work
    verify('raft.tls-enabled', 'no')
    verify('raft.tls-enabled', 'yes')
    verify('raft.tls-enabled', 'no')
    verify('raft.tls-enabled', 'yes')
    verify('raft.tls-enabled', 'no')
    verify('raft.tls-enabled', 'yes')


def test_config_before_init(cluster):
    """
    Test configuration parameters that are only allowed before init/join
    """

    node = cluster.add_node(cluster_setup=False)
    node.start()

    def verify(name, value):
        node.config_set(name, value)
        assert node.config_get(name) == {name: str(value)}

    def verify_failure(name, value):
        with raises(ResponseError, match='not allowed after init/join'):
            node.config_set(name, value)

    verify('raft.id', '100000')
    verify('raft.addr', '80.80.80.80:80')
    verify('raft.external-sharding', 'yes')
    verify('raft.log-filename', 'newfile.db')
    verify('raft.slot-config', '0:10000')

    node.cluster('init')

    verify_failure('raft.id', '50000')
    verify_failure('raft.addr', '199.10.10.10:2220')
    verify_failure('raft.external-sharding', 'no')
    verify_failure('raft.log-filename', 'anotherfile.db')
    verify_failure('raft.slot-config', '10:20000')


def test_config_args(cluster):
    """
    Test configuration via module arguments
    """

    raft_args = {'periodic-interval':          8001,
                 'request-timeout':            8002,
                 'election-timeout':           8003,
                 'connection-timeout':         8004,
                 'join-timeout':               8005,
                 'response-timeout':           8006,
                 'proxy-response-timeout':     8007,
                 'reconnect-interval':         8008,
                 'shardgroup-update-interval': 8009,
                 'append-req-max-count':       8010,
                 'append-req-max-size':        8099,
                 'snapshot-req-max-count':     8111,
                 'snapshot-req-max-size':      8112,
                 'log-max-cache-size':         8011,
                 'log-max-file-size':          8012,
                 'scan-size':                  8013,
                 'log-delay-apply':            8014,
                 'snapshot-delay':             8015,
                 'log-fsync':                  'no',
                 'follower-proxy':             'yes',
                 'quorum-reads':               'no',
                 'sharding':                   'yes',
                 'external-sharding':          'yes',
                 'tls-enabled':                'no',
                 'log-disable-apply':          'yes',
                 'snapshot-fail':              'yes',
                 'snapshot-disable':           'yes',
                 'snapshot-disable-load':      'yes',
                 'log-filename':               'filename8001.log',
                 'addr':                       '80.80.80.80:1002',
                 'slot-config':                '0:8001',
                 'ignored-commands':           'cmd1,cmd2',
                 'cluster-user':               'user1',
                 'cluster-password':           'password1',
                 'loglevel':                   'warning',
                 'migration-debug':            'fail-unlock',
                 'trace':                      'raftlib'}

    r1 = cluster.add_node(raft_args, cluster_setup=False)
    r1.start()

    for key, value in raft_args.items():
        assert r1.config_get('raft.' + key) == {'raft.' + key: str(value)}


def test_invalid_configs(cluster):
    """
    Invalid configurations.
    """

    r1 = cluster.add_node()

    def verify_failure(name, value):
        with raises(ResponseError, match='.*failed.*'):
            r1.config_set(name, value)

    verify_failure('raft.periodic-interval', 0)
    verify_failure('raft.periodic-interval', -1)
    verify_failure('raft.request-timeout', 0)
    verify_failure('raft.request-timeout', -1)
    verify_failure('raft.election-timeout', 0)
    verify_failure('raft.election-timeout', -1)
    verify_failure('raft.connection-timeout', 0)
    verify_failure('raft.connection-timeout', -1)
    verify_failure('raft.join-timeout', 0)
    verify_failure('raft.join-timeout', -1)
    verify_failure('raft.response-timeout', 0)
    verify_failure('raft.response-timeout', -1)
    verify_failure('raft.proxy-response-timeout', 0)
    verify_failure('raft.proxy-response-timeout', -1)
    verify_failure('raft.reconnect-interval', 0)
    verify_failure('raft.reconnect-interval', -1)
    verify_failure('raft.shardgroup-update-interval', 0)
    verify_failure('raft.shardgroup-update-interval', -1)
    verify_failure('raft.append-req-max-count', 0)
    verify_failure('raft.append-req-max-count', -1)
    verify_failure('raft.append-req-max-size', 0)
    verify_failure('raft.append-req-max-size', -1)
    verify_failure('raft.snapshot-req-max-count', 0)
    verify_failure('raft.snapshot-req-max-count', -1)
    verify_failure('raft.snapshot-req-max-size', 0)
    verify_failure('raft.snapshot-req-max-size', -1)
    verify_failure('raft.log-max-cache-size', -1)
    verify_failure('raft.log-max-file-size', -1)
    verify_failure('raft.scan-size', -1)
    verify_failure('raft.log-delay-apply', -1)
    verify_failure('raft.snapshot-delay', -1)

    verify_failure('raft.log-fsync', 'someinvalidvalue')
    verify_failure('raft.follower-proxy', 'someinvalidvalue')
    verify_failure('raft.quorum-reads', 'someinvalidvalue')
    verify_failure('raft.sharding', 'someinvalidvalue')
    verify_failure('raft.tls-enabled', 'someinvalidvalue')
    verify_failure('raft.log-disable-apply', 'someinvalidvalue')
    verify_failure('raft.snapshot-fail', 'someinvalidvalue')
    verify_failure('raft.snapshot-disable', 'someinvalidvalue')
    verify_failure('raft.snapshot-disable-load', 'someinvalidvalue')

    verify_failure('raft.loglevel', 'somelevel')
    verify_failure('raft.trace', 'sometracelevel')
    verify_failure('raft.migration-debug', 'somevalue')


def test_ignored_commands(cluster):
    """
    ignored commands
    """
    # create a non initialized single node cluster
    node = cluster.add_node(
        raft_args={"ignored-commands": "cmd1,cmd2"},
        cluster_setup=False)
    node.start()

    # ignored commands should give a non-existent command redis error
    with raises(ResponseError, match="unknown command"):
        node.client.execute_command("cmd1", "test", "command")
    with raises(ResponseError, match="unknown command"):
        node.client.execute_command("cmd2", "to", "ignore")

    # while not ignored commands should give an uninitialized cluster error
    with raises(ResponseError, match='NOCLUSTER No Raft Cluster'):
        node.client.execute_command("set", "a", "b")

    node.config_set('raft.ignored-commands', 'cmd3,cmd4')

    # ignored commands should give a non-existent command redis error
    with raises(ResponseError, match="unknown command"):
        node.client.execute_command("cmd3", "test", "command")
    with raises(ResponseError, match="unknown command"):
        node.client.execute_command("cmd4", "to", "ignore")

    # while not ignored commands should give an uninitialized cluster error
    with raises(ResponseError, match='NOCLUSTER No Raft Cluster'):
        node.client.execute_command("set", "a", "b")
    with raises(ResponseError, match='NOCLUSTER No Raft Cluster'):
        node.client.execute_command("cmd1", "a", "b")
    with raises(ResponseError, match='NOCLUSTER No Raft Cluster'):
        node.client.execute_command("cmd2", "a", "b")


def test_module_command_flags(cluster):
    """
    Test commandspec table is updated after module load/unload
    """

    node = cluster.add_node(
        redis_args=["--enable-module-command", "yes"],
        cluster_setup=False)
    node.start()

    # ensure it doesn't exit
    with raises(ResponseError, match='unknown command'):
        node.execute("raft.debug", "commandspec", "hellomodule")

    node.load_module("hellomodule.so")

    dont_intercept = 1 << 4

    node.config_set('raft.ignored-commands', 'hellomodule')
    assert node.execute("raft.debug", "commandspec",
                        "hellomodule") == dont_intercept
