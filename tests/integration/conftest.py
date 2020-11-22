"""
This file is part of RedisRaft.

Copyright (c) 2020 Redis Labs

RedisRaft is dual licensed under the GNU Affero General Public License version 3
(AGPLv3) or the Redis Source Available License (RSAL).
"""

import os.path
from types import SimpleNamespace
import pytest
from .sandbox import Cluster
from .workload import Workload


def pytest_addoption(parser):
    import os
    parser.addoption(
        '--redis-executable', default='../redis/src/redis-server',
        help='Name of redis-server executable to use.')
    parser.addoption(
        '--raft-loglevel', default='debug',
        help='RedisRaft Module log level.')
    parser.addoption(
        '--raft-module', default='redisraft.so',
        help='RedisRaft Module filename.')
    parser.addoption(
        '--work-dir', default='tests/tmp',
        help='Working directory for tests temporary files.')
    parser.addoption(
        '--redis-up-timeout', default=3,
        help='Seconds to wait for Redis to start before timing out.')
    parser.addoption(
        '--valgrind', default=False, action='store_true',
        help='Run Redis under valgrind.')
    parser.addoption(
        '--keep-files', default=False, action='store_true',
        help='Do not clean up temporary test files.')


def create_config(pytest_config):
    config = SimpleNamespace()

    config.executable = os.path.abspath(
        pytest_config.getoption('--redis-executable'))
    config.args = None
    config.raftmodule = pytest_config.getoption('--raft-module')
    config.up_timeout = pytest_config.getoption('--redis-up-timeout')
    config.raft_loglevel = pytest_config.getoption('--raft-loglevel')
    config.workdir = pytest_config.getoption('--work-dir')
    config.keepfiles = pytest_config.getoption('--keep-files')

    if (pytest_config.getoption('--valgrind')):
        if config.args is None:
            config.args = []
        config.args = [
            '--leak-check=full',
            '--show-reachable=no',
            '--show-possibly-lost=yes',
            '--show-reachable=no',
            '--suppressions=../redis/src/valgrind.sup',
            '--suppressions=libuv.supp',
            '--log-file={}/valgrind-redis.%p'.format(config.workdir),
            config.executable] + config.args
        config.executable = 'valgrind'

    return config


@pytest.fixture
def cluster(request):
    """
    A fixture for a sandbox Cluster()
    """

    _cluster = Cluster(create_config(request.config))
    yield _cluster
    _cluster.destroy()


@pytest.fixture
def cluster_factory(request):
    """
    A fixture for a creating custom sandboxed Cluster()s
    """

    created_clusters = []
    cluster_args = {'base_port': 5000, 'base_id': 0}

    def _create_cluster():
        _cluster = Cluster(create_config(request.config), **cluster_args)
        cluster_args['base_port'] += 100
        cluster_args['base_id'] += 100
        created_clusters.append(_cluster)
        return _cluster

    yield _create_cluster

    for _c in created_clusters:
        _c.destroy()

@pytest.fixture
def workload():
    """
    A fixture for a Workload.
    """

    _workload = Workload()
    yield _workload
    _workload.terminate()
