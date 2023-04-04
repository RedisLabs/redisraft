"""
Copyright Redis Ltd. 2020 - present
Licensed under your choice of the Redis Source Available License 2.0 (RSALv2)
or the Server Side Public License v1 (SSPLv1).
"""

import os.path
import subprocess
import typing
from types import SimpleNamespace
import pytest
from .sandbox import Cluster, Elle, ElleWorker
from .workload import Workload


def pytest_addoption(parser):

    parser.addoption(
        '--redis-executable', default='../redis/src/redis-server',
        help='Name of redis-server executable to use.')
    parser.addoption(
        '--raft-loglevel', default='debug',
        help='RedisRaft Module log level.')
    parser.addoption(
        '--raft-trace', default='off',
        help='RedisRaft Module trace log levels.')
    parser.addoption(
        '--raft-module', default='redisraft.so',
        help='RedisRaft Module filename.')
    parser.addoption(
        '--work-dir', default='tests/tmp',
        help='Working directory for tests temporary files.')
    parser.addoption(
        '--redis-up-timeout', default=120,
        help='Seconds to wait for Redis to start before timing out.')
    parser.addoption(
        '--valgrind', default=False, action='store_true',
        help='Run Redis under valgrind.')
    parser.addoption(
        '--keep-files', default=False, action='store_true',
        help='Do not clean up temporary test files.')
    parser.addoption(
        '--fsync', default=False, action='store_true',
        help='Use log-fsync')
    parser.addoption(
        '--tls', default=False, action='store_true',
        help='Use tls')
    parser.addoption(
        "--runslow", action="store_true", default=False, help="run slow tests")
    parser.addoption(
        '--elle-cli',
        default='../elle-cli/target/elle-cli-0.1.5-standalone.jar',
        help='location for elle-cli jar file')
    parser.addoption(
        '--elle-threads', default=0, help='number of elle worker threads')
    parser.addoption(
        '--elle-ops-per-tx', default=1,
        help='number of append/read pairs per transaction')
    parser.addoption(
        '--repeat', action='store',
        help='Number of times to repeat each test')


def pytest_generate_tests(metafunc):
    if metafunc.config.option.repeat is not None:
        count = int(metafunc.config.option.repeat)

        metafunc.fixturenames.append('repeat_test')
        metafunc.parametrize('repeat_test', range(count))


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: mark test as slow to run")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--runslow"):
        # --runslow given in cli: do not skip slow tests
        return
    skip_slow = pytest.mark.skip(reason="need --runslow option to run")
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip_slow)


def create_config(pytest_config):
    config = SimpleNamespace()

    config.executable = os.path.abspath(
        pytest_config.getoption('--redis-executable'))
    config.args = None
    config.raftmodule = pytest_config.getoption('--raft-module')
    config.up_timeout = pytest_config.getoption('--redis-up-timeout')
    config.raft_loglevel = pytest_config.getoption('--raft-loglevel')
    config.raft_trace = pytest_config.getoption('--raft-trace')
    config.workdir = pytest_config.getoption('--work-dir')
    config.keepfiles = pytest_config.getoption('--keep-files')
    config.fsync = pytest_config.getoption('--fsync')
    config.tls = pytest_config.getoption('--tls')
    config.elle_cli = os.path.abspath(pytest_config.getoption('--elle-cli'))
    config.elle_threads = int(pytest_config.getoption('--elle-threads'))
    config.elle_num_ops = int(pytest_config.getoption('--elle-ops-per-tx'))

    if pytest_config.getoption('--valgrind'):
        if config.args is None:
            config.args = []
        config.args = [
            '--leak-check=full',
            '--show-reachable=no',
            '--show-possibly-lost=yes',
            '--show-reachable=no',
            '--suppressions=../redis/src/valgrind.sup',
            '--log-file={}/valgrind-redis.%p'.format(config.workdir),
            config.executable] + config.args
        config.executable = 'valgrind'

    return config


@pytest.fixture
def elle(request):
    _config = create_config(request.config)

    elle_main = Elle(_config)

    yield elle_main

    elle_main.logfile.close()
    # no reason to run elle if failed
    if _config.elle_threads > 0 and not request.session.testsfailed:
        p = subprocess.run(["/usr/bin/java", "-jar", elle_main.config.elle_cli,
                            "-v", "--model", "list-append",
                            os.path.join(elle_main.logdir, "logfile.edn")],
                           capture_output=True)
        if p.returncode != 0:
            print(f"stdout = {p.stdout.decode()}")
            print(f"stderr = {p.stderr.decode()}")

        assert p.returncode == 0


@pytest.fixture
def cluster(request):
    """
    A fixture for a sandbox Cluster()
    """

    _cluster = Cluster(create_config(request.config))
    yield _cluster
    _cluster.destroy()


@pytest.fixture
def cluster_factory(request, elle):
    """
    A fixture for a creating custom sandboxed Cluster()s
    """

    created_clusters = []
    workers: typing.List[ElleWorker] = []

    num_elle_keys = 1
    key_hash_tag = "test"

    marker = request.node.get_closest_marker("num_elle_keys")
    if marker is not None:
        num_elle_keys = marker.args[0]

    marker = request.node.get_closest_marker("key_hash_tag")
    if marker is not None:
        key_hash_tag = marker.args[0]

    keys = [f"{{{key_hash_tag}}}elle" + str(x) for x in range(num_elle_keys)]

    marker = request.node.get_closest_marker("elle_test")
    if marker is not None:
        workers = [ElleWorker(elle, created_clusters, keys)
                   for _ in range(create_config(request.config).elle_threads)]

    for worker in workers:
        worker.start()

    cluster_args = {'base_port': 5000, 'base_id': 0, 'cluster_id': 0}

    def _create_cluster():
        _cluster = Cluster(create_config(request.config), **cluster_args)
        cluster_args['base_port'] += 100
        cluster_args['base_id'] += 100
        cluster_args['cluster_id'] += 1
        created_clusters.append(_cluster)
        return _cluster

    yield _create_cluster

    for worker in workers:
        worker.finish()
        worker.join()

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
