"""
Copyright Redis Ltd. 2020 - present
Licensed under your choice of the Redis Source Available License 2.0 (RSALv2)
or the Server Side Public License v1 (SSPLv1).
"""

import re
import time
import os
import os.path
import subprocess
import threading
import itertools
import random
import logging
import signal
import typing
import uuid
import shutil
from typing import List

import redis
from redis import ResponseError

LOG = logging.getLogger('sandbox')


class RedisRaftBug(Exception):
    pass


class RedisRaftError(Exception):
    pass


class RedisRaftTimeout(RedisRaftError):
    pass


class RedisRaftFailedToStart(RedisRaftError):
    pass


class PipeLogger(threading.Thread):
    def __init__(self, pipe, prefix):
        super(PipeLogger, self).__init__()
        self.prefix = prefix
        self.pipe = pipe
        self.daemon = True
        self.loglines = ""
        self.start()

    def run(self):
        try:
            for line in iter(self.pipe.readline, b''):
                linestr = str(line, 'utf-8').rstrip()
                self.loglines += linestr
                LOG.debug('%s: %s', self.prefix, linestr)
        except ValueError as err:
            LOG.debug("PipeLogger: %s", str(err))
            return


class RawConnection(object):
    """
    Implement a simple way of executing a Redis command and return the raw
    unprocessed reply (unlike redis-py's execute_command() which applies some
    command-specific parsing.)
    """

    def __init__(self, client):
        self._pool = client.connection_pool
        self._conn = self._pool.get_connection('raw-connection')

    def execute(self, *cmd):
        self._conn.send_command(*cmd)
        return self._conn.read_response()

    def release(self):
        self._pool.release(self._conn)

    def disconnect(self):
        self._conn.disconnect()
        self.release()

    def getClientId(self):
        return self.execute("CLIENT", "ID")


class RedisRaft(object):
    def __init__(self, _id, port, config, redis_args=None, raft_args=None,
                 use_id_arg=True, cluster_id=0, password=None,
                 cacert_type=None):
        self.id = _id
        self.cluster_id = cluster_id
        self.guid = str(uuid.uuid4())
        self.port = port
        self.executable = config.executable
        self.process = None
        self.paused = False
        self.workdir = os.path.abspath(config.workdir)
        self.serverdir = os.path.join(self.workdir, self.guid)
        self._raftlog = 'redis{}.db'.format(self.id)
        self._raftlogidx = '{}.idx'.format(self.raftlog)
        self._dbfilename = 'redis{}.rdb'.format(self.id)
        self.up_timeout = config.up_timeout
        self.keepfiles = config.keepfiles
        self.args = config.args.copy() if config.args else []

        self.args += ['--port', str(0) if config.tls else str(port),
                      '--bind', '0.0.0.0',
                      '--dir', self.serverdir,
                      '--dbfilename', self._dbfilename,
                      '--loglevel', config.raft_loglevel]
        if password:
            self.args += ['--requirepass', password]

        self.cacert = os.getcwd() + '/tests/tls/ca.crt'
        self.cacert_dir = os.getcwd() + '/tests/tls'
        self.cert = os.getcwd() + '/tests/tls/redis.crt'
        self.key = os.getcwd() + '/tests/tls/redis.key'

        if config.tls:
            # 'cacert_type' can be None, 'dir', 'file' and 'both'.

            # For None, 'both' and 'file', we set --tls-ca-cert-file
            if cacert_type is None or cacert_type in ('both', 'file'):
                self.args += ['--tls-ca-cert-file', self.cacert]

            # For 'both' and 'dir', we set --tls-ca-cert-dir
            if cacert_type is not None and cacert_type in ('both', 'dir'):
                self.args += ['--tls-ca-cert-dir', self.cacert_dir]

            self.args += ['--tls-port', str(port),
                          '--tls-cert-file', self.cert,
                          '--tls-key-file', self.key,
                          '--tls-key-file-pass', 'redisraft']

        self.args += redis_args if redis_args else []
        self.args += ['--loadmodule', os.path.abspath(config.raftmodule)]

        if raft_args is None:
            raft_args = {}
        else:
            raft_args = raft_args.copy()

        if password:
            raft_args['cluster-password'] = password

        if use_id_arg:
            raft_args['id'] = str(_id)

        default_args = {'addr': self.address,
                        'log-filename': self._raftlog,
                        'log-fsync': 'yes' if config.fsync else 'no',
                        'loglevel': config.raft_loglevel,
                        'trace': config.raft_trace,
                        'tls-enabled': 'yes' if config.tls else 'no'}

        for defkey, defval in default_args.items():
            if defkey not in raft_args:
                raft_args[defkey] = defval

        raft_args = {'--raft.' + k: v for k, v in raft_args.items()}
        self.raft_args = [str(x) for x in
                          itertools.chain.from_iterable(raft_args.items())]

        client_cacert = os.getcwd() + '/tests/tls/ca.crt'
        client_cert = os.getcwd() + '/tests/tls/client.crt'
        client_key = os.getcwd() + '/tests/tls/client.key'

        self.client = redis.Redis(host='localhost', port=self.port,
                                  socket_timeout=20,
                                  password=password,
                                  ssl=config.tls,
                                  ssl_certfile=client_cert,
                                  ssl_keyfile=client_key,
                                  ssl_ca_certs=client_cacert)

        self.client.connection_pool.connection_kwargs['parser_class'] = \
            redis.connection.PythonParser
        self.client.set_response_callback('info raft', redis.client.parse_info)
        self.client.set_response_callback('config get',
                                          redis.client.parse_config_get)
        self.stdout = None
        self.stderr = None
        self.cleanup()

    @property
    def address(self):
        return 'localhost:{}'.format(self.port)

    @property
    def raftlog(self):
        return os.path.join(self.serverdir, self._raftlog)

    @property
    def raftlogidx(self):
        return os.path.join(self.serverdir, self._raftlogidx)

    @property
    def dbfilename(self):
        return os.path.join(self.serverdir, self._dbfilename)

    def cluster(self, *args, single_run=False):
        retries = self.up_timeout
        if retries is not None:
            retries *= 10
        if single_run:
            retries = 1
        while True:
            try:
                return self.client.execute_command('RAFT.CLUSTER', *args)
            except redis.exceptions.RedisError as err:
                LOG.info(err)
                if retries is not None:
                    retries -= 1
                    if retries <= 0:
                        LOG.fatal('RAFT.CLUSTER %s failed', " ".join(args))
                        raise err
                time.sleep(0.1)

    def init(self, cluster_id=None):
        self.cleanup()
        self.start()

        if cluster_id is None:
            dbid = self.cluster('init')
        else:
            dbid = self.cluster('init', cluster_id)

        LOG.info('Cluster created: %s', dbid)
        return self

    def join(self, addresses, single_run=False):
        self.start()
        self.cluster('join', *addresses, single_run=single_run)
        return self

    def start(self, extra_raft_args=None, verify=True):
        try:
            os.makedirs(self.serverdir)
        except OSError:
            pass

        if extra_raft_args is None:
            extra_raft_args = []
        args = [self.executable] + self.args + self.raft_args + extra_raft_args
        logging.info("starting node: args = {}".format(args))

        self.process = subprocess.Popen(
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            executable=self.executable,
            args=args)

        self.stdout = PipeLogger(self.process.stdout,
                                 'c{}/n{}/stdout'.format(self.cluster_id,
                                                         self.id))
        self.stderr = PipeLogger(self.process.stderr,
                                 'c{}/n{}/stderr'.format(self.cluster_id,
                                                         self.id))

        if not verify:
            return
        self.verify_up()
        LOG.info('RedisRaft<%s> is up, pid=%s, guid=%s', self.id,
                 self.process.pid, self.guid)

    def load_module(self, module):
        self.verify_up()

        path = f'{os.getcwd()}/tests/integration/modules/{module}'
        self.client.execute_command("module", "load", path)

    def unload_module(self, module):
        self.verify_up()
        self.client.execute_command("module", "unload", module)

    def process_is_up(self):
        if not self.process:
            return False

        self.process.poll()
        return self.process.returncode is None

    def verify_down(self, retries=50, retry_delay=0.1):
        while retries > 0:
            if not self.process_is_up():
                return True
            time.sleep(retry_delay)
            retries -= 1
        return False

    def verify_up(self):
        retries = self.up_timeout
        if retries is not None:
            retries *= 10
        while True:
            try:
                self.client.ping()
                return
            except (redis.exceptions.ConnectionError, redis.TimeoutError):
                if retries is not None:
                    retries -= 1
                    if not retries:
                        LOG.fatal('RedisRaft<%s> failed to start', self.id)
                        raise RedisRaftFailedToStart(
                            'RedisRaft<%s> failed to start' % self.id)
                time.sleep(0.1)

    def check_error_in_logs(self):
        stdout = ""
        stderr = ""

        if self.stdout:
            stdout = self.stdout.loglines.lower()

        if self.stderr:
            stderr = self.stderr.loglines.lower()

        if 'sanitizer' in stdout or 'sanitizer' in stderr:
            raise RedisRaftBug('Sanitizer error')

        if 'redis bug report' in stdout or 'redis bug report' in stderr:
            raise RedisRaftBug('RedisRaft crash')

    def terminate(self):
        if self.process:
            if self.paused:
                self.resume()
            try:
                self.process.terminate()

                try:
                    self.process.communicate(timeout=60)
                except subprocess.TimeoutExpired:
                    self.process.kill()

            except OSError as err:
                LOG.error('RedisRaft<%s> failed to terminate: %s',
                          self.id, err)
            else:
                LOG.info('RedisRaft<%s> terminated', self.id)

        if self.stdout:
            self.stdout.join(timeout=30)
        if self.stderr:
            self.stderr.join(timeout=30)

        self.process = None
        self.check_error_in_logs()

    def kill(self):
        if self.process:
            try:
                self.process.kill()

                try:
                    self.process.communicate(timeout=60)
                except subprocess.TimeoutExpired:
                    self.process.kill()

            except OSError as err:
                LOG.error('Cannot kill RedisRaft<%s>: %s',
                          self.id, err)
            else:
                LOG.info('RedisRaft<%s> killed', self.id)
        self.process = None
        self.check_error_in_logs()

    def restart(self, retries=5):
        self.terminate()
        while retries > 0:
            try:
                self.start()
                break
            except RedisRaftFailedToStart:
                retries -= 1
                time.sleep(0.5)
                continue

    def pause(self):
        if self.process is not None:
            self.paused = True
            self.process.send_signal(signal.SIGSTOP)

    def resume(self):
        if self.process is not None:
            self.paused = False
            self.process.send_signal(signal.SIGCONT)

    def cleanup(self):
        if not self.keepfiles:
            shutil.rmtree(self.serverdir, ignore_errors=True)

    def execute(self, *cmd):
        return self.client.execute_command(*cmd)

    def config_set(self, key, val):
        return self.client.config_set(key, val)

    def config_get(self, key):
        return self.client.config_get(key)

    def info(self):
        return self.client.execute_command('info raft')

    def raft_debug_exec(self, *cmd):
        """
        Execute the specified Redis command through RAFT.DEBUG EXEC,
        so it executes locally and does not go through Raft interception.
        """

        return self.client.execute_command('raft.debug', 'exec', *cmd)

    def commit_index(self):
        return self.info()['raft_commit_index']

    def current_index(self):
        return self.info()['raft_current_index']

    def transfer_leader(self, *args, **kwargs):
        return self.client.execute_command('raft.transfer_leader',
                                           *args, **kwargs)

    def timeout_now(self):
        return self.client.execute_command('raft.timeout_now')

    @staticmethod
    def _wait_for_condition(test_func, timeout_func, timeout=3):
        retries = timeout * 10
        while retries > 0:
            try:
                if test_func():
                    return
            except (redis.ConnectionError, redis.TimeoutError):
                pass

            retries -= 1
            time.sleep(0.1)
        timeout_func()

    def wait_for_election(self, timeout=20):
        def has_leader():
            return bool(self.info()['raft_leader_id'] != -1)

        def raise_no_master_error():
            raise RedisRaftTimeout('No master elected')
        self._wait_for_condition(has_leader, raise_no_master_error, timeout)

    def wait_for_leader_change(self, old_leader, timeout=20):
        def new_leader():
            leader = self.info()['raft_leader_id']
            return bool(leader != -1 and leader != old_leader)

        def raise_no_master_error():
            raise RedisRaftTimeout('No master elected')
        self._wait_for_condition(new_leader, raise_no_master_error, timeout)

    def wait_for_log_committed(self, timeout=20):
        def current_idx_committed():
            ret = self.info()
            return bool(ret['raft_commit_index'] == ret['raft_current_index'])

        def raise_not_committed():
            raise RedisRaftTimeout('Last log entry not yet committed')
        self._wait_for_condition(current_idx_committed, raise_not_committed,
                                 timeout)
        LOG.debug("Finished waiting for latest entry to be committed.")

    def wait_for_log_applied(self, timeout=20):
        def commit_idx_applied():
            info = self.info()
            commit = info['raft_commit_index']
            last_applied = info['raft_last_applied_index']
            return bool(commit == last_applied)

        def raise_not_applied():
            raise RedisRaftTimeout('Last committed entry not yet applied')
        self._wait_for_condition(commit_idx_applied, raise_not_applied,
                                 timeout)
        LOG.debug("Finished waiting logs to be applied.")

    def wait_for_current_index(self, idx, timeout=20):
        def current_idx_reached():
            info = self.info()
            return bool(info['raft_current_index'] == idx)

        def raise_not_reached():
            info = self.info()
            LOG.debug("------- last info before bail out: %s\n", info)

            raise RedisRaftTimeout(
                'Expected current index %s not reached' % idx)
        self._wait_for_condition(current_idx_reached, raise_not_reached,
                                 timeout)

    def wait_for_commit_index(self, idx, gt_ok=False, timeout=20):
        def commit_idx_reached():
            info = self.info()
            if gt_ok:
                return bool(info['raft_commit_index'] >= idx)
            return bool(info['raft_commit_index'] == idx)

        def raise_not_reached():
            info = self.info()
            LOG.debug("------- last info before bail out: %s\n", info)

            raise RedisRaftTimeout(
                'Expected commit index %s not reached' % idx)
        self._wait_for_condition(commit_idx_reached, raise_not_reached,
                                 timeout)

    def wait_for_num_voting_nodes(self, count, timeout=20):
        def num_voting_nodes_match():
            info = self.info()
            return bool(info['raft_num_voting_nodes'] == count)

        def raise_not_added():
            raise RedisRaftTimeout('Nodes not added')

        self._wait_for_condition(num_voting_nodes_match, raise_not_added,
                                 timeout)
        LOG.debug("Finished waiting for num_voting_nodes == %d", count)

    def wait_for_num_nodes(self, count, timeout=20):
        def num_nodes_match():
            info = self.info()
            return bool(info['raft_num_nodes'] == count)

        def raise_not_added():
            info = self.info()
            num = info['raft_num_nodes']
            raise RedisRaftTimeout('Nodes count did not modify: ' + str(num))

        self._wait_for_condition(num_nodes_match, raise_not_added, timeout)
        LOG.debug("Finished waiting for num_nodes == %d", count)

    def wait_for_node_voting(self, value='yes', timeout=20):
        def check_voting():
            info = self.info()
            return bool(info['raft_is_voting'] == value)

        def raise_not_voting():
            info = self.info()
            LOG.debug("Non voting node: %s", str(info))
            raise RedisRaftTimeout('Node voting != %s' % value)

        self._wait_for_condition(check_voting, raise_not_voting, timeout)

    def wait_for_info_param(self, name, value, greater=False, timeout=20):
        def check_param():
            info = self.info()
            if greater:
                return bool(info.get(name) > value)

            return bool(info.get(name) == value)

        def raise_not_matched():
            raise RedisRaftTimeout('INFO "%s" did not reach "%s"' %
                                   (name, value))

        self._wait_for_condition(check_param, raise_not_matched, timeout)

    def destroy(self):
        try:
            self.terminate()
        finally:
            self.cleanup()


class Cluster(object):
    noleader_timeout = 30

    def __init__(self, config, base_port=5000, base_id=0, cluster_id=0):
        self.next_id = base_id + 1
        self.cluster_id = cluster_id
        self.base_port = base_port
        self.nodes = {}
        self.leader = None
        self.raft_args = None
        self.config = config

    def nodes_count(self):
        return len(self.nodes)

    def node_ids(self):
        return self.nodes.keys()

    def node_ports(self):
        return [n.port for n in self.nodes.values()]

    def node_addresses(self):
        return [n.address for n in self.nodes.values()]

    def create(self, node_count, raft_args=None, cluster_id=None,
               password=None, prepopulate_log=0, cacert_type=None):
        if raft_args is None:
            raft_args = {}
        self.raft_args = raft_args.copy()
        assert self.nodes == {}
        self.nodes = {x: RedisRaft(x, self.base_port + x,
                                   config=self.config,
                                   raft_args=raft_args,
                                   cluster_id=self.cluster_id,
                                   password=password,
                                   cacert_type=cacert_type)
                      for x in range(1, node_count + 1)}
        self.next_id = node_count + 1
        for _id, node in self.nodes.items():
            if _id == 1:
                node.init(cluster_id=cluster_id)
            else:
                logging.info("{} joining".format(_id))
                node.join(['localhost:{}'.format(self.base_port + 1)])

        self.leader = 1
        self.node(1).wait_for_num_voting_nodes(len(self.nodes))
        self.wait_for_unanimity()

        # Pre-populate if asked
        for _ in range(prepopulate_log):
            assert self.execute('INCR', 'log-prepopulate-key')

        return self

    def config_set(self, param, value):
        for node in self.nodes.values():
            node.client.config_set(param, value)

    def add_initialized_node(self, node):
        self.nodes[node.id] = node

    def add_node(self, raft_args=None, port=None, cluster_setup=True,
                 node_id=None, use_cluster_args=False, single_run=False,
                 join_addr_list=None, redis_args=None, cacert_type=None,
                 **kwargs):
        _raft_args = raft_args
        if use_cluster_args:
            _raft_args = self.raft_args
        _id = self.next_id if node_id is None else node_id
        self.next_id += 1
        if port is None:
            port = self.base_port + _id
        node = None
        try:
            node = RedisRaft(_id, port, self.config, redis_args,
                             raft_args=_raft_args, cacert_type=cacert_type,
                             **kwargs)
            if cluster_setup:
                if self.nodes:
                    if join_addr_list is None:
                        join_addr_list = self.node_addresses()
                    node.join(join_addr_list, single_run=single_run)
                else:
                    node.init()
                    self.leader = _id
            self.nodes[_id] = node
            return node
        except redis.exceptions.RedisError:
            node.kill()
            raise

    def reset_leader(self):
        self.leader = next(iter(self.nodes.keys()))

    def remove_node(self, _id):
        def _func():
            self.node(self.leader).client.execute_command(
                'RAFT.NODE', 'REMOVE', _id)
        try:
            self.raft_retry(_func)
        except redis.ResponseError as err:
            # If we are removing the leader, leader will shut down before
            # sending the reply. On retry, we should get
            # "node id does not exist" reply. If we get this reply and still
            # have '_id' in our local list, we know removal was successful.
            # For other cases, we just propagate the exception.
            missing_node_id = str(err).startswith("node id does not exist")
            was_retry = missing_node_id and _id in self.nodes
            if not was_retry:
                raise err

        self.nodes[_id].destroy()
        del self.nodes[_id]
        if self.leader == _id:
            self.reset_leader()

    def random_node_id(self):
        while True:
            node_id = random.choice(list(self.nodes.keys()))
            if not self.node(node_id).paused:
                return node_id

    def find_node_id_by_port(self, port):
        for node in self.nodes.values():
            if node.port == port:
                return node.id
        return None

    def node(self, _id):
        return self.nodes[_id]

    def random_node(self):
        return self.nodes[self.random_node_id()]

    def leader_node(self):
        return self.nodes[self.leader]

    def follower_node(self):
        if len(self.nodes) <= 1:
            raise RedisRaftError('No followers in the cluster')

        while True:
            _id = random.choice(list(self.nodes.keys()))
            if _id != self.leader:
                return self.node(_id)

    def pause_leader(self) -> int:
        old_leader = self.leader
        self.node(old_leader).pause()
        self.leader = self.random_node_id()

        return old_leader

    def update_leader(self):
        def _func():
            # This command will be redirected to the leader in raft_retry
            self.node(self.leader).client.execute_command('get x')
        self.raft_retry(_func)

    def wait_for_unanimity(self, exclude=None, timeout=20):
        commit_idx = self.node(self.leader).commit_index()
        for _id, node in self.nodes.items():
            if exclude is not None and int(_id) in exclude:
                continue
            node.wait_for_commit_index(commit_idx, gt_ok=True, timeout=timeout)
            node.wait_for_log_applied(timeout=timeout)

    def wait_for_replication(self, exclude=None):
        current_idx = self.node(self.leader).current_index()
        for _id, node in self.nodes.items():
            if exclude is not None and int(_id) in exclude:
                continue
            node.wait_for_current_index(current_idx)

    def wait_for_info_param(self, name, value, exclude=None, timeout=20):
        for _id, node in self.nodes.items():
            if exclude is not None and int(_id) in exclude:
                continue
            node.wait_for_info_param(name, value, timeout=timeout)

    def raft_retry(self, func):
        start_time = time.time()
        while time.time() < start_time + self.noleader_timeout:
            try:
                return func()
            except redis.ConnectionError:
                self.leader = self.random_node_id()
            except (redis.ReadOnlyError, redis.TimeoutError):
                time.sleep(0.5)
            except redis.ResponseError as err:
                if str(err).startswith('READONLY'):
                    # While loading a snapshot we can get a READONLY
                    time.sleep(0.5)
                if str(err).startswith('UNBLOCKED'):
                    # Ignore unblocked replies...
                    time.sleep(0.5)
                elif str(err).startswith('MOVED'):
                    start_time = time.time()
                    port = int(str(err).split(':')[-1])
                    new_leader = self.find_node_id_by_port(port)
                    assert new_leader is not None
                    assert new_leader != self.leader

                    # When removing a leader there can be a race condition,
                    # in this case we need to do nothing
                    if new_leader in self.nodes:
                        self.leader = new_leader
                elif str(err).startswith('CLUSTERDOWN') or \
                        str(err).startswith('NOCLUSTER'):
                    rem = start_time + self.noleader_timeout - time.time()
                    if rem > 0:
                        LOG.info("-CLUSTERDOWN response received, will retry"
                                 " for %.2f seconds", rem)
                    time.sleep(0.5)
                else:
                    raise
        raise RedisRaftError('No leader elected')

    def execute(self, *cmd):
        """
        Execute the specified command on the leader node; Handle redirects
        and retries as necessary.
        """

        def _func():
            return self.nodes[self.leader].client.execute_command(*cmd)
        return self.raft_retry(_func)

    def destroy(self):
        err = None

        for node in self.nodes.values():
            try:
                node.destroy()
            except RedisRaftBug as e:
                err = e
        if err:
            raise err

    def terminate(self):
        err = None
        for node in self.nodes.values():
            try:
                node.terminate()
            except RedisRaftBug as e:
                err = e
        if err:
            raise err

    def start(self):
        for node in self.nodes.values():
            node.start()

    def restart(self):
        for node in self.nodes.values():
            node.terminate()
        for node in self.nodes.values():
            node.start()


class ElleOp(object):
    def __init__(self, op_type: str, key: str,
                 value: typing.Optional[str] = None):
        self.op_type = op_type
        self.key = key
        self.value = value

    def __str__(self):
        if self.op_type == "append":
            return f"[:append \"{self.key}\" {self.value}]"
        elif self.op_type == "read":
            return f"[:r \"{self.key}\" nil]"
        else:
            raise Exception(f"invalid op_type = {self.op_type}")


class Elle(object):
    def __init__(self, config):
        self.config = config
        workdir = os.path.abspath(self.config.workdir)
        self.logdir = os.path.join(workdir, str(uuid.uuid4()))
        os.makedirs(self.logdir, exist_ok=True)
        self.lock = threading.Lock()
        self.logfile = open(os.path.join(self.logdir, "logfile.edn"), "x")
        self.index = {}
        self.value = 0

    @staticmethod
    def generate_ops_value(ops: typing.List[ElleOp]) -> str:
        val = " ".join(map(str, ops))
        return f"[{val}]"

    def generate_command_output(self, thread: int, ops: List[ElleOp],
                                state: str):
        return (f"{{"
                f":index {self.index[thread]} "
                f":process {thread} "
                f":type :{state}, "
                f":value {self.generate_ops_value(ops)}"
                f"}}")

    def log_command(self, thread: int, ops: List[ElleOp]):
        try:
            self.index[thread] += 1
        except KeyError:
            self.index[thread] = 1

        output = self.generate_command_output(thread, ops, "invoke")
        self.lock.acquire()
        self.logfile.write(output)
        self.logfile.write("\n")
        self.lock.release()

    @staticmethod
    def generate_results_value(results: typing.List, ops: typing.List[ElleOp]):
        val = "["
        for i in range(len(ops)):
            if i != 0:
                val += " "
            if ops[i].op_type == "append":
                val += str(ops[i])
            elif ops[i].op_type == "read":
                tmp = " ".join([x.decode('utf-8') for x in results[i]])
                val += f"[:r \"{ops[i].key}\" [{tmp}]]"
            else:
                raise Exception(f"unknown op type {ops[i].op_type}")
        val += "]"
        return val

    def generate_result_output(self, thread: int, result: str,
                               results: typing.Optional[List],
                               ops: List[ElleOp]):
        output: str
        if result == "ok":
            output = self.generate_results_value(results, ops)
            return (f"{{"
                    f":index {self.index[thread]} "
                    f":process {thread} "
                    f":type :{result}, "
                    f":value {output}"
                    f"}}")
        elif result == "fail":
            output = self.generate_ops_value(ops)
            return (f"{{"
                    f":index {self.index[thread]} "
                    f":process {thread} "
                    f":type :{result}, "
                    f":value {output}"
                    f"}}")
        else:
            raise Exception(f"unknown result value {result}")

    def log_result(self, thread, result: str, results: typing.Optional[List],
                   ops: List[ElleOp]):
        # if this throws an exception, we have a failure elsewhere
        self.index[thread] += 1

        output = self.generate_result_output(thread, result, results, ops)
        self.lock.acquire()
        self.logfile.write(output)
        self.logfile.write("\n")
        self.lock.release()

    def log_comment(self, msg: str):
        self.lock.acquire()
        self.logfile.write("; " + msg + "\n")
        self.lock.release()

    def get_next_value(self):
        with self.lock:
            ret = self.value
            self.value += 1
            return ret


class ElleWorker(threading.Thread):
    def __init__(self, elle: Elle, clusters: typing.List[Cluster],
                 keys: typing.Optional[List[str]] = None):
        super().__init__()

        self.clusters = clusters
        self.elle: Elle = elle
        self.ended: bool = False
        self.id: int = 0
        self.keys: typing.List
        if keys is not None:
            self.keys = keys
        else:
            self.keys = ["test"]

    def finish(self):
        self.ended = True

    def set_keys(self, keys: typing.List):
        self.keys = keys

    # We might not want to generate ops. Perhaps, each multi should be a
    # single append/read of the same key.
    def generate_ops(self, available_keys: typing.List[str]):
        ops: typing.List[ElleOp] = []
        for _ in range(self.elle.config.elle_num_ops):
            key = available_keys[random.randrange(0, len(available_keys))]
            val = self.elle.get_next_value()
            ops.append(ElleOp("append", key, str(val)))
            ops.append(ElleOp("read", key))

        return ops

    def do_ops(self, ops: typing.List[ElleOp]):
        # needs a way to handle asking
        good_write = False
        needs_asking = False

        client = redis.Redis(host="localhost",
                             port=self.clusters[0].leader_node().port)
        self.elle.log_command(self.id, ops)

        # Three possible hops can happen in a normal scenario:
        # MOVED -> ASK -> (remote) MOVED
        # (we'll send ASKING again to the remote leader).
        # If it doesn't work by then, log an error.
        conn: typing.Optional[RawConnection] = None
        for i in range(0, 3):
            if conn is not None:
                conn.release()

            conn = RawConnection(client)

            if needs_asking:
                assert conn.execute('ASKING') == b'OK'

            assert conn.execute('MULTI') == b'OK'

            # queue up operations
            for j in range(len(ops)):
                if ops[j].op_type == "append":
                    ret = conn.execute('RPUSH', ops[j].key, ops[j].value)
                    assert ret == b'QUEUED'
                elif ops[j].op_type == "read":
                    ret = conn.execute('LRANGE', ops[j].key, 0, -1)
                    assert ret == b'QUEUED'
                else:
                    raise Exception(f"Unknown op_type {ops[j].op_type}")

            # exec/handle failure
            try:
                ret = conn.execute('EXEC')
                self.elle.log_result(self.id, "ok", ret, ops)
                good_write = True
                break
            except ResponseError as e:
                # handle MOVED/ASK as those aren't "failures"
                m = re.search(r"^MOVED \d* (.*):(.*)$", str(e))
                a = re.search(r"^ASK \d* (.*):(.*)$", str(e))
                if m:
                    client = redis.Redis(host=m.group(1), port=m.group(2))
                elif a:
                    client = redis.Redis(host=a.group(1), port=a.group(2))
                    needs_asking = True
                else:
                    # Some other failure, therefore don't retry, just break.
                    # We expect failures to occur.
                    break

        conn.release()

        if not good_write:
            self.elle.log_result(self.id, "fail", None, ops)

    def run(self) -> None:
        self.id = threading.get_ident()

        while not self.ended:
            if len(self.clusters) == 0:
                continue
            if len(self.clusters[0].nodes) == 0:
                continue
            if self.clusters[0].leader is None:
                continue

            ops = self.generate_ops(self.keys)
            self.do_ops(ops)


def assert_after(func, timeout, retry_interval=0.5):
    """
    Call func() which is expected to perform certain assertions.
    If assertions failed, retry with a retry_interval delay until
    timeout has been reached -- at which point an exception is raised.
    """
    start_time = time.time()
    while True:
        try:
            func()
            break
        except AssertionError:
            if time.time() > start_time + timeout:
                raise
            time.sleep(retry_interval)


# copied from redis-py-cluster
def crc16(data: bytes) -> int:
    x_mode_m_crc16_lookup = [
        0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7,
        0x8108, 0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef,
        0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
        0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de,
        0x2462, 0x3443, 0x0420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485,
        0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
        0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4,
        0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc,
        0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823,
        0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b,
        0x5af5, 0x4ad4, 0x7ab7, 0x6a96, 0x1a71, 0x0a50, 0x3a33, 0x2a12,
        0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a,
        0x6ca6, 0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41,
        0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b, 0x8d68, 0x9d49,
        0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70,
        0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78,
        0x9188, 0x81a9, 0xb1ca, 0xa1eb, 0xd10c, 0xc12d, 0xf14e, 0xe16f,
        0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
        0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e,
        0x02b1, 0x1290, 0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256,
        0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
        0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
        0xa7db, 0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e, 0xc71d, 0xd73c,
        0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
        0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab,
        0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882, 0x28a3,
        0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
        0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92,
        0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8, 0x8dc9,
        0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1,
        0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8,
        0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93, 0x3eb2, 0x0ed1, 0x1ef0
    ]

    crc = 0
    for byte in data:
        crc = ((crc << 8) & 0xff00) ^ \
              x_mode_m_crc16_lookup[((crc >> 8) & 0xff) ^ byte]
    return crc & 0xffff


def key_hash_slot(key: str) -> int:
    assert key is not None
    return crc16(key.encode()) % 16384


class SlotRangeType():
    UNDEF = '0'
    STABLE = '1'
    IMPORTING = '2'
    MIGRATING = '3'
    MAX = '4'
