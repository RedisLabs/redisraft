import sys
import time
import os
import os.path
import subprocess
import weakref
import redis
import logging

LOG = logging.getLogger('sandbox')

class RedisRaftError(Exception):
    pass

class RedisRaftTimeout(RedisRaftError):
    pass

class RedisRaft(object):
    def __init__(self, _id, port, raftmodule='redisraft.so',
                 executable='redis-server', redis_args=None):
        self.id = _id
        self.port = port
        self.executable = executable
        self.process = None
        self.raftlog = 'raftlog{}.db'.format(self.id)
        self.logfile = 'redis{}.log'.format(self.id)
        self.args = ['--port', str(port)]
        if redis_args:
            self.args += redis_args
        self.args += ['--loadmodule', os.path.abspath(raftmodule)]
        self.raft_args = ['id={}'.format(_id),
                          'addr=localhost:{}'.format(self.port),
                          'raftlog=' + self.raftlog]

        self.client = redis.Redis(host='localhost', port=self.port)
        self.client.connection_pool.connection_kwargs['parser_class'] = \
            redis.connection.PythonParser
        self.client.set_response_callback('raft.info', redis.client.parse_info)

    def init(self):
        self.cleanup()
        self.start(['init'])
        return self

    def join(self, port):
        self.cleanup()
        self.start(['join=localhost:{}'.format(port)])
        return self

    def start(self, extra_raft_args=None):
        if extra_raft_args is None:
            extra_raft_args = []
        args = [self.executable] + self.args + self.raft_args + extra_raft_args
        self.process = subprocess.Popen(
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            executable=self.executable,
            args=args)
        self.verify_up()
        LOG.info('RedisRaft<%s> is up, pid=%s', self.id, self.process.pid)

    def verify_up(self, timeout=2):
        retries = timeout * 100
        while True:
            try:
                self.client.ping()
                return
            except redis.exceptions.ConnectionError:
                retries -= 1
                if not retries:
                    LOG.fatal('RedisRaft<%s> failed to start', self.id)
                    raise RuntimeError('RedisRaft<%s> failed to start' %
                                       self.id)
                time.sleep(0.01)

    @staticmethod
    def _dump_log(data, title):
        if len(data) == 0:
            return
        LOG.debug('==========> Begining of {} <=========='.format(title))
        for line in data.decode('utf-8').split('\\n'):
            LOG.debug(line)
        LOG.debug('==========> End of {} <=========='.format(title))

    def terminate(self):
        if self.process:
            try:
                self.process.terminate()
                self.process.wait()
                _stdout, _stderr = self.process.communicate()
                self._dump_log(_stdout, "RedisRaft<{}> STDOUT Result".format(
                    self.id))
                self._dump_log(_stderr, "RedisRaft<{}> STDERR Result".format(
                    self.id))

            except OSError as err:
                LOG.error('RedisRaft<%s> failed to terminate: %s',
                          self.id, err)
                pass
            else:
                LOG.info('RedisRaft<%s> terminated', self.id)
        self.process = None

    def restart(self):
        self.terminate()
        self.start()

    def cleanup(self):
        try:
            os.unlink(self.raftlog)
        except OSError:
            pass

    def raft_exec(self, *args):
        cmd = ['RAFT'] + list(args)
        return self.client.execute_command(*cmd)

    def raft_config_set(self, key, val):
        return self.client.execute_command('raft.config', 'set', key, val)

    def raft_info(self):
        return self.client.execute_command('raft.info')

    def _wait_for_condition(self, test_func, timeout_func, timeout=3):
        retries = timeout * 10
        while retries > 0:
            if test_func():
                return
            retries -= 1
            time.sleep(0.1)
        timeout_func()

    def wait_for_election(self, timeout=3):
        def has_leader():
            return bool(self.raft_info()['leader_id'] != -1)
        def raise_no_master_error():
            raise RedisRaftTimeout('No master elected')
        self._wait_for_condition(has_leader, raise_no_master_error, timeout)

    def wait_for_log_applied(self, timeout=3):
        def commit_idx_applied():
            info = self.raft_info()
            return bool(info['commit_index'] == info['last_applied_index'])
        def raise_not_applied():
            raise RedisRaftTimeout('Last committed entry not yet applied')
        self._wait_for_condition(commit_idx_applied, raise_not_applied,
                                 timeout)
        LOG.debug("Finished waiting logs to be applied.")

    def wait_for_num_voting_nodes(self, count, timeout=10):
        def num_voting_nodes_match():
            info = self.raft_info()
            return bool(info['num_voting_nodes'] == count)
        def raise_not_added():
            raise RedisRaftTimeout('Nodes not added')
        self._wait_for_condition(num_voting_nodes_match, raise_not_added,
                                 timeout)
        LOG.debug("Finished waiting for num_voting_nodes == %d", count)

    def destroy(self):
        self.terminate()
        self.cleanup()

class Cluster(object):
    noleader_retries = 5
    base_port = 5000

    def __init__(self):
        self.nodes = {}
        self.leader = None

    def create(self, node_count):
        assert self.nodes == {}
        self.nodes = {x: RedisRaft(x, self.base_port + x)
                      for x in range(1, node_count + 1)}
        for _id, node in self.nodes.items():
            if _id == 1:
                node.init()
            else:
                node.join(self.base_port + 1)
        self.leader = 1
        self.node(1).wait_for_num_voting_nodes(len(self.nodes))
        self.node(1).wait_for_log_applied()

    def add_node(self):
        _id = max(self.nodes.keys()) + 1 if len(self.nodes) > 0 else 1
        node = RedisRaft(_id, self.base_port + _id)
        if len(self.nodes) > 0:
            node.join(self.base_port + self.leader)
        else:
            node.init()
            self.leader = _id
        self.nodes[_id] = node
        return node

    def node(self, _id):
        return self.nodes[_id]

    def exec_all(self, *cmd):
        result = []
        for _id, node in self.nodes.items():
            try:
                r = node.client.execute_command(*cmd)
                result.append(r)
            except redis.ConnectionError:
                pass
        return result

    def raft_exec(self, *cmd):
        retries = self.noleader_retries
        while retries > 0:
            try:
                return self.nodes[self.leader].raft_exec(*cmd)
            except redis.ConnectionError:
                self.leader += 1
                if self.leader > len(self.nodes):
                    self.leader = 1
            except redis.ResponseError as err:
                if str(err).startswith('MOVED'):
                    port = int(str(err).split(':')[-1])
                    new_leader = port - 5000
                    assert new_leader != self.leader
                    self.leader = new_leader
                elif str(err).startswith('NOLEADER'):
                    retries -= 1
                    time.sleep(0.500)
                else:
                    raise
        raise RedisRaftError('No leader elected')

    def destroy(self):
        for node in self.nodes.values():
            node.destroy()
