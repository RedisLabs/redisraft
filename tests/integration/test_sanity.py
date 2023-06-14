"""
Copyright Redis Ltd. 2020 - present
Licensed under your choice of the Redis Source Available License 2.0 (RSALv2)
or the Server Side Public License v1 (SSPLv1).
"""

import socket
import typing

import pytest as pytest
import time
from redis import ResponseError, ConnectionError
from pytest import raises
from retry import retry

from .sandbox import RedisRaft, RedisRaftFailedToStart, RawConnection
from retry import retry


def test_info_before_cluster_init(cluster):
    node = RedisRaft(1, cluster.base_port, cluster.config)
    node.start()
    assert node.info()["raft_state"] == "uninitialized"


def test_info(cluster):
    r1 = cluster.add_node()
    data = r1.client.execute_command('info')
    assert data["cluster_enabled"] == 1


def test_set_cluster_id(cluster):
    cluster_id = "12345678901234567890123456789012"
    cluster.create(3, cluster_id=cluster_id)
    assert str(cluster.leader_node().info()["raft_dbid"]) == cluster_id


def test_set_cluster_password(cluster):
    password = "foobar"
    cluster_id = "12345678901234567890123456789012"
    cluster.create(3, password=password, cluster_id=cluster_id)
    assert str(cluster.leader_node().info()["raft_dbid"]) == cluster_id


def test_set_cluster_id_too_short(cluster):
    with raises(ResponseError, match="cluster id must be 32 characters"):
        cluster.create(3, cluster_id="123456789012345678901234567890")


def test_set_cluster_id_too_long(cluster):
    with raises(ResponseError, match="cluster id must be 32 characters"):
        cluster.create(3, cluster_id="1234567890123456789012345678901234")


def test_fail_cluster_enabled(cluster):
    with raises(RedisRaftFailedToStart):
        cluster.add_node(redis_args=["--cluster_enabled", "yes"])


def test_add_node_as_a_single_leader(cluster):
    """
    Single node becomes a leader
    """
    # Do some basic sanity
    r1 = cluster.add_node()
    assert r1.client.set('key', 'value')
    assert r1.info()['raft_current_index'] == 3


def test_node_joins_and_gets_data(cluster):
    """
    Node joins and gets data
    """
    r1 = cluster.add_node()
    assert r1.client.set('key', 'value')
    r2 = cluster.add_node()
    r2.wait_for_election()
    assert r2.info().get('raft_leader_id') == 1
    commit_idx = cluster.leader_node().commit_index()
    r2.wait_for_commit_index(commit_idx, gt_ok=True)
    r2.wait_for_log_applied()
    assert r2.raft_debug_exec('get', 'key') == b'value'

    # Also validate -MOVED as expected
    with raises(ResponseError, match='MOVED'):
        r2.client.set('key', 'value')


def test_single_node_log_is_reapplied(cluster):
    """Single node log is reapplied on startup"""
    r1 = cluster.add_node()
    assert r1.client.set('key', 'value')
    r1.restart()
    r1.wait_for_election()
    assert r1.info().get('raft_leader_id') == 1
    r1.wait_for_log_applied()
    assert r1.client.get('key') == b'value'


def test_reelection_basic_flow(cluster):
    """
    Basic reelection flow
    """
    cluster.create(3)
    assert cluster.leader == 1
    assert cluster.leader_node().client.set('key', 'value')
    cluster.node(1).terminate()
    cluster.node(2).wait_for_election()
    assert cluster.execute('set', 'key2', 'value')


@pytest.mark.skipif("config.getoption('tls')")
def test_resp3(cluster):
    cluster.create(3)

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    host_ip = socket.gethostbyname('localhost')
    s.connect((host_ip, cluster.node(cluster.leader).port))

    s.send("*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n".encode())
    assert s.recv(1024).decode().startswith("%7")

    s.send(
        "*4\r\n$4\r\nhset\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$3\r\nbaz\r\n".encode())
    assert s.recv(1024).decode().startswith(":1")

    s.send("*2\r\n$7\r\nhgetall\r\n$3\r\nfoo\r\n".encode())
    assert s.recv(1024).decode() == '%1\r\n$3\r\nbar\r\n$3\r\nbaz\r\n'


def test_proxying(cluster):
    """
    Command proxying from follower to leader works
    """
    cluster.create(3)
    assert cluster.leader == 1
    with raises(ResponseError, match='MOVED'):
        assert cluster.node(2).client.set('key', 'value')
    assert cluster.node(2).client.execute_command(
        'CONFIG', 'SET', 'raft.follower-proxy', 'yes') == b'OK'

    # Basic sanity
    assert cluster.node(2).client.set('key', 'value2')
    assert cluster.leader_node().client.get('key') == b'value2'

    # Numeric values
    assert cluster.node(2).client.sadd('myset', 'a') == 1
    assert cluster.node(2).client.sadd('myset', 'b') == 1

    # Multibulk
    assert set(cluster.node(2).client.smembers('myset')) == {b'a', b'b'}

    # Nested multibulk
    assert set(cluster.node(2).client.execute_command(
        'EVAL', 'return {{\'a\',\'b\',\'c\'}};', 0)[0]) == {b'a', b'b', b'c'}
    # Error
    with raises(ResponseError, match='WRONGTYPE'):
        cluster.node(2).client.incr('myset')


def test_readonly_commands(cluster):
    """
    Test read-only command execution, which does not go through the Raft
    log.
    """
    cluster.create(3)
    assert cluster.leader == 1

    # Write something
    assert cluster.node(1).current_index() == 6
    assert cluster.node(1).client.set('key', 'value')
    assert cluster.node(1).current_index() == 7

    # Read something, log should not grow
    assert cluster.node(1).client.get('key') == b'value'
    assert cluster.node(1).current_index() == 7

    # Tear down cluster, reads should hang
    cluster.node(2).terminate()
    cluster.node(3).terminate()
    conn = cluster.node(1).client.connection_pool.get_connection(
        'RAFT', socket_timeout=1)
    conn.send_command('GET', 'key')
    assert not conn.can_read(timeout=1)

    # Now configure non-quorum reads
    cluster.node(1).config_set('raft.quorum-reads', 'no')
    assert cluster.node(1).client.get('key') == b'value'


def test_nonquorum_reads(cluster):
    """
    Test non-quorum reads, requests are not processed until an entry from the
    current term is applied.
    """
    cluster.create(2, raft_args={'quorum-reads': 'no'})

    assert cluster.leader == 1

    # Disable apply on node-2 and make it leader. Node-2 will not be able to
    # apply NOOP entry and read requests should fail.
    cluster.node(2).config_set('raft.log-disable-apply', 'yes')
    cluster.node(2).timeout_now()
    cluster.node(2).wait_for_info_param('raft_leader_id', 2)

    # Read requests are rejected before noop entry is applied
    with raises(ResponseError, match='CLUSTERDOWN'):
        cluster.node(2).client.get('x')

    # Disable delay. The new leader can apply NOOP entry and verify it can
    # process read requests.
    cluster.node(2).config_set('raft.log-disable-apply', 'no')
    cluster.node(2).client.get('x')


def test_nonquorum_read_scripts(cluster):
    """
    Test non-quorum reads with scripts
    """
    cluster.create(2, raft_args={'quorum-reads': 'no'})

    assert cluster.leader == 1

    cluster.execute('set', 'abc', 1234)
    cluster.wait_for_unanimity()

    sha = cluster.execute("script", "load", "return redis.call('GET','abc');")

    function_script = """#!lua name=mylib
    redis.register_function{
    function_name='testfunc',
    callback=function(keys, args) return redis.call('GET','abc') end,
    flags={ 'no-writes' }
    }
    """
    cluster.execute("function", "load", function_script)

    cluster.node(2).pause()

    assert cluster.execute('EVAL_RO', """
        return redis.call('GET','abc');""", '0') == b'1234'

    assert cluster.execute('EVALSHA_RO', sha.decode(), 0) == b'1234'

    assert cluster.execute('FCALL_RO', 'testfunc', 0) == b'1234'


def test_auto_ids(cluster):
    """
    Test automatic assignment of ids.
    """

    # -- Test auto id on create --
    node1 = cluster.add_node(cluster_setup=False, use_id_arg=False)
    node1.start()

    # No id initially
    assert node1.info()['raft_node_id'] == 0

    # Create cluster, id should be generated
    node1.cluster('init')
    assert node1.info()['raft_node_id'] != 0

    # -- Test auto id on join --
    node2 = cluster.add_node(cluster_setup=False, use_id_arg=False)
    node2.start()

    assert node2.info()['raft_node_id'] == 0
    node2.cluster('join', '127.0.0.1:5001')
    node2.wait_for_node_voting()
    assert node2.info()['raft_node_id'] != 0

    # -- Test recovery of id from log after restart --
    _id = node2.info()['raft_node_id']
    node2.restart()
    node2.wait_for_election()
    assert _id == node2.info()['raft_node_id']


def test_interception_does_not_affect_lua(cluster):
    """
    Make sure command interception does not affect Lua commands.
    """

    r1 = cluster.add_node()
    assert r1.info()['raft_current_index'] == 2
    assert r1.client.execute_command('EVAL', """
redis.call('SET','key1','value1');
redis.call('SET','key2','value2');
redis.call('SET','key3','value3');
return 1234;""", '0') == 1234
    assert r1.info()['raft_current_index'] == 3
    assert r1.client.get('key1') == b'value1'
    assert r1.client.get('key2') == b'value2'
    assert r1.client.get('key3') == b'value3'


def test_proxying_with_interception(cluster):
    """
    Test follower proxy mode together with command interception enabled.

    This is a regression test for issues #13, #14 and basically ensures
    that command filtering does not trigger re-entrancy when processing
    a log entry over a thread safe context.
    """
    cluster.create(3)
    assert cluster.leader == 1

    cluster.config_set('raft.follower-proxy', 'yes')

    assert cluster.node(1).client.rpush('list-a', 'x') == 1
    assert cluster.node(1).client.rpush('list-a', 'x') == 2
    assert cluster.node(1).client.rpush('list-a', 'x') == 3

    time.sleep(1)
    assert cluster.node(1).info()['raft_current_index'] == 9


def test_rolled_back_reply(cluster):
    """
    Watch the reply to a write operation that ends up being discarded
    due to election change before commit.
    """

    cluster.create(3)

    cluster.node(2).pause()
    cluster.node(3).pause()

    conn = cluster.node(1).client.connection_pool.get_connection('deferred')
    conn.send_command('INCR', 'key')

    cluster.node(1).pause()

    cluster.node(2).kill()
    cluster.node(3).kill()
    cluster.node(2).start()
    cluster.node(3).start()

    cluster.node(2).wait_for_election()
    cluster.node(1).resume()

    with raises(ResponseError, match='TIMEOUT'):
        conn.read_response()


def test_rolled_back_read_only_reply(cluster):
    """
    Watch the reply to a read-only operation that ends up being discarded
    due to election change before commit.
    """

    cluster.create(3)
    cluster.node(1).client.set('key', 'value')

    cluster.node(2).pause()
    cluster.node(3).pause()

    conn = cluster.node(1).client.connection_pool.get_connection('deferred')
    conn.send_command('GET', 'key')

    cluster.node(1).pause()

    cluster.node(2).kill()
    cluster.node(3).kill()
    cluster.node(2).start()
    cluster.node(3).start()

    cluster.node(2).wait_for_election()
    cluster.node(1).resume()

    with raises(ResponseError, match='TIMEOUT'):
        conn.read_response()


def test_rolled_back_multi_reply(cluster):
    """
    Watch the reply to a MULTI operation that ends up being discarded
    due to election change before commit.
    """

    cluster.create(3)
    cluster.node(1).client.set('key', 'value')

    cluster.node(2).pause()
    cluster.node(3).pause()

    conn = cluster.node(1).client.connection_pool.get_connection('deferred')
    conn.send_command('MULTI')
    assert conn.read_response() == b'OK'
    conn.send_command('INCR', 'key')
    assert conn.read_response() == b'QUEUED'
    conn.send_command('EXEC')

    cluster.node(1).pause()

    cluster.node(2).kill()
    cluster.node(3).kill()
    cluster.node(2).start()
    cluster.node(3).start()

    cluster.node(2).wait_for_election()
    cluster.node(1).resume()

    with raises(ResponseError, match='TIMEOUT'):
        assert conn.read_response() is None


def test_rolled_back_read_only_multi_reply(cluster):
    """
    Watch the reply to a MULTI operation that ends up being discarded
    due to election change before commit.
    """

    cluster.create(3)
    cluster.node(1).client.set('key', 'value')

    cluster.node(2).pause()
    cluster.node(3).pause()

    conn = cluster.node(1).client.connection_pool.get_connection('deferred')
    conn.send_command('MULTI')
    assert conn.read_response() == b'OK'
    conn.send_command('GET', 'key')
    assert conn.read_response() == b'QUEUED'
    conn.send_command('EXEC')

    cluster.node(1).pause()

    cluster.node(2).kill()
    cluster.node(3).kill()
    cluster.node(2).start()
    cluster.node(3).start()

    cluster.node(2).wait_for_election()
    cluster.node(1).resume()

    with raises(ResponseError, match='TIMEOUT'):
        assert conn.read_response() is None


def test_tls_reconfig(cluster):
    if not cluster.config.tls:
        return

    cluster.create(3)

    n1 = cluster.node(1)
    n1.client.execute_command("config", "set", "tls-cert-file", n1.cert)

    cluster.node(2).restart()
    cluster.node(3).restart()
    cluster.node(3).wait_for_election()

    assert cluster.node(3).info().get('raft_leader_id') == 1
    commit_idx = cluster.leader_node().commit_index()
    cluster.node(3).wait_for_commit_index(commit_idx, gt_ok=True)
    cluster.node(3).wait_for_log_applied()


@pytest.mark.skipif("not config.getoption('tls')")
def test_tls_ca_cert_dir(cluster):
    cluster.create(3, cacert_type='dir')
    assert cluster.execute('set', 'key', 'value')
    assert cluster.execute('get', 'key') == b'value'


@pytest.mark.skipif("not config.getoption('tls')")
def test_tls_ca_cert_file(cluster):
    cluster.create(3, cacert_type='file')
    assert cluster.execute('set', 'key', 'value')
    assert cluster.execute('get', 'key') == b'value'


@pytest.mark.skipif("not config.getoption('tls')")
def test_tls_ca_cert_both(cluster):
    cluster.create(3, cacert_type='both')
    assert cluster.execute('set', 'key', 'value')
    assert cluster.execute('get', 'key') == b'value'


def test_appendentries_size_limit(cluster):
    """
    Test appendentries message max size limit.
    """
    cluster.create(1)

    for i in range(10000):
        cluster.execute('set', 'x', 1)

    cluster.config_set('raft.append-req-max-size', '1kb')

    n2 = cluster.add_node()
    cluster.wait_for_unanimity()

    # Verify n2 received multiple appendreq messages rather than just 1
    assert n2.info()['raft_appendreq_with_entry_received'] > 10


def test_throttle_applying_entries(cluster):
    """
    Test slow running operations will not cause timeouts and new elections.
    """
    cluster.create(2)

    # Create some entries.
    for i in range(60):
        cluster.execute('set', 'key', 'value')

    # Restart nodes with log-delay-apply config.
    cluster.terminate()
    cluster.node(1).start()
    cluster.node(1).config_set('raft.log-delay-apply', 100000)
    cluster.node(1).pause()

    cluster.node(2).start()
    cluster.node(2).config_set('raft.log-delay-apply', 100000)

    # Resume node-1, so, nodes will elect a leader and start applying entries.
    cluster.node(1).resume()

    cluster.node(1).wait_for_election()
    term = cluster.node(1).info()['raft_current_term']
    cluster.wait_for_unanimity()

    # Term should stay same as we expect no election will occur.
    assert cluster.node(1).info()['raft_current_term'] == term

    assert cluster.node(1).info()['raft_exec_throttled'] > 0
    assert cluster.node(2).info()['raft_exec_throttled'] > 0


def test_maxmemory(cluster):
    cluster.create(3)

    val = ''.join('1' for _ in range(2000000))

    cluster.execute('set', 'key1', val)
    cluster.config_set('maxmemory', 1)

    msg = "OOM command not allowed when used memory > 'maxmemory'"
    with raises(ResponseError, match=msg):
        cluster.execute('set', 'key2', val)

    assert cluster.execute('get', 'key1').decode('utf8') == val
    cluster.execute('del', 'key1')

    with raises(ResponseError, match=msg):
        cluster.execute('set', 'key1', val)

    cluster.config_set('maxmemory', 0)
    cluster.execute('set', 'key1', val)


def test_maxmemory_with_cluster_ops(cluster):
    """
    Test cluster is operational on OOM: We are able to add/remove nodes,
    transfer leadership and deliver the snapshot.
    """

    cluster.create(1)
    cluster.execute('set', 'x', 1)

    # Create a snapshot
    n1 = cluster.node(1)
    n1.execute('raft.debug', 'exec', 'debug', 'populate', 40000, 'a', 200)
    n1.execute('raft.debug', 'compact')
    n1.config_set('maxmemory', '1')

    # Add new nodes. New nodes will receive the snapshot.
    n2 = cluster.add_node(redis_args=["--maxmemory", "1"])
    n3 = cluster.add_node(redis_args=["--maxmemory", "1"])

    n1.wait_for_num_voting_nodes(3)
    cluster.wait_for_unanimity()
    assert n2.info()['raft_snapshots_received'] == 1
    assert n3.info()['raft_snapshots_received'] == 1

    # Remove the node
    cluster.remove_node(n3.id)
    n1.wait_for_num_nodes(2)

    # Transfer leadership
    assert cluster.leader == 1
    cluster.leader_node().transfer_leader(2)


def test_acl(cluster):
    cluster.create(3)

    cluster.execute('set', 'key', 1)
    cluster.execute('set', 'abc', 1)
    cluster.execute('acl', 'setuser', 'default', 'resetkeys',
                    '(+set', '~key*)', '(+get', '~key*)')
    cluster.execute('get', 'key')
    with (raises(ResponseError, match="No permissions to access a key")):
        cluster.execute('get', 'abc')

    cluster.execute('set', 'key', 2)
    cluster.wait_for_unanimity()
    assert cluster.node(1).raft_debug_exec('get', 'key') == b'2'
    assert cluster.node(2).raft_debug_exec('get', 'key') == b'2'
    assert cluster.node(3).raft_debug_exec('get', 'key') == b'2'

    with (raises(ResponseError, match="No permissions to access a key")):
        cluster.execute('set', 'abc', 2)

    cluster.wait_for_unanimity()
    assert cluster.node(1).raft_debug_exec('get', 'abc') == b'1'
    assert cluster.node(2).raft_debug_exec('get', 'abc') == b'1'
    assert cluster.node(3).raft_debug_exec('get', 'abc') == b'1'

    assert cluster.execute('EVAL', """
    redis.call('SET','key', 3);
    return 1234;""", '0') == 1234
    cluster.wait_for_unanimity()
    assert cluster.node(1).raft_debug_exec('get', 'key') == b'3'
    assert cluster.node(2).raft_debug_exec('get', 'key') == b'3'
    assert cluster.node(3).raft_debug_exec('get', 'key') == b'3'

    msg = "ACL failure in script: No permissions to access a key"
    with (raises(ResponseError, match=msg)):
        cluster.execute('EVAL', """
        redis.call('SET','abc', 3);
        return 1234;""", '0')
    cluster.wait_for_unanimity()
    assert cluster.node(1).raft_debug_exec('get', 'abc') == b'1'
    assert cluster.node(2).raft_debug_exec('get', 'abc') == b'1'
    assert cluster.node(3).raft_debug_exec('get', 'abc') == b'1'

    assert cluster.execute('EVAL_RO', """
    redis.call('GET','key');
    return 1234;""", '0') == 1234

    with (raises(ResponseError, match=msg)):
        cluster.execute('EVAL_RO', """
        redis.call('GET','abc');
        return 1234;""", '0')


def test_ro_permutations(cluster):
    cluster.create(3)

    cluster.execute('set', 'abc', 1234)
    cluster.wait_for_unanimity()

    sha = cluster.execute("script", "load", "return redis.call('GET','abc');")

    function_script = """#!lua name=mylib
    redis.register_function{
    function_name='testfunc',
    callback=function(keys, args) return redis.call('GET','abc') end,
    flags={ 'no-writes' }
    }
    """
    cluster.execute("function", "load", function_script)

    # no oom, acl allow
    assert cluster.execute('EVAL_RO', """
        return redis.call('GET','abc');""", '0') == b'1234'
    assert cluster.execute('EVALSHA_RO', sha.decode(), 0) == b'1234'
    assert cluster.execute('FCALL_RO', 'testfunc', 0) == b'1234'
    assert cluster.execute("get", "abc") == b'1234'

    # oom, acl allow
    cluster.config_set('maxmemory', 1)
    assert cluster.execute('EVAL_RO', """
        return redis.call('GET','abc');""", '0') == b'1234'
    assert cluster.execute('EVALSHA_RO', sha.decode(), 0) == b'1234'
    assert cluster.execute('FCALL_RO', 'testfunc', 0) == b'1234'
    assert cluster.execute("get", "abc") == b'1234'
    cluster.config_set('maxmemory', 0)

    # no oom, acl deny
    cluster.execute('acl', 'setuser', 'default', 'resetkeys')
    with raises(ResponseError, match="No permissions to access a key"):
        cluster.execute('EVAL_RO', "return redis.call('GET','abc');", '0')
    with raises(ResponseError, match="No permissions to access a key"):
        cluster.execute('EVALSHA_RO', sha.decode(), 0)
    with raises(ResponseError, match="No permissions to access a key"):
        cluster.execute('FCALL_RO', 'testfunc', 0)
    with raises(ResponseError, match="No permissions to access a key"):
        cluster.execute("get", "abc")

    # oom, acl deny
    cluster.config_set('maxmemory', 1)
    with raises(ResponseError, match="No permissions to access a key"):
        cluster.execute('EVAL_RO', "return redis.call('GET','abc');", '0')
    with raises(ResponseError, match="No permissions to access a key"):
        cluster.execute('EVALSHA_RO', sha.decode(), 0)
    with raises(ResponseError, match="No permissions to access a key"):
        cluster.execute('FCALL_RO', 'testfunc', 0)
    with raises(ResponseError, match="No permissions to access a key"):
        cluster.execute("get", "abc")
    cluster.config_set('maxmemory', 0)


def test_metadata_restore_after_restart(cluster):
    """
    Test if we restore raft metadata (term and vote) correctly after a restart.
    """
    cluster.create(2)

    n1 = cluster.node(1)
    n2 = cluster.node(2)

    # Let's trigger an election and take the cluster down
    n2.timeout_now()
    n2.wait_for_election()
    cluster.node(1).kill()
    cluster.node(2).kill()

    # Start only node-1, so there won't be a new election
    cluster.node(1).start()
    n1.wait_for_info_param('raft_state', 'up')

    # n2 triggered an election, so n1 should have voted for n2.
    assert n1.info()['raft_current_term'] == 2
    assert n1.info()['raft_voted_for'] == 2

    # Start node-2 only so there won't be a new election
    cluster.node(1).kill()
    cluster.node(2).start()
    n2.wait_for_info_param('raft_state', 'up')

    # n2 triggered an election, so n2 should have voted for itself.
    assert n2.info()['raft_current_term'] == 2
    assert n2.info()['raft_voted_for'] == 2


def test_followers_in_oom(cluster):
    cluster.create(3)

    cluster.node(2).execute('config', 'set', 'maxmemory', 1)
    cluster.node(3).execute('config', 'set', 'maxmemory', 1)

    cluster.execute("set", "abc", 1234)
    cluster.wait_for_unanimity()

    assert cluster.execute("get", "abc") == b'1234'
    assert cluster.node(2).raft_debug_exec("get", "abc") == b'1234'
    assert cluster.node(3).raft_debug_exec("get", "abc") == b'1234'

    cluster.execute('EVAL', """
        redis.call('SET','abc', 5678);
        return 1234;""", '0')
    cluster.wait_for_unanimity()

    assert cluster.execute("get", "abc") == b'5678'
    assert cluster.node(2).raft_debug_exec("get", "abc") == b'5678'
    assert cluster.node(3).raft_debug_exec("get", "abc") == b'5678'


def test_session_counting(cluster):
    cluster.create(3)

    @retry(delay=1, tries=10)
    def assert_num_sessions(val):
        assert cluster.node(1).info()["raft_num_sessions"] == val
        assert cluster.node(2).info()["raft_num_sessions"] == val
        assert cluster.node(3).info()["raft_num_sessions"] == val

    # default, no sessions
    assert_num_sessions(0)

    # normal watch/unwatch session setup tear down
    conn1 = RawConnection(cluster.leader_node().client)
    conn1.execute("WATCH", "X")
    cluster.wait_for_unanimity()

    assert_num_sessions(1)

    conn2 = RawConnection(cluster.leader_node().client)
    conn2.execute("WATCH", "X")
    cluster.wait_for_unanimity()

    assert_num_sessions(2)

    conn1.execute("UNWATCH")
    cluster.wait_for_unanimity()

    assert_num_sessions(1)

    conn2.execute("UNWATCH")
    cluster.wait_for_unanimity()

    assert_num_sessions(0)

    # leader election
    conn1.execute("WATCH", "X")
    conn2.execute("WATCH", "X")
    cluster.wait_for_unanimity()

    assert_num_sessions(2)

    assert cluster.leader == 1
    cluster.node(1).restart()
    cluster.node(1).wait_for_election()
    cluster.wait_for_unanimity()

    assert_num_sessions(0)

    # client disconnect
    cluster.execute("get", "X")  # force update of leader ndode
    conn1 = RawConnection(cluster.leader_node().client)
    conn1.execute("WATCH", "X")
    cluster.wait_for_unanimity()

    assert_num_sessions(1)

    conn1.disconnect()

    assert_num_sessions(0)


def test_session_not_persisting(cluster):
    cluster.create(3)

    @retry(delay=1, tries=10)
    def assert_num_sessions(val):
        assert cluster.node(1).info()["raft_num_sessions"] == val
        assert cluster.node(2).info()["raft_num_sessions"] == val
        assert cluster.node(3).info()["raft_num_sessions"] == val

    assert_num_sessions(0)

    conn1 = RawConnection(cluster.leader_node().client)
    conn2 = RawConnection(cluster.leader_node().client)
    conn1.execute("WATCH", "X")
    cluster.wait_for_unanimity()

    assert_num_sessions(1)

    cluster.leader_node().transfer_leader(2)
    cluster.leader_node().wait_for_election()
    cluster.wait_for_unanimity()

    assert_num_sessions(0)

    cluster.execute("get", "X")

    cluster.leader_node().transfer_leader(1)
    cluster.leader_node().wait_for_election()
    cluster.wait_for_unanimity()

    assert_num_sessions(0)

    # was in the middle of a session
    with raises(ConnectionError, match="Connection (closed|reset)"):
        conn1.execute("get", "X")

    # not in the middle of a session
    conn2.execute("get", "x")


def test_session_same_id_clients_persisting(cluster):
    cluster.create(3)

    @retry(delay=1, tries=10)
    def assert_num_sessions(val):
        assert cluster.node(1).info()["raft_num_sessions"] == val
        assert cluster.node(2).info()["raft_num_sessions"] == val
        assert cluster.node(3).info()["raft_num_sessions"] == val

    assert_num_sessions(0)

    conn1: typing.Optional[RawConnection] = None

    for i in range(100):
        if conn1 is not None:
            conn1.disconnect()
        conn1 = RawConnection(cluster.leader_node().client)

    conn1_id = conn1.get_client_id()

    conn2: typing.Optional[RawConnection] = None
    conn2_id = -1

    while conn2_id < conn1_id:
        if conn2 is not None:
            conn2.disconnect()
        conn2 = RawConnection(cluster.node(2).client)
        conn2_id = conn2.get_client_id()

    assert conn1_id == conn2_id

    conn1.execute("WATCH", "X")
    cluster.wait_for_unanimity()

    assert_num_sessions(1)

    cluster.leader_node().transfer_leader(2)
    cluster.leader_node().wait_for_election()
    cluster.wait_for_unanimity()

    assert_num_sessions(0)

    with raises(ConnectionError, match="Connection (closed|reset)"):
        conn1.execute("get", "X")
    conn2.execute("get", "X")


def test_entry_cache_compaction(cluster):
    """
    Verify entry cache compaction wouldn't free entries that are waiting to be
    applied. If an entry is freed from the cache before apply, user might
    get -TIMEOUT response.
    """
    cluster.create(2)

    # Compaction happens in periodic callback, make it more frequent.
    cluster.config_set("raft.periodic-interval", 1)
    cluster.config_set("raft.log-max-cache-size", "1kb")

    # Replicating a large entry will take longer, and it gives more chances for
    # entry cache compaction to kick in.
    val = "a" * (1024 * 1024 * 64)
    assert cluster.execute("set", "x", val)
