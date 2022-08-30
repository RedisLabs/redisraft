"""
This file is part of RedisRaft.

Copyright (c) 2020-2021 Redis Ltd.

RedisRaft is licensed under the Redis Source Available License (RSAL).
"""
import socket

import pytest as pytest
import time
from redis import ResponseError
from pytest import raises
from .sandbox import RedisRaft, RedisRaftFailedToStart


def test_info_before_cluster_init(cluster):
    node = RedisRaft(1, cluster.base_port, cluster.config)
    node.start()
    assert node.info()["raft_state"] == "uninitialized"
    assert node.client.execute_command("info")["module"]["name"] == "raft"


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
    cluster_id = "123456789012345678901234567890"
    with raises(ResponseError, match='invalid cluster message \(cluster id length\)'):
        cluster.create(3, cluster_id=cluster_id)


def test_set_cluster_id_too_long(cluster):
    cluster_id = "1234567890123456789012345678901234"
    with raises(ResponseError, match='invalid cluster message \(cluster id length\)'):
        cluster.create(3, cluster_id=cluster_id)


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
    cluster.node(2).client.execute_command('raft.debug', 'disable_apply', '1')
    cluster.node(2).timeout_now()
    cluster.node(2).wait_for_info_param('raft_leader_id', 2)

    # Read requests are rejected before noop entry is applied
    with raises(ResponseError, match='CLUSTERDOWN'):
        cluster.node(2).client.get('x')

    # Disable delay. The new leader can apply NOOP entry and verify it can
    # process read requests.
    cluster.node(2).client.execute_command('raft.debug', 'disable_apply', '0')
    cluster.node(2).client.get('x')


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

    assert cluster.node(1).client.execute_command(
        'CONFIG', 'SET', 'raft.follower-proxy', 'yes') == b'OK'
    assert cluster.node(2).client.execute_command(
        'CONFIG', 'SET', 'raft.follower-proxy', 'yes') == b'OK'
    assert cluster.node(3).client.execute_command(
        'CONFIG', 'SET', 'raft.follower-proxy', 'yes') == b'OK'

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
    cluster.create(3, tls_ca_cert_location='dir')
    assert cluster.execute('set', 'key', 'value')
    assert cluster.execute('get', 'key') == b'value'


@pytest.mark.skipif("not config.getoption('tls')")
def test_tls_ca_cert_file(cluster):
    cluster.create(3, tls_ca_cert_location='file')
    assert cluster.execute('set', 'key', 'value')
    assert cluster.execute('get', 'key') == b'value'


@pytest.mark.skipif("not config.getoption('tls')")
def test_tls_ca_cert_both(cluster):
    cluster.create(3, tls_ca_cert_location='both')
    assert cluster.execute('set', 'key', 'value')
    assert cluster.execute('get', 'key') == b'value'


def test_appendentries_size_limit(cluster):
    """
    Test appendentries message max size limit.
    """
    cluster.create(1)

    for i in range(10000):
        cluster.execute('set', 'x', 1)

    cluster.node(1).execute('config', 'set', 'raft.append-req-max-size', '1kb')

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

    # Restart nodes with delay_apply config.
    cluster.terminate()
    cluster.node(1).start()
    cluster.node(1).execute('raft.debug', 'delay_apply', 100000)
    cluster.node(1).pause()

    cluster.node(2).start()
    cluster.node(2).execute('raft.debug', 'delay_apply', 100000)

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
    cluster.execute('config', 'set', 'maxmemory', 100000)

    with (raises(ResponseError, match="OOM command not allowed when used memory > 'maxmemory'")):
        cluster.execute('set', 'key2', val)

    assert cluster.execute('get', 'key1').decode('utf8') == val
    cluster.execute('del', 'key1')

    with (raises(ResponseError, match="OOM command not allowed when used memory > 'maxmemory'")):
        cluster.execute('set', 'key1', val)

    cluster.execute('config', 'set', 'maxmemory', 4000000000)
    cluster.execute('set', 'key1', val)


def test_acl(cluster):
    cluster.create(3)

    cluster.execute('set', 'key', 1)
    cluster.execute('set', 'abc', 1)
    cluster.execute('acl', 'setuser', 'default', 'resetkeys', '(+set', '~key*)', '(+get', '~key*)')
    cluster.execute('get', 'key')
    with (raises(ResponseError, match="User default has no permissions to access the 'abc' key")):
        cluster.execute('get', 'abc')

    cluster.execute('set', 'key', 2)
    commit_idx = cluster.leader_node().commit_index()
    assert cluster.node(1).raft_debug_exec('get', 'key') == b'2'
    cluster.node(2).wait_for_commit_index(commit_idx, gt_ok=True)
    cluster.node(2).wait_for_log_applied()
    assert cluster.node(2).raft_debug_exec('get', 'key') == b'2'
    cluster.node(3).wait_for_commit_index(commit_idx, gt_ok=True)
    cluster.node(3).wait_for_log_applied()
    assert cluster.node(3).raft_debug_exec('get', 'key') == b'2'

    with (raises(ResponseError, match="User default has no permissions to access the 'abc' key")):
        cluster.execute('set', 'abc', 2)

    commit_idx = cluster.leader_node().commit_index()
    assert cluster.node(1).raft_debug_exec('get', 'abc') == b'1'
    cluster.node(2).wait_for_commit_index(commit_idx, gt_ok=True)
    cluster.node(2).wait_for_log_applied()
    assert cluster.node(2).raft_debug_exec('get', 'abc') == b'1'
    cluster.node(3).wait_for_commit_index(commit_idx, gt_ok=True)
    cluster.node(3).wait_for_log_applied()
    assert cluster.node(3).raft_debug_exec('get', 'abc') == b'1'

    assert cluster.execute('EVAL', """
    redis.call('SET','key', 3);
    return 1234;""", '0') == 1234
    commit_idx = cluster.leader_node().commit_index()
    assert cluster.node(1).raft_debug_exec('get', 'key') == b'3'
    cluster.node(2).wait_for_commit_index(commit_idx, gt_ok=True)
    cluster.node(2).wait_for_log_applied()
    assert cluster.node(2).raft_debug_exec('get', 'key') == b'3'
    cluster.node(3).wait_for_commit_index(commit_idx, gt_ok=True)
    cluster.node(3).wait_for_log_applied()
    assert cluster.node(3).raft_debug_exec('get', 'key') == b'3'

    with (raises(ResponseError, match="ACL failure in script: User redis_raft1 has no permissions to access the 'abc' key")):
        cluster.execute('EVAL', """
        redis.call('SET','abc', 3);
        return 1234;""", '0')
    commit_idx = cluster.leader_node().commit_index()
    assert cluster.node(1).raft_debug_exec('get', 'abc') == b'1'
    cluster.node(2).wait_for_commit_index(commit_idx, gt_ok=True)
    cluster.node(2).wait_for_log_applied()
    assert cluster.node(2).raft_debug_exec('get', 'abc') == b'1'
    cluster.node(3).wait_for_commit_index(commit_idx, gt_ok=True)
    cluster.node(3).wait_for_log_applied()
    assert cluster.node(3).raft_debug_exec('get', 'abc') == b'1'


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

    # n2 triggerred an election, so n1 should have voted for n2.
    assert n1.info()['raft_current_term'] == 2
    assert n1.info()['raft_voted_for'] == 2

    # Start node-2 only so there won't be a new election
    cluster.node(1).kill()
    cluster.node(2).start()
    n2.wait_for_info_param('raft_state', 'up')

    # n2 triggerred an election, so n2 should have voted for itself.
    assert n2.info()['raft_current_term'] == 2
    assert n2.info()['raft_voted_for'] == 2

