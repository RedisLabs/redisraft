import time

import pytest
from _pytest.python_api import raises

from redis.exceptions import ConnectionError, ResponseError
from .sandbox import RawConnection


def test_brpop(cluster):
    cluster.create(3)

    c1 = cluster.leader_node().client.connection_pool.get_connection('c1')
    c1.send_command('blpop', 'x', 0)
    c2 = cluster.leader_node().client.connection_pool.get_connection('c2')
    c2.send_command('blpop', 'x', 0)

    cluster.leader_node().execute("lpush", "x", 1)
    cluster.leader_node().execute("lpush", "x", 2)
    cluster.leader_node().execute("lpush", "x", 3)

    cluster.wait_for_unanimity()

    assert c1.read_response() == [b'x', b'1']
    assert c2.read_response() == [b'x', b'2']

    for i in range(1, 3):
        val = cluster.node(i).raft_debug_exec("lrange", "x", 0, -1)
        assert val == [b'3']


def test_blpop(cluster):
    cluster.create(3)

    c1 = cluster.leader_node().client.connection_pool.get_connection('c1')
    c1.send_command('brpop', 'x', 0)
    c2 = cluster.leader_node().client.connection_pool.get_connection('c2')
    c2.send_command('brpop', 'x', 0)

    cluster.leader_node().execute("lpush", "x", 1)
    cluster.leader_node().execute("lpush", "x", 2)
    cluster.leader_node().execute("lpush", "x", 3)

    cluster.wait_for_unanimity()

    assert c1.read_response() == [b'x', b'1']
    assert c2.read_response() == [b'x', b'2']

    for i in range(1, 3):
        val = cluster.node(i).raft_debug_exec("lrange", "x", 0, -1)
        assert val == [b'3']


def test_brpoplpush(cluster):
    cluster.create(3)

    c1 = cluster.leader_node().client.connection_pool.get_connection('c1')
    c1.send_command('brpoplpush', 'x', 'y', 0)
    c2 = cluster.leader_node().client.connection_pool.get_connection('c2')
    c2.send_command('brpoplpush', 'x', 'y', 0)

    cluster.leader_node().execute("lpush", "x", 1)
    cluster.leader_node().execute("lpush", "x", 2)
    cluster.leader_node().execute("lpush", "x", 3)

    cluster.wait_for_unanimity()

    assert c1.read_response() == b'1'
    assert c2.read_response() == b'2'

    for i in range(1, 3):
        val = cluster.node(i).raft_debug_exec("lrange", "y", 0, -1)
        assert val == [b'2', b'1']
        val = cluster.node(i).raft_debug_exec("lrange", "x", 0, -1)
        assert val == [b'3']


def test_blmove(cluster):
    cluster.create(3)

    c1 = cluster.leader_node().client.connection_pool.get_connection('c1')
    c1.send_command("blmove", "x", "y", "right", "left", 0)
    c2 = cluster.leader_node().client.connection_pool.get_connection('c2')
    c2.send_command("blmove", "x", "y", "right", "left", 0)

    cluster.leader_node().execute("lpush", "x", 1)
    cluster.leader_node().execute("lpush", "x", 2)
    cluster.leader_node().execute("lpush", "x", 3)

    cluster.wait_for_unanimity()

    assert c1.read_response() == b'1'
    assert c2.read_response() == b'2'

    for i in range(1, 3):
        val = cluster.node(i).raft_debug_exec("lrange", "y", 0, -1)
        assert val == [b'2', b'1']
        val = cluster.node(i).raft_debug_exec("lrange", "x", 0, -1)
        assert val == [b'3']


def test_blmpop(cluster):
    cluster.create(3)

    c1 = cluster.leader_node().client.connection_pool.get_connection('c1')
    c1.send_command("blmpop", 0, 2, "x", "y", "left")
    c2 = cluster.leader_node().client.connection_pool.get_connection('c2')
    c2.send_command("blmpop", 0, 2, "x", "y", "left")

    cluster.leader_node().execute("lpush", "x", 1)
    cluster.leader_node().execute("lpush", "y", 2)
    cluster.leader_node().execute("lpush", "x", 3)

    cluster.wait_for_unanimity()

    assert c1.read_response() == [b'x', [b'1']]
    assert c2.read_response() == [b'y', [b'2']]

    for i in range(1, 3):
        val = cluster.node(i).raft_debug_exec("lrange", "x", 0, -1)
        assert val == [b'3']
    for i in range(1, 3):
        val = cluster.node(i).raft_debug_exec("lrange", "y", 0, -1)
        assert val == []


def test_bzpopmin(cluster):
    cluster.create(3)

    c1 = cluster.leader_node().client.connection_pool.get_connection('c1')
    c1.send_command("bzpopmin", "x", "y", 0)
    c2 = cluster.leader_node().client.connection_pool.get_connection('c2')
    c2.send_command("bzpopmin", "x", "y", 0)

    cluster.leader_node().execute("ZADD", "x", 0, "a", 1, "b", 2, "c")

    cluster.wait_for_unanimity()

    assert c1.read_response() == [b'x', b'a', b'0']
    assert c2.read_response() == [b'x', b'b', b'1']

    for i in range(1, 3):
        val = cluster.node(i).raft_debug_exec("zrange", "x", 0, -1)
        assert val == [b'c']


def test_bzpopmax(cluster):
    cluster.create(3)

    c1 = cluster.leader_node().client.connection_pool.get_connection('c1')
    c1.send_command("bzpopmax", "x", "y", 0)
    c2 = cluster.leader_node().client.connection_pool.get_connection('c2')
    c2.send_command("bzpopmax", "x", "y", 0)

    cluster.leader_node().execute("ZADD", "x", 0, "a", 1, "b", 2, "c")

    cluster.wait_for_unanimity()

    assert c1.read_response() == [b'x', b'c', b'2']
    assert c2.read_response() == [b'x', b'b', b'1']

    for i in range(1, 3):
        val = cluster.node(i).raft_debug_exec("zrange", "x", 0, -1)
        assert val == [b'a']


def test_bzmpop(cluster):
    cluster.create(3)

    c1 = cluster.leader_node().client.connection_pool.get_connection('c1')
    c1.send_command("bzmpop", 0, 2, "x", "y", "MAX")
    c2 = cluster.leader_node().client.connection_pool.get_connection('c2')
    c2.send_command("bzmpop", 0, 2, "x", "y", "MAX")

    cluster.leader_node().execute("ZADD", "x", 0, "a", 1, "b", 2, "c")

    cluster.wait_for_unanimity()

    assert c1.read_response() == [b'x', [[b'c', b'2']]]
    assert c2.read_response() == [b'x', [[b'b', b'1']]]

    for i in range(1, 3):
        val = cluster.node(i).raft_debug_exec("zrange", "x", 0, -1)
        assert val == [b'a']


def test_multi_block_without_data(cluster):
    cluster.create(3)

    conn = RawConnection(cluster.node(1).client)

    assert conn.execute('multi') == b'OK'
    assert conn.execute('brpop', 'x', 0) == b'QUEUED'
    assert conn.execute('exec') == [None]


def test_multi_block_with_data(cluster):
    cluster.create(3)

    cluster.leader_node().execute("lpush", "x", 1)

    conn = RawConnection(cluster.node(1).client)

    assert conn.execute('multi') == b'OK'
    assert conn.execute('brpop', 'x', 0) == b'QUEUED'
    assert conn.execute('exec') == [[b'x', b'1']]


def test_blocking_with_snapshot(cluster):
    cluster.create(3)

    c1 = cluster.leader_node().client.connection_pool.get_connection('c1')
    c1.send_command("brpop", "x", 0)
    c2 = cluster.leader_node().client.connection_pool.get_connection('c2')
    c2.send_command("brpop", "x", 0)

    cluster.node(2).client.execute_command('raft.debug', 'compact')
    cluster.node(3).client.execute_command('raft.debug', 'compact')
    cluster.node(2).restart()
    cluster.node(3).restart()
    cluster.node(2).wait_for_info_param('raft_state', 'up')
    cluster.node(3).wait_for_info_param('raft_state', 'up')

    cluster.leader_node().execute("lpush", "x", 1)
    cluster.leader_node().execute("lpush", "x", 2)
    cluster.leader_node().execute("lpush", "x", 3)

    cluster.wait_for_unanimity()

    assert c1.read_response() == [b'x', b'1']
    assert c2.read_response() == [b'x', b'2']

    for i in range(1, 3):
        val = cluster.node(i).raft_debug_exec("lrange", "x", 0, -1)
        assert val == [b'3']


def test_blocking_with_timeout(cluster):
    cluster.create(3)

    c1 = cluster.leader_node().client.connection_pool.get_connection('c1')
    c1.send_command("brpop", "x", 1)
    c2 = cluster.leader_node().client.connection_pool.get_connection('c2')
    c2.send_command("brpop", "x", 10)

    assert c1.read_response() is None

    cluster.leader_node().execute("lpush", "x", 1)
    cluster.leader_node().execute("lpush", "x", 2)
    cluster.leader_node().execute("lpush", "x", 3)

    assert c2.read_response() == [b'x', b'1']

    cluster.wait_for_unanimity()

    for i in range(1, 3):
        val = cluster.node(i).raft_debug_exec("lrange", "x", 0, -1)
        assert val == [b'3', b'2']


def test_blocking_with_term_change(cluster):
    cluster.create(3)

    c1 = cluster.leader_node().client.connection_pool.get_connection('c1')
    c1.send_command("brpop", "x", 0)
    c2 = cluster.leader_node().client.connection_pool.get_connection('c2')
    c2.send_command("brpop", "x", 0)

    cluster.node(1).restart()

    with raises(ConnectionError, match="Connection (closed|reset)"):
        c1.read_response()
    with raises(ConnectionError, match="Connection (closed|reset)"):
        c2.read_response()

    cluster.node(1).wait_for_info_param('raft_state', 'up')
    cluster.node(1).wait_for_election()

    cluster.execute("lpush", "x", 1)
    cluster.execute("lpush", "x", 2)
    cluster.execute("lpush", "x", 3)

    cluster.wait_for_unanimity()

    for i in range(1, 3):
        val = cluster.node(i).raft_debug_exec("lrange", "x", 0, -1)
        assert val == [b'3', b'2', b'1']


def test_blocking_with_key_rename(cluster):
    cluster.create(3)

    c1 = cluster.leader_node().client.connection_pool.get_connection('c1')
    c1.send_command("brpop", "x", 0)

    cluster.execute("lpush", "y", "1")
    cluster.execute("lpush", "y", "2")
    cluster.execute("lpush", "y", "3")
    cluster.execute("rename", "y", "x")

    cluster.wait_for_unanimity()

    assert c1.read_response() == [b'x', b'1']

    for i in range(1, 3):
        val = cluster.node(i).raft_debug_exec("lrange", "x", 0, -1)
        assert val == [b'3', b'2']


def test_blocking_with_invalid_timeout(cluster):
    cluster.create(3)

    with raises(ResponseError, match="timeout is not a float or out of range"):
        cluster.execute("blpop", "x", "a")

    with raises(ResponseError, match="timeout is negative"):
        cluster.execute("blpop", "x", "-1")

    with raises(ResponseError, match="timeout is out of range"):
        cluster.execute("blpop", "x", "0x7FFFFFFFFFFFFF")


def test_blocking_with_unblock(cluster):
    cluster.create(3)

    c1 = cluster.leader_node().client.connection_pool.get_connection('c1')
    c1.send_command("client", "id")
    client_id = c1.read_response()

    c1.send_command("brpop", "x", 0)

    cluster.execute("client", "unblock", client_id)

    assert c1.read_response() is None


def test_blocking_with_unblock_error(cluster):
    cluster.create(3)

    c1 = cluster.leader_node().client.connection_pool.get_connection('c1')
    c1.send_command("client", "id")
    client_id = c1.read_response()

    c1.send_command("brpop", "x", 0)

    cluster.execute("client", "unblock", client_id, "error")

    with raises(ResponseError,
                match="UNBLOCKED client unblocked via CLIENT UNBLOCK"):
        c1.read_response()

    # ensure that log plays correctly after restart
    cluster.node(2).restart()
    cluster.node(3).restart()

    cluster.execute("lpush", "x", 1)

    cluster.wait_for_unanimity()

    for i in range(1, 3):
        val = cluster.node(i).raft_debug_exec("lrange", "x", 0, -1)
        assert val == [b'1']


def test_blocking_with_disconnect(cluster):
    cluster.create(3)

    c1 = cluster.leader_node().client.connection_pool.get_connection('c1')
    c1.send_command("brpop", "x", 0)
    c1.disconnect()
    cluster.leader_node().client.connection_pool.release(c1)

    cluster.wait_for_unanimity()

    cluster.execute("lpush", "x", "1")

    cluster.wait_for_unanimity()

    for i in range(1, 3):
        val = cluster.node(i).raft_debug_exec("lrange", "x", 0, -1)
        assert val == [b'1']


@pytest.mark.skip(reason="skipping, as promise/handler isn't atomic "
                         "and abort is not working as expected")
def test_blocking_with_timeout_after_unblock(cluster):
    cluster.create(3)

    c1 = cluster.leader_node().client.connection_pool.get_connection('c1')
    c2 = cluster.leader_node().client.connection_pool.get_connection('c2')
    c3 = cluster.leader_node().client.connection_pool.get_connection('c3')

    c1.send_command("client", "id")
    client_id = c1.read_response()

    c1.send_command("brpop", "x", 0)

    cluster.wait_for_unanimity()
    time.sleep(1)

    cluster.config_set("raft.log-disable-apply", "yes")

    c2.send_command("lpush", "x", 1)
    c3.send_command("client", "unblock", client_id)

    cluster.config_set("raft.log-disable-apply", "no")

    assert c1.read_response() == [b'x', b'1']
    assert c2.read_response()
    assert c3.read_response() == 0
    cluster.wait_for_unanimity()

    for i in range(1, 3):
        val = cluster.node(i).raft_debug_exec("lrange", "x", 0, -1)
        assert type(val) == list
        assert len(val) == 0
