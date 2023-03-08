from threading import Thread

from .sandbox import RawConnection


def test_brpop(cluster):
    cluster.create(3)
    s = set()

    def client():
        # val = (b'popped key', b'value popped')
        val = cluster.leader_node().execute("brpop", "x", 0)
        assert val[0] == b'x'
        s.add(val[1])

    t1 = Thread(target=client, daemon=True)
    t1.start()
    t2 = Thread(target=client, daemon=True)
    t2.start()

    cluster.leader_node().execute("lpush", "x", 1)
    cluster.leader_node().execute("lpush", "x", 2)
    cluster.leader_node().execute("lpush", "x", 3)

    t1.join()
    t2.join()
    cluster.wait_for_unanimity()

    assert len(s) == 2
    assert b'1' in s
    assert b'2' in s
    for i in range(1, 3):
        val = cluster.node(i).raft_debug_exec("lrange", "x", 0, -1)
        assert val == [b'3']


def test_blpop(cluster):
    cluster.create(3)

    s = set()

    def client():
        # val = (b'popped key', b'value popped')
        val = cluster.leader_node().execute("blpop", "x", 0)
        assert val[0] == b'x'
        s.add(val[1])

    t1 = Thread(target=client, daemon=True)
    t1.start()
    t2 = Thread(target=client, daemon=True)
    t2.start()

    cluster.leader_node().execute("lpush", "x", 1)
    cluster.leader_node().execute("lpush", "x", 2)
    cluster.leader_node().execute("lpush", "x", 3)

    t1.join()
    t2.join()
    cluster.wait_for_unanimity()

    assert len(s) == 2
    assert b'1' in s
    assert b'2' in s
    for i in range(1, 3):
        val = cluster.node(i).raft_debug_exec("lrange", "x", 0, -1)
        assert val == [b'3']


def test_brpoplpush(cluster):
    cluster.create(3)

    s = set()

    def client():
        # value = popped value from source list
        value = cluster.leader_node().execute("brpoplpush", "x", "y", 0)
        print(f"client value = {value}\n")
        s.add(value)

    t1 = Thread(target=client, daemon=True)
    t1.start()
    t2 = Thread(target=client, daemon=True)
    t2.start()

    cluster.leader_node().execute("lpush", "x", 1)
    cluster.leader_node().execute("lpush", "x", 2)
    cluster.leader_node().execute("lpush", "x", 3)

    t1.join()
    t2.join()
    cluster.wait_for_unanimity()

    assert len(s) == 2
    assert b'1' in s
    assert b'2' in s
    for i in range(1, 3):
        val = cluster.node(i).raft_debug_exec("lrange", "y", 0, -1)
        assert val == [b'2', b'1']
        val = cluster.node(i).raft_debug_exec("lrange", "x", 0, -1)
        assert val == [b'3']


def test_blmove(cluster):
    cluster.create(3)
    s = set()

    def client():
        # val = popped value from source list
        n = cluster.leader_node()
        val = n.execute("blmove", "x", "y", "right", "left", 0)
        s.add(val)

    t1 = Thread(target=client, daemon=True)
    t1.start()
    t2 = Thread(target=client, daemon=True)
    t2.start()

    cluster.leader_node().execute("lpush", "x", 1)
    cluster.leader_node().execute("lpush", "x", 2)
    cluster.leader_node().execute("lpush", "x", 3)

    t1.join()
    t2.join()
    cluster.wait_for_unanimity()

    assert len(s) == 2
    assert b'1' in s
    assert b'2' in s
    for i in range(1, 3):
        val = cluster.node(i).raft_debug_exec("lrange", "y", 0, -1)
        assert val == [b'2', b'1']
        val = cluster.node(i).raft_debug_exec("lrange", "x", 0, -1)
        assert val == [b'3']


def test_blmpop(cluster):
    cluster.create(3)
    s1 = set()
    s2 = set()

    def client():
        # val = [key popped from, [list of elements popped]
        val = cluster.leader_node().execute("blmpop", 0, 2, "x", "y", "left")
        if val[0] == b'x':
            s1.add(val[1][0])
        elif val[0] == b'y':
            s2.add(val[1][0])

    t1 = Thread(target=client, daemon=True)
    t1.start()
    t2 = Thread(target=client, daemon=True)
    t2.start()

    cluster.leader_node().execute("lpush", "x", 1)
    cluster.leader_node().execute("lpush", "y", 2)
    cluster.leader_node().execute("lpush", "x", 3)

    t1.join()
    t2.join()
    cluster.wait_for_unanimity()

    assert len(s1) == 1
    assert b'1' in s1
    assert len(s2) == 1
    assert b'2' in s2
    for i in range(1, 3):
        val = cluster.node(i).raft_debug_exec("lrange", "x", 0, -1)
        assert val == [b'3']
    for i in range(1, 3):
        val = cluster.node(i).raft_debug_exec("lrange", "y", 0, -1)
        assert val == []


def test_bzpopmin(cluster):
    cluster.create(3)
    s1 = set()
    s2 = set()

    def client():
        # val = (set popped from, set element popped, set element value popped)
        val = cluster.leader_node().execute("bzpopmin", "x", "y", 0)
        assert val[0] == b'x'
        if val[1] == b'a':
            s1.add(val[2])
        elif val[1] == b'b':
            s2.add(val[2])

    t1 = Thread(target=client, daemon=True)
    t1.start()
    t2 = Thread(target=client, daemon=True)
    t2.start()

    cluster.leader_node().execute("ZADD", "x", 0, "a", 1, "b", 2, "c")

    t1.join()
    t2.join()
    cluster.wait_for_unanimity()

    assert len(s1) == 1
    assert 0.0 in s1
    assert len(s2) == 1
    assert 1.0 in s2
    for i in range(1, 3):
        val = cluster.node(i).raft_debug_exec("zrange", "x", 0, -1)
        assert val == [b'c']


def test_bzpopmax(cluster):
    cluster.create(3)
    s1 = set()
    s2 = set()

    def client():
        # val = (set popped from, set element popped, set element value popped)
        val = cluster.leader_node().execute("bzpopmax", "x", "y", 0)
        assert val[0] == b'x'
        if val[1] == b'c':
            s1.add(val[2])
        elif val[1] == b'b':
            s2.add(val[2])

    t1 = Thread(target=client, daemon=True)
    t1.start()
    t2 = Thread(target=client, daemon=True)
    t2.start()

    cluster.leader_node().execute("ZADD", "x", 0, "a", 1, "b", 2, "c")

    t1.join()
    t2.join()
    cluster.wait_for_unanimity()

    assert len(s1) == 1
    assert 2.0 in s1
    assert len(s2) == 1
    assert 1.0 in s2
    for i in range(1, 3):
        val = cluster.node(i).raft_debug_exec("zrange", "x", 0, -1)
        assert val == [b'a']


def test_bzmpop(cluster):
    cluster.create(3)
    s1 = set()
    s2 = set()

    def client():
        val = cluster.leader_node().execute("bzmpop", 0, 2, "x", "y", "MAX")
        assert val[0] == b'x'
        if val[1][0][0] == b'c':
            s1.add(val[1][0][1])
        elif val[1][0][0] == b'b':
            s2.add(val[1][0][1])

    t1 = Thread(target=client, daemon=True)
    t1.start()
    t2 = Thread(target=client, daemon=True)
    t2.start()

    cluster.leader_node().execute("ZADD", "x", 0, "a", 1, "b", 2, "c")

    t1.join()
    t2.join()
    cluster.wait_for_unanimity()

    assert len(s1) == 1
    assert b'2' in s1
    assert len(s2) == 1
    assert b'1' in s2
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
    s = set()

    def client():
        # val = (b'popped key', b'value popped')
        val = cluster.leader_node().execute("brpop", "x", 0)
        assert val[0] == b'x'
        s.add(val[1])

    t1 = Thread(target=client, daemon=True)
    t1.start()
    t2 = Thread(target=client, daemon=True)
    t2.start()

    cluster.node(2).client.execute_command('raft.debug', 'compact')
    cluster.node(3).client.execute_command('raft.debug', 'compact')
    cluster.node(2).restart()
    cluster.node(3).restart()
    cluster.node(2).wait_for_info_param('raft_state', 'up')
    cluster.node(3).wait_for_info_param('raft_state', 'up')

    cluster.leader_node().execute("lpush", "x", 1)
    cluster.leader_node().execute("lpush", "x", 2)
    cluster.leader_node().execute("lpush", "x", 3)

    t1.join()
    t2.join()
    cluster.wait_for_unanimity()

    assert len(s) == 2
    assert b'1' in s
    assert b'2' in s
    for i in range(1, 3):
        val = cluster.node(i).raft_debug_exec("lrange", "x", 0, -1)
        assert val == [b'3']
