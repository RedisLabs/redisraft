import time
from threading import Thread

from .sandbox import RawConnection


def test_brpop(cluster):
    cluster.create(3)

    def client1():
        val = cluster.leader_node().execute("brpop", "x", 0)
        assert val == (b'x', b'1')

    def client2():
        val = cluster.leader_node().execute("brpop", "x", 0)
        assert val == (b'x', b'2')

    t1 = Thread(target=client1, daemon=True)
    t1.start()
    time.sleep(1)
    t2 = Thread(target=client2, daemon=True)
    t2.start()

    cluster.leader_node().execute("lpush", "x", 1)
    cluster.leader_node().execute("lpush", "x", 2)
    cluster.leader_node().execute("lpush", "x", 3)

    t1.join()
    t2.join()
    cluster.wait_for_unanimity()

    for i in range(1, 3):
        val = cluster.node(i).raft_debug_exec("lrange", "x", 0, -1)
        assert val == [b'3']


def test_blpop(cluster):
    cluster.create(3)

    def client1():
        val = cluster.leader_node().execute("blpop", "x", 0)
        assert val == (b'x', b'1')

    def client2():
        val = cluster.leader_node().execute("blpop", "x", 0)
        assert val == (b'x', b'2')

    t1 = Thread(target=client1, daemon=True)
    t1.start()
    time.sleep(1)
    t2 = Thread(target=client2, daemon=True)
    t2.start()

    cluster.leader_node().execute("lpush", "x", 1)
    cluster.leader_node().execute("lpush", "x", 2)
    cluster.leader_node().execute("lpush", "x", 3)

    t1.join()
    t2.join()
    cluster.wait_for_unanimity()

    for i in range(1, 3):
        val = cluster.node(i).raft_debug_exec("lrange", "x", 0, -1)
        assert val == [b'3']


def test_brpoplpush(cluster):
    cluster.create(3)

    def client1():
        conn = RawConnection(cluster.leader_node().client)
        val = conn.execute("brpoplpush", "x", "y", 0)
        assert val == b'1'

    def client2():
        conn = RawConnection(cluster.leader_node().client)
        val = conn.execute("brpoplpush", "x", "y", 0)
        assert val == b'2'

    t1 = Thread(target=client1, daemon=True)
    t1.start()
    time.sleep(1)
    t2 = Thread(target=client2, daemon=True)
    t2.start()

    cluster.leader_node().execute("lpush", "x", 1)
    cluster.leader_node().execute("lpush", "x", 2)
    cluster.leader_node().execute("lpush", "x", 3)

    t1.join()
    t2.join()
    cluster.wait_for_unanimity()

    for i in range(1, 3):
        val = cluster.node(i).raft_debug_exec("lrange", "y", 0, -1)
        assert val == [b'2', b'1']
        val = cluster.node(i).raft_debug_exec("lrange", "x", 0, -1)
        assert val == [b'3']


def test_blmove(cluster):
    cluster.create(3)

    def client1():
        conn = RawConnection(cluster.leader_node().client)
        val = conn.execute("blmove", "x", "y", "right", "left", 0)
        assert val == b'1'

    def client2():
        conn = RawConnection(cluster.leader_node().client)
        val = conn.execute("blmove", "x", "y", "right", "left", 0)
        assert val == b'2'

    t1 = Thread(target=client1, daemon=True)
    t1.start()
    time.sleep((1))
    t2 = Thread(target=client2, daemon=True)
    t2.start()

    cluster.leader_node().execute("lpush", "x", 1)
    cluster.leader_node().execute("lpush", "x", 2)
    cluster.leader_node().execute("lpush", "x", 3)

    t1.join()
    t2.join()
    cluster.wait_for_unanimity()

    for i in range(1, 3):
        val = cluster.node(i).raft_debug_exec("lrange", "y", 0, -1)
        assert val == [b'2', b'1']
        val = cluster.node(i).raft_debug_exec("lrange", "x", 0, -1)
        assert val == [b'3']


def test_blmpop(cluster):
    cluster.create(3)

    def client1():
        val = cluster.leader_node().execute("blmpop", 0, 2, "x", "y", "left")
        assert val == [b'x', [b'1']]

    def client2():
        val = cluster.leader_node().execute("blmpop", 0, 2, "x", "y", "left")
        assert val == [b'y', [b'2']]

    t1 = Thread(target=client1, daemon=True)
    t1.start()
    time.sleep(1)
    t2 = Thread(target=client2, daemon=True)
    t2.start()

    cluster.leader_node().execute("lpush", "x", 1)
    cluster.leader_node().execute("lpush", "y", 2)
    cluster.leader_node().execute("lpush", "x", 3)

    t1.join()
    t2.join()
    cluster.wait_for_unanimity()

    for i in range(1, 3):
        val = cluster.node(i).raft_debug_exec("lrange", "x", 0, -1)
        assert val == [b'3']


def test_bzpopmin(cluster):
    cluster.create(3)

    def client1():
        val = cluster.leader_node().execute("bzpopmin", "x", "y", 0)
        assert val == (b'x', b'a', 0.0)

    def client2():
        val = cluster.leader_node().execute("bzpopmin", "x", "y", 0)
        assert val == (b'x', b'b', 1.0)

    t1 = Thread(target=client1, daemon=True)
    t1.start()
    time.sleep(1)
    t2 = Thread(target=client2, daemon=True)
    t2.start()

    cluster.leader_node().execute("ZADD", "x", 0, "a", 1, "b", 2, "c")

    t1.join()
    t2.join()
    cluster.wait_for_unanimity()

    for i in range(1, 3):
        val = cluster.node(i).raft_debug_exec("zrange", "x", 0, -1)
        assert val == [b'c']


def test_bzpopmax(cluster):
    cluster.create(3)

    def client1():
        val = cluster.leader_node().execute("bzpopmax", "x", "y", 0)
        assert val == (b'x', b'c', 2.0)

    def client2():
        val = cluster.leader_node().execute("bzpopmax", "x", "y", 0)
        assert val == (b'x', b'b', 1.0)

    t1 = Thread(target=client1, daemon=True)
    t1.start()
    time.sleep(1)
    t2 = Thread(target=client2, daemon=True)
    t2.start()

    cluster.leader_node().execute("ZADD", "x", 0, "a", 1, "b", 2, "c")

    t1.join()
    t2.join()
    cluster.wait_for_unanimity()

    for i in range(1, 3):
        val = cluster.node(i).raft_debug_exec("zrange", "x", 0, -1)
        assert val == [b'a']


def test_bzmpop(cluster):
    cluster.create(3)

    def client1():
        val = cluster.leader_node().execute("bzmpop", 0, 2, "x", "y", "MAX")
        assert val == [b'x', [[b'c', b'2']]]

    def client2():
        val = cluster.leader_node().execute("bzmpop", 0, 2, "x", "y", "MAX")
        assert val == [b'x', [[b'b', b'1']]]

    t1 = Thread(target=client1, daemon=True)
    t1.start()
    time.sleep(1)
    t2 = Thread(target=client2, daemon=True)
    t2.start()

    cluster.leader_node().execute("ZADD", "x", 0, "a", 1, "b", 2, "c")

    t1.join()
    t2.join()
    cluster.wait_for_unanimity()

    for i in range(1, 3):
        val = cluster.node(i).raft_debug_exec("zrange", "x", 0, -1)
        assert val == [b'a']
