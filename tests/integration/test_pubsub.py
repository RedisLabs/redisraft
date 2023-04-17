import time

import redis
from pytest import raises
from redis import ResponseError


def test_pubsub_sanity(cluster):
    """
    Test basic functionality of PUBLISH, SUBSCRIBE and PSUBSCRIBE commands
    """
    cluster.create(1)

    c1 = cluster.node(1).client

    ps1 = c1.pubsub(ignore_subscribe_messages=True)
    ps1.subscribe("chan1")

    c1.publish("chan1", "hello")

    while True:
        msg = ps1.get_message()
        if not msg:
            time.sleep(0.1)
            continue

        assert msg["channel"] == b"chan1"
        assert msg["data"] == b"hello"
        break

    ps1.psubscribe("pchan?")
    c1.publish("pchan1", "hello")

    while True:
        msg = ps1.get_message()
        if not msg:
            time.sleep(0.1)
            continue

        assert msg["channel"] == b"pchan1"
        assert msg["data"] == b"hello"
        break


def test_pubsub_followers(cluster):
    """
    Test SUBSCRIBE, SSUBSCRIBE and PSUBSCRIBE receive -MOVED reply on followers
    """
    cluster.create(3)

    c2 = cluster.node(2).client
    ps2 = c2.pubsub(ignore_subscribe_messages=True)

    with raises(ResponseError, match='MOVED'):
        ps2.subscribe("chan1")

        while True:
            msg = ps2.get_message()
            if not msg:
                time.sleep(0.1)
                continue
            break

    with raises(ResponseError, match='MOVED'):
        ps2.psubscribe("chan1?")

        while True:
            msg = ps2.get_message()
            if not msg:
                time.sleep(0.1)
                continue
            break

    with raises(ResponseError, match='MOVED'):
        c2.execute_command("ssubscribe", "chan1?")


def test_pubsub_subcommand(cluster):
    """
    Test PUBSUB subcommands
    """
    cluster.create(3)

    c1 = cluster.node(1).client
    ps1 = c1.pubsub(ignore_subscribe_messages=True)
    ps1.subscribe("chan1")
    ps1.psubscribe("h?llo")

    # Test regular channels
    assert cluster.execute("pubsub", "channels") == [b"chan1"]
    assert cluster.execute("pubsub", "numpat") == 1
    subs = cluster.execute("pubsub", "numsub", b"chan1", "hallo")
    assert subs == [b"chan1", 1, b"hallo", 0]

    # Test shard channels
    conn1 = c1.connection_pool.get_connection('c1')
    conn1.send_command("ssubscribe", "chan2")

    assert c1.execute_command("pubsub", "shardchannels") == [b"chan2"]
    subs = c1.execute_command("pubsub", "shardnumsub", b"chan2", "hallo")
    assert subs == [b"chan2", 1, b"hallo", 0]


def test_pubsub_leader_change(cluster):
    """
    Test subscriber's connection is closed if node becomes a follower
    """
    cluster.create(3)

    c1 = cluster.node(1).client
    ps1 = c1.pubsub(ignore_subscribe_messages=True)
    ps1.subscribe("chan1")

    cluster.node(1).transfer_leader()

    # Connection will be closed
    with raises(redis.exceptions.ConnectionError):
        while True:
            msg = ps1.get_message()
            if not msg:
                time.sleep(0.1)
                continue
            break


def test_pubsub_log_replay_with_multi(cluster):
    """
    Test SUBSCRIBE is not allowed until log replay is completed.
    """
    cluster.create(1)

    c1 = cluster.node(1).client

    ps1 = c1.pubsub(ignore_subscribe_messages=True)
    ps1.subscribe("chan1")

    c1.execute_command("multi")
    c1.execute_command("set", "x", 1)  # Make multi a write command
    c1.execute_command("publish", "chan1", "first")
    c1.execute_command("publish", "chan1", "second")
    c1.execute_command("exec")

    # Verify subscriber receives pubsub
    while True:
        msg = ps1.get_message()
        if not msg:
            time.sleep(0.1)
            continue

        assert msg["channel"] == b"chan1"
        assert msg["data"] == b"first"

        msg = ps1.get_message()
        assert msg["channel"] == b"chan1"
        assert msg["data"] == b"second"
        break

    # Restart node but prevent log replay
    cluster.node(1).terminate()
    cluster.node(1).start(extra_raft_args=["--raft.log-disable-apply", "yes"])
    cluster.node(1).wait_for_election()

    # Previous subscription should get an error
    with raises(redis.exceptions.ConnectionError):
        ps1.get_message()

    # Subscribe is not allowed until log is replayed
    with raises(redis.exceptions.ResponseError, match="CLUSTERDOWN"):
        ps2 = c1.pubsub(ignore_subscribe_messages=True)
        ps2.subscribe("chan1")

        while True:
            msg = ps2.get_message()
            if not msg:
                time.sleep(0.1)
                continue

    # Replay logs
    cluster.node(1).config_set("raft.log-disable-apply", "no")
    cluster.wait_for_unanimity()

    # Subscribe should be allowed now
    ps3 = c1.pubsub(ignore_subscribe_messages=True)
    ps3.subscribe("chan3")
    c1.publish("chan3", "third")

    while True:
        msg = ps3.get_message()
        if not msg:
            time.sleep(0.1)
            continue

        assert msg["channel"] == b"chan3"
        assert msg["data"] == b"third"
        break


def test_pubsub_log_replay_with_lua(cluster):
    """
    Basic test of PUBLISH in a lua script.
    """
    cluster.create(2)

    c1 = cluster.node(1).client

    ps1 = c1.pubsub(ignore_subscribe_messages=True)
    ps1.subscribe("ch1")
    c1.execute_command("EVAL", """redis.call('PUBLISH','ch1','hello');""", '0')

    while True:
        msg = ps1.get_message()
        if not msg:
            time.sleep(0.1)
            continue

        assert msg["channel"] == b"ch1"
        assert msg["data"] == b"hello"
        break

    cluster.node(1).transfer_leader()

    with raises(redis.exceptions.ConnectionError):
        while True:
            msg = ps1.get_message()
            if not msg:
                time.sleep(0.1)
                continue
            break


def test_pubsub_acl(cluster):
    """
    Test PUBSUB with ACL rules
    """
    cluster.create(3)

    cluster.execute('acl', 'setuser', 'default', 'resetchannels', '&ch1')

    c1 = cluster.node(1).client

    # Pubsub on an allowed channel
    ps1 = c1.pubsub(ignore_subscribe_messages=True)
    ps1.subscribe("ch1")
    c1.publish("ch1", "hello")

    while True:
        msg = ps1.get_message()
        if not msg:
            time.sleep(0.1)
            continue
        assert msg["channel"] == b"ch1"
        assert msg["data"] == b"hello"
        break

    # Pubsub on not allowed channel
    ps2 = c1.pubsub(ignore_subscribe_messages=True)
    ps2.subscribe("ch2")

    with raises(redis.exceptions.NoPermissionError):
        while True:
            msg = ps2.get_message()
            if not msg:
                time.sleep(0.1)
                continue
            break

    with raises(redis.exceptions.NoPermissionError):
        c1.publish("ch2", "hello")
