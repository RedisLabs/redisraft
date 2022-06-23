from _pytest.python_api import raises
from redis import ResponseError

def test_raft_import(cluster):
    cluster.create(3, raft_args={'sharding': 'yes', 'external-sharding': 'yes'})
    assert cluster.execute('set', 'key', 'value')
    assert cluster.execute('get', 'key') == b'value'

    serialized = cluster.execute('dump', 'key')
    assert cluster.execute('del', 'key') == 1

    assert cluster.execute('get', 'key') is None

    assert cluster.execute(
        'RAFT.SHARDGROUP', 'REPLACE',
        '2',
        '12345678901234567890123456789013',
        '1', '1',
        '0', '16383', '3', '123',
        '1234567890123456789012345678901334567890', '3.3.3.3:3333',
        cluster.leader_node().info()["raft_dbid"],
        '1', '1',
        '0', '16383', '2', '123',
        '1234567890123456789012345678901234567890', '2.2.2.2:2222',
    ) == b'OK'

    # initial import
    assert cluster.execute('raft.import', '2', '123', 'key', serialized) == b'OK'
    # older term, fails
    with raises(ResponseError, match='invalid term'):
        assert cluster.execute('raft.import', '1', '123', 'key', serialized)
    # not matched session migration key
    with raises(ResponseError, match='invalid migration_session_key'):
        assert cluster.execute('raft.import', '2', '10', 'key', serialized)
    # repeated with correct values
    assert cluster.execute('raft.import', '2', '123', 'key', serialized) == b'OK'
    # repeated with updated term
    assert cluster.execute('raft.import', '3', '123', 'key', serialized) == b'OK'
    # again, older, previously valid term
    with raises(ResponseError, match='invalid term'):
        assert cluster.execute('raft.import', '2', '123', 'key', serialized)

    conn = cluster.leader_node().client.connection_pool.get_connection('deferred')
    conn.send_command('ASKING')
    assert conn.read_response() == b'OK'

    conn.send_command('get', 'key')
    assert conn.read_response() == b'value'


def test_migrate_to_follower(cluster_factory):
    cluster1 = cluster_factory().create(3, raft_args={
        'sharding': 'yes',
        'external-sharding': 'yes'})
    cluster2 = cluster_factory().create(3, raft_args={
        'sharding': 'yes',
        'external-sharding': 'yes'})

    cluster1_dbid = cluster1.leader_node().info()["raft_dbid"]
    cluster2_dbid = cluster2.leader_node().info()["raft_dbid"]

    assert cluster1.execute('set', 'key', 'value')
    assert cluster1.execute('get', 'key') == b'value'
    assert cluster1.execute('set', '{key}key1', 'value1')
    assert cluster1.execute('get', '{key}key1') == b'value1'

    assert cluster1.execute(
        'RAFT.SHARDGROUP', 'REPLACE',
        '2',
        cluster2_dbid,
        '1', '3',
        '0', '16383', '2', '123',
        '%s00000001' % cluster2_dbid, 'localhost:%s' % cluster2.node(1).port,
        '%s00000002' % cluster2_dbid, 'localhost:%s' % cluster2.node(2).port,
        '%s00000002' % cluster2_dbid, 'localhost:%s' % cluster2.node(3).port,
        cluster1_dbid,
        '1', '3',
        '0', '16383', '3', '123',
        '%s00000001' % cluster1_dbid, 'localhost:%s' % cluster1.node(1).port,
        '%s00000002' % cluster1_dbid, 'localhost:%s' % cluster1.node(2).port,
        '%s00000003' % cluster1_dbid, 'localhost:%s' % cluster1.node(3).port,
        ) == b'OK'

    c = cluster1.node(2).client;
    with raises(ResponseError, match="MOVED 0 localhost:5001"):
        assert c.execute_command("migrate", "", "", "", "", "", "keys", "key", "{key}key1") == b'OK'

def test_happy_migrate(cluster_factory):
    cluster1 = cluster_factory().create(1, raft_args={
        'sharding': 'yes',
        'external-sharding': 'yes'})
    cluster2 = cluster_factory().create(1, raft_args={
        'sharding': 'yes',
        'external-sharding': 'yes'})

    cluster1_dbid = cluster1.leader_node().info()["raft_dbid"]
    cluster2_dbid = cluster2.leader_node().info()["raft_dbid"]

    assert cluster1.execute('set', 'key', 'value')
    assert cluster1.execute('get', 'key') == b'value'
    assert cluster1.execute('set', '{key}key1', 'value1')
    assert cluster1.execute('get', '{key}key1') == b'value1'

    assert cluster1.execute(
        'RAFT.SHARDGROUP', 'REPLACE',
        '2',
        cluster2_dbid,
        '1', '1',
        '0', '16383', '2', '123',
        '%s00000001' % cluster2_dbid, 'localhost:%s' % cluster2.node(1).port,
        cluster1_dbid,
        '1', '1',
        '0', '16383', '3', '123',
        '%s00000001' % cluster1_dbid, 'localhost:%s' % cluster1.node(1).port,
        ) == b'OK'

    assert cluster2.execute(
        'RAFT.SHARDGROUP', 'REPLACE',
        '2',
        cluster2_dbid,
        '1', '1',
        '0', '16383', '2', '123',
        '%s00000001' % cluster2_dbid, 'localhost:%s' % cluster2.node(1).port,
        cluster1_dbid,
        '1', '1',
        '0', '16383', '3', '123',
        '%s00000001' % cluster1_dbid, 'localhost:%s' % cluster1.node(1).port,
        ) == b'OK'

    assert cluster1.execute("migrate", "", "", "", "", "", "keys", "key", "{key}key1") == b'OK'

    with raises(ResponseError, match="ASK 12539 localhost"):
        cluster1.execute("get", "key")

    with raises(ResponseError, match="ASK 12539 localhost"):
        cluster1.execute("get", "{key}key1")

    with raises(ResponseError, match="MOVED 12539 localhost"):
        # can't use cluster.execute() as that will try to handle the MOVED response itself
        cluster2.leader_node().client.get("key")

    conn = cluster2.leader_node().client.connection_pool.get_connection('deferred')
    conn.send_command('ASKING')
    assert conn.read_response() == b'OK'

    conn.send_command('get', 'key')
    assert conn.read_response() == b'value'

    conn.send_command('get', '{key}key1')
    with raises(ResponseError, match="MOVED 12539 localhost:5001"):
        conn.read_response()

    conn.send_command('ASKING')
    assert conn.read_response() == b'OK'
    conn.send_command('get', '{key}key1')
    assert conn.read_response() == b'value1'

    conn.send_command('get', 'key1')
    with raises(ResponseError, match="MOVED 9189 localhost:5001"):
        conn.read_response()

    conn.send_command('ASKING')
    assert conn.read_response() == b'OK'
    conn.send_command('get', 'key1')
    with raises(ResponseError, match="TRYAGAIN"):
        conn.read_response()

    assert cluster2.execute(
        'RAFT.SHARDGROUP', 'REPLACE',
        '1',
        cluster2_dbid,
        '1', '1',
        '0', '16383', '1', "123",
        '%s00000001' % cluster2_dbid, 'localhost:%s' % cluster2.node(1).port,
    ) == b'OK'

    assert cluster2.execute("get", "key") == b'value'
    assert cluster2.execute("get", "{key}key1") == b'value1'
    assert cluster2.execute("get", "key1") is None

