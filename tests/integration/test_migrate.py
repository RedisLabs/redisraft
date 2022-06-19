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
