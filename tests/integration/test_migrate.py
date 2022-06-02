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
        '0', '16383', '3',
        '1234567890123456789012345678901334567890', '3.3.3.3:3333',
        cluster.leader_node().info()["raft_dbid"],
        '1', '1',
        '0', '16383', '2',
        '1234567890123456789012345678901234567890', '2.2.2.2:2222',
    ) == b'OK'

    assert cluster.execute('raft.import', '2', '0', 'key', serialized) == b'OK'

    assert cluster.execute("asking", "get", "key") == b'value'
