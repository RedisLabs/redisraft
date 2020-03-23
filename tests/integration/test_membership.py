import time
from pytest import raises
from redis import ResponseError, RedisError
from fixtures import cluster

def test_node_join_iterates_all_addrs(cluster):
    """
    Node join iterates all addresses.
    """
    r1 = cluster.add_node()
    assert r1.raft_exec('SET', 'key', 'value') == b'OK'
    r2 = cluster.add_node(cluster_setup=False)
    r2.start()
    assert r2.cluster('join', 'localhost:1', 'localhost:2',
                      'localhost:{}'.format(cluster.node_ports()[0])) == b'OK'
    r2.wait_for_election()


def test_node_join_redirects_to_leader(cluster):
    """
    Node join can redirect to leader.
    """
    r1 = cluster.add_node()
    assert r1.raft_exec('SET', 'key', 'value') == b'OK'
    r2 = cluster.add_node()
    r2.wait_for_election()
    r3 = cluster.add_node(cluster_setup=False)
    r3.start()
    r3.cluster('join', 'localhost:{}'.format(r2.port))
    r3.wait_for_election()


def test_leader_removal_not_allowed(cluster):
    """
    Leader node cannot be removed.
    """

    cluster.create(3)
    assert cluster.leader == 1
    with raises(ResponseError, match='cannot remove leader'):
        cluster.node(1).client.execute_command('RAFT.NODE', 'REMOVE', '1')


def test_single_voting_change_enforced(cluster):
    """
    A single concurrent voting change is enforced when removing nodes.
    """

    cluster.create(5)
    assert cluster.leader == 1

    # Simulate a partition
    cluster.node(2).terminate()
    cluster.node(3).terminate()
    cluster.node(4).terminate()

    assert cluster.node(1).client.execute_command(
        'RAFT.NODE', 'REMOVE', '5') == b'OK'
    with raises(ResponseError,
            match='a voting change is already in progress'):
        assert cluster.node(1).client.execute_command(
            'RAFT.NODE', 'REMOVE', '4') == b'OK'

    time.sleep(1)
    assert cluster.node(1).raft_info()['num_nodes'] == 5


def test_removed_node_remains_dead(cluster):
    """
    A removed node stays down and does not resurrect in any case.
    """

    cluster.create(3)

    # Some baseline data
    for _ in range(100):
        cluster.raft_exec('INCR', 'counter')

    # Remove node 3
    cluster.node(1).client.execute_command('RAFT.NODE', 'REMOVE', '3')
    cluster.node(1).wait_for_num_voting_nodes(2)

    # Add more data
    for _ in range(100):
        cluster.raft_exec('INCR', 'counter')

    # Check
    node = cluster.node(3)

    # Verify node 3 does not accept writes
    with raises(RedisError):
        node.client.execute_command('RAFT', 'INCR', 'counter')

    # Verify node 3 still does not accept writes after a restart
    node.terminate()
    node.start()

    with raises(RedisError):
        node.client.execute_command('RAFT', 'INCR', 'counter')


def test_full_cluster_remove_and_rejoin(cluster):
    """
    Remove all cluster nodes, then attempt rejoin.
    """

    cluster.create(5)
    for _ in range(100):
        cluster.raft_exec('INCR', 'counter')

    leader = cluster.node(1)
    expected_nodes = 5
    for node_id in (2, 3, 4, 5):
        leader.client.execute_command('RAFT.NODE', 'REMOVE', str(node_id))
        expected_nodes -= 1
        leader.wait_for_num_nodes(expected_nodes)
        leader.wait_for_commit_index(leader.current_index())

    # more changes
    for _ in range(10):
        cluster.raft_exec('INCR', 'counter')

    # make sure other nodes are down
    time.sleep(3)
    for node_id in (2, 3, 4, 5):
        assert not cluster.node(node_id).process_is_up()

    # and make sure they start up in uninitialized state
    for node_id in (2, 3, 4, 5):
        cluster.node(node_id).terminate()
        cluster.node(node_id).start()

    time.sleep(3)
    for node_id in (2, 3, 4, 5):
        assert cluster.node(node_id).raft_info()['state'] == 'uninitialized'
