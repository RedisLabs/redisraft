import time
from pytest import raises
from redis import ResponseError
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
    with raises(ResponseError):
        node.client.execute_command('RAFT', 'INCR', 'counter')

    # Verify node 3 still does not accept writes after a restart
    node.terminate()
    node.start()

    with raises(ResponseError):
        node.client.execute_command('RAFT', 'INCR', 'counter')
