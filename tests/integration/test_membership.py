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
