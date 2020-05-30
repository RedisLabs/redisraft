"""
This file is part of RedisRaft.

Copyright (c) 2020 Redis Labs

RedisRaft is dual licensed under the GNU General Public License version 3
(AGPLv3) or the Redis Source Available License (RSAL).
"""

import time
import logging
import threading
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


def test_full_cluster_remove(cluster):
    """
    Remove all cluster nodes.
    """

    cluster.create(5)

    leader = cluster.leader_node()
    expected_nodes = 5
    for node_id in (2, 3, 4, 5):
        leader.client.execute_command('RAFT.NODE', 'REMOVE', str(node_id))
        expected_nodes -= 1
        leader.wait_for_num_nodes(expected_nodes)

    # make sure other nodes are down
    for node_id in (2, 3, 4, 5):
        assert not cluster.node(node_id).process_is_up()

    # and make sure they start up in uninitialized state
    for node_id in (2, 3, 4, 5):
        cluster.node(node_id).terminate()
        cluster.node(node_id).start()

    for node_id in (2, 3, 4, 5):
        assert cluster.node(node_id).raft_info()['state'] == 'uninitialized'

def test_node_history_with_same_address(cluster):
    ""
    ""
    cluster.create(5)
    cluster.raft_exec("INCR", "step-counter")

    # Remove nodes
    ports = []
    for node_id in [2, 3, 4, 5]:
        ports.append(cluster.node(node_id).port)
        cluster.remove_node(node_id)
        cluster.leader_node().wait_for_log_applied()
    cluster.node(cluster.leader).wait_for_num_nodes(1)

    # Now add and remove several more times
    for _ in range(5):
        for port in ports:
            n = cluster.add_node(port=port)
            cluster.leader_node().wait_for_num_nodes(2)
            cluster.leader_node().wait_for_log_applied()
            cluster.remove_node(n.id)
            cluster.leader_node().wait_for_num_nodes(1)
            cluster.leader_node().wait_for_log_applied()

    # Add enough data in the log to satisfy timing
    for _ in range(3000):
        cluster.raft_exec("INCR", "step-counter")

    # Add another node
    new_node = cluster.add_node(port=ports[0])
    new_node.wait_for_node_voting()

    # Terminate all
    cluster.terminate()

    # Start new node
    cluster.start()

    # need some time to start applying logs..
    time.sleep(2)

    assert cluster.raft_exec("GET", "step-counter") == b'3001'


def test_update_self_voting_state_from_snapshot(cluster):
    cluster.create(3)

    assert cluster.node(1).client.execute_command('RAFT.DEBUG', 'NODECFG', '2', '-voting') == b'OK'
    assert cluster.node(2).raft_info()['is_voting'] == 'yes'
    assert cluster.node(1).client.execute_command('RAFT.DEBUG', 'COMPACT') == b'OK'

    cluster.node(1).client.execute_command('RAFT.DEBUG', 'SENDSNAPSHOT', '2')
    cluster.node(2).wait_for_info_param('snapshots_loaded', 1)
    assert cluster.node(2).raft_info()['is_voting'] == 'no'
