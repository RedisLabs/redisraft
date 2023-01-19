"""
Copyright Redis Ltd. 2020 - present
Licensed under your choice of the Redis Source Available License 2.0 (RSALv2)
or the Server Side Public License v1 (SSPLv1).
"""

import logging
import time
from threading import Thread
import pytest
from pytest import raises
from redis import ResponseError, RedisError

logger = logging.getLogger("integration")


def test_node_join_iterates_all_addrs(cluster):
    """
    Node join iterates all addresses.
    """
    r1 = cluster.add_node()
    assert r1.client.set('key', 'value')
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
    assert r1.client.set('key', 'value')
    r2 = cluster.add_node()
    r2.wait_for_election()
    r3 = cluster.add_node(cluster_setup=False)
    r3.start()
    r3.cluster('join', 'localhost:{}'.format(r2.port))
    r3.wait_for_election()


def test_leader_removal_allowed(cluster):
    """
    Leader node can be removed.
    """

    cluster.create(3)
    assert cluster.leader == 1
    cluster.remove_node(1)
    cluster.node(2).wait_for_num_nodes(2)


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

    # Initiate process again with a longer timeout, and release the
    # cluster while in progress.
    def remove_node_blocked():
        cluster.node(1).client.execute_command('RAFT.NODE', 'REMOVE', '5')

    t = Thread(target=remove_node_blocked)
    t.start()

    time.sleep(0.2)

    with raises(ResponseError, match='one voting change only'):
        cluster.node(1).client.execute_command('RAFT.NODE', 'REMOVE', '4')

    assert cluster.node(1).info()['raft_num_nodes'] == 5

    # Starting nodes again, so, blocked thread can join gracefully
    cluster.node(2).start()
    cluster.node(3).start()
    cluster.node(4).start()
    t.join()


def test_removed_node_remains_dead(cluster):
    """
    A removed node stays down and does not resurrect in any case.
    """

    cluster.create(3)

    # Some baseline data
    for _ in range(100):
        cluster.execute('INCR', 'counter')

    # Remove node 3
    cluster.node(1).client.execute_command('RAFT.NODE', 'REMOVE', '3')
    cluster.node(1).wait_for_num_voting_nodes(2)

    # Add more data
    for _ in range(100):
        cluster.execute('INCR', 'counter')

    # Check
    node = cluster.node(3)

    # Verify node 3 does not accept writes
    with raises(RedisError):
        node.client.incr('counter')

    # Verify node 3 still does not accept writes after a restart
    node.terminate()
    node.start()

    with raises(RedisError):
        node.client.incr('counter')


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
        leader.wait_for_log_applied()

    # remove the leader node finally
    try:
        leader.client.execute_command('RAFT.NODE', 'REMOVE', leader.id)
    except Exception as e:
        logger.info('RAFT.NODE REMOVE throws ' + str(e))
        pass

    # wait some time for the last node
    time.sleep(3)

    # make sure other nodes are down
    for node_id in (1, 2, 3, 4, 5):
        assert cluster.node(node_id).verify_down()

    # and make sure they start up in uninitialized state
    for node_id in (1, 2, 3, 4, 5):
        cluster.node(node_id).terminate()
        cluster.node(node_id).start()

    for node_id in (1, 2, 3, 4, 5):
        state = cluster.node(node_id).info()['raft_state']
        assert state == 'uninitialized'


@pytest.mark.parametrize("use_snapshot", [False, True])
def test_remove_and_rejoin_node_with_same_id_fails(cluster, use_snapshot):
    cluster.create(3)
    node_ids = [node_id for node_id in cluster.nodes]

    if use_snapshot:
        n = cluster.node(1)
        assert n.client.execute_command('RAFT.DEBUG', 'COMPACT') == b'OK'
        assert n.info()['raft_log_entries'] == 0

        logger.info("Restarting node from snapshot")
        n.restart()

    for node in cluster.nodes.values():
        used_node_ids = node.client.execute_command('RAFT.DEBUG',
                                                    'USED_NODE_IDS')
        assert set(used_node_ids) == set(node_ids), \
            "Wrong used_node_ids for node {}".format(node.id)

    if use_snapshot:
        cluster.wait_for_unanimity()

    logger.info("Remove node")
    node_id = cluster.random_node_id()
    port = cluster.node(node_id).port

    cluster.remove_node(node_id)
    cluster.leader_node().wait_for_log_applied()
    cluster.node(cluster.leader).wait_for_num_nodes(2)

    logger.info("Re-add node")
    with raises(ResponseError, match='failed to connect to cluster for join'):
        cluster.add_node(port=port, node_id=node_id, single_run=True)


def test_node_history_with_same_address(cluster):
    cluster.create(5)
    cluster.execute("INCR", "step-counter")

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
            cluster.leader_node().wait_for_num_voting_nodes(2)
            cluster.leader_node().wait_for_log_committed()
            cluster.leader_node().wait_for_log_applied()
            cluster.remove_node(n.id)
            cluster.leader_node().wait_for_num_voting_nodes(1)
            cluster.leader_node().wait_for_log_applied()

    # Add enough data in the log to satisfy timing
    for _ in range(3000):
        cluster.execute("INCR", "step-counter")

    # Add another node
    new_node = cluster.add_node(port=ports[0])
    new_node.wait_for_node_voting()

    # Terminate all
    cluster.terminate()

    # Start new node
    cluster.start()

    # need some time to start applying logs..
    time.sleep(2)

    assert cluster.execute("GET", "step-counter") == b'3001'


def test_update_self_voting_state_from_snapshot(cluster):
    cluster.create(3)

    assert cluster.node(2).info()['raft_is_voting'] == 'yes'
    cluster.node(2).kill()

    # Set node-2 non-voting and take a snapshot
    assert cluster.node(1).client.execute_command('RAFT.DEBUG', 'NODECFG',
                                                  '2', '-voting') == b'OK'

    # Submit an entry and include that in the snapshot. When node-2 wakes up,
    # as it doesn't have an entry at this index, leader will send the snapshot
    cluster.execute('set', 'x', '1')
    assert cluster.node(1).client.execute_command('RAFT.DEBUG',
                                                  'COMPACT') == b'OK'

    # Leader will send snapshot to node-2 after wake up
    cluster.node(2).start()
    cluster.node(2).wait_for_info_param('raft_snapshots_received', 1)

    # Validate node-2 updates its voting status from the snapshot
    assert cluster.node(2).info()['raft_is_voting'] == 'no'


def test_join_while_cluster_is_down(cluster):
    cluster.create(3)

    cluster.node(1).pause()
    cluster.node(2).pause()
    time.sleep(3)

    # Confirm nodes cannot be added
    with raises(ResponseError, match='failed to connect to cluster for join'):
        cluster.add_node(raft_args={'join-timeout': 1}, single_run=True,
                         join_addr_list=[cluster.node(3).address])


def test_join_wrong_cluster(cluster):
    cluster.create(3)

    # Confirm nodes fails fast with bad server address
    with raises(ResponseError, match='failed to connect to cluster for join'):
        cluster.add_node(raft_args={'join-timeout': 1}, single_run=True,
                         join_addr_list=["bad-server:1234"])

    # Config node can be added after failure
    cluster.add_node(raft_args={'join-timeout': 1}, single_run=True,
                     join_addr_list=[cluster.node(3).address])


def test_transfer_not_leader(cluster):
    cluster.create(3)

    with raises(ResponseError, match="not leader"):
        cluster.node(2).transfer_leader(3)


def test_transfer_invalid(cluster):
    cluster.create(3)

    with raises(ResponseError, match="invalid node id"):
        cluster.leader_node().transfer_leader(4)


def test_transfer_succeed(cluster):
    cluster.create(3, raft_args={'election-timeout': '3000'})

    cluster.leader_node().transfer_leader(2)


def test_transfer_to_any_other(cluster):
    cluster.create(2, raft_args={'election-timeout': '3000'})
    cluster.leader_node().transfer_leader()


def test_transfer_timeout(cluster):
    cluster.create(3, raft_args={'election-timeout': '3000'})
    cluster.node(2).pause()
    with raises(ResponseError, match='transfer timed out'):
        cluster.leader_node().transfer_leader(2)


def test_transfer_unexpected(cluster):
    cluster.create(3, raft_args={'election-timeout': '10000'})
    cluster.node(2).pause()

    def timeout():
        time.sleep(3)
        cluster.node(3).timeout_now()

    Thread(target=timeout, daemon=True).start()
    with raises(ResponseError, match="different node elected leader"):
        cluster.leader_node().transfer_leader(2)


def test_nodeshutdown_wrong_id(cluster):
    cluster.create(1, raft_args={'election-timeout': '10000'})

    with raises(ResponseError, match='invalid node id'):
        cluster.node(1).client.execute_command('RAFT.NODESHUTDOWN', 51423122)
