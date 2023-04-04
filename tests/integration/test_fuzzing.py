"""
Copyright Redis Ltd. 2020 - present
Licensed under your choice of the Redis Source Available License 2.0 (RSALv2)
or the Server Side Public License v1 (SSPLv1).
"""

import random
import logging
import time
from enum import Enum

import pytest
from redis import ResponseError
from .workload import MultiWithLargeReply, MonotonicIncrCheck


def test_fuzzing_with_restarts(cluster):
    """
    Basic Raft fuzzer test
    """

    nodes = 3
    cycles = 100

    cluster.create(nodes)
    for i in range(cycles):
        assert cluster.execute('INCRBY', 'counter', 1) == i + 1
        logging.info('---------- Executed INCRBY # %s', i)
        if i % 7 == 0:
            r = random.randint(1, nodes)
            logging.info('********** Restarting node %s **********', r)
            cluster.node(r).restart()
            cluster.node(r).wait_for_election()
            logging.info('********** Node %s is UP **********', r)

    assert int(cluster.execute('GET', 'counter')) == cycles


def test_fuzzing_with_restarts_and_rewrites(cluster):
    """
    Counter fuzzer with log rewrites.
    """

    nodes = 3
    cycles = 100

    cluster.create(nodes)
    # Randomize max log entries
    for node in cluster.nodes.values():
        node.client.execute_command(
            'CONFIG', 'SET', 'raft.log-max-file-size',
            str(random.randint(1000, 2000)))

    for i in range(cycles):
        assert cluster.execute('INCRBY', 'counter', 1) == i + 1
        logging.info('---------- Executed INCRBY # %s', i)
        if random.randint(1, 7) == 1:
            r = random.randint(1, nodes)
            logging.info('********** Restarting node %s **********', r)
            cluster.node(r).restart()
            cluster.node(r).wait_for_election()
            logging.info('********** Node %s is UP **********', r)

    assert int(cluster.execute('GET', 'counter')) == cycles


def test_fuzzing_with_config_changes(cluster):
    """
    Basic Raft fuzzer test
    """

    nodes = 5
    cycles = 100

    cluster.create(nodes)
    for i in range(cycles):
        assert cluster.execute('INCRBY', 'counter', 1) == i + 1
        if random.randint(1, 7) == 1:
            try:
                node_id = cluster.random_node_id()
                cluster.remove_node(node_id)
            except ResponseError:
                continue
            cluster.add_node().wait_for_node_voting()

    assert int(cluster.execute('GET', 'counter')) == cycles


def test_fuzzing_with_proxy_multi_and_restarts(cluster, workload):
    """
    Test proxy with transaction safety and random node restarts.
    """

    nodes = 3
    cycles = 20
    thread_count = 200

    cluster.create(nodes, raft_args={'follower-proxy': 'yes'})
    workload.start(thread_count, cluster, MultiWithLargeReply)
    for i in range(cycles):
        time.sleep(1)
        try:
            logging.info('Cycle %s: %s', i, workload.stats())
            cluster.random_node().restart()
        except ResponseError as err:
            logging.error('Remove node: %s', err)
            continue
    logging.info('All cycles finished')
    workload.stop()


def test_proxy_with_multi_and_reconnections(cluster, workload):
    """
    Test proxy mode with MULTI transactions safety checks and
    reconnections (dropping clients with CLIENT KILL).
    """

    thread_count = 100
    cycles = 20

    cluster.create(3, raft_args={'follower-proxy': 'yes'})
    workload.start(thread_count, cluster, MultiWithLargeReply)
    for _ in range(cycles):
        time.sleep(1)
        logging.info('Initiating client kill cycle')
        cluster.leader_node().client.execute_command(
            'CLIENT', 'KILL', 'TYPE', 'normal')

    logging.info('All cycles finished')
    workload.stop()


def test_stale_reads_on_restarts(cluster, workload):
    """
    Test proxy mode with MULTI transactions safety checks and
    reconnections (dropping clients with CLIENT KILL).
    """

    thread_count = 50
    cycles = 20
    cluster.create(3, raft_args={'follower-proxy': 'yes'})
    workload.start(thread_count, cluster, MonotonicIncrCheck)
    for _ in range(cycles):
        time.sleep(1)
        cluster.restart()
    logging.info('All cycles finished')
    workload.stop()


def test_snapshot_delivery_with_config_changes(cluster):
    """
    Test big snapshot delivery (~70 mb on disk) while adding/removing nodes
    """
    cycles = 10

    cluster.create(1, raft_args={'response-timeout': 5000})
    cluster.execute('set', 'x', '1')
    cluster.execute('set', 'x', '1')

    n1 = cluster.node(1)
    n1.execute('raft.debug', 'exec', 'debug', 'populate', 2000000, 'a', 200)

    # After populating 2 million keys, snapshot can take a while. We just
    # trigger it asynchronously and wait until completed for 30 seconds.
    n1.execute('raft.debug', 'compact', 1)
    n1.wait_for_info_param('raft_snapshots_created', 1, timeout=30)

    cluster.add_node(use_cluster_args=True)
    cluster.add_node(use_cluster_args=True)
    cluster.add_node(use_cluster_args=True)
    cluster.add_node(use_cluster_args=True)

    cluster.wait_for_unanimity()

    for i in range(cycles):
        assert cluster.execute('INCRBY', 'counter', 1) == i + 1
        try:
            cluster.remove_node(cluster.random_node_id())
        except ResponseError:
            continue

        cluster.add_node(use_cluster_args=True).wait_for_node_voting()

    logging.info('All cycles finished')
    assert int(cluster.execute('GET', 'counter')) == cycles


@pytest.mark.slow
@pytest.mark.timeout(24000)
def test_proxy_stability_under_load(cluster, workload):
    """
    Test stability of the cluster with follower proxy under load.
    """

    thread_count = 500
    duration = 300

    cluster.create(5, raft_args={'follower-proxy': 'yes'})
    workload.start(thread_count, cluster, MultiWithLargeReply)

    # Monitor progress
    start = time.time()
    last_commit_index = 0
    while start + duration > time.time():
        time.sleep(2)
        new_commit_index = cluster.node(cluster.leader).commit_index()
        assert new_commit_index >= last_commit_index
        last_commit_index = new_commit_index

    workload.stop()


@pytest.mark.slow
@pytest.mark.timeout(24000)
def test_stability_with_snapshots_and_restarts(cluster, workload):
    """
    Test stability of the cluster with frequent snapshotting.
    """

    thread_count = 100
    duration = 300

    cluster.create(5, raft_args={'follower-proxy': 'yes',
                                 'log-max-file-size': '2000'})

    workload.start(thread_count, cluster, MultiWithLargeReply)

    # Monitor progress
    start = time.time()

    while start + duration > time.time():
        time.sleep(2)
        cluster.random_node().restart()

    workload.stop()


@pytest.mark.slow
@pytest.mark.timeout(24000)
def test_fuzzing_with_large_snapshots(cluster, workload):
    """
    Fill nodes with 1,7 Gb of data (~800 mb RDB file) and do some fuzzing.
    """

    class Operation(Enum):
        ADD_NODE = 0
        ADD_MULTIPLE_NODES = 1
        RESTART_FOLLOWER = 2
        RESTART_FOLLOWER_WITH_DELAY = 3
        RESTART_LEADER = 4
        RESTART_LEADER_WITH_DELAY = 5
        RESTART_CLUSTER = 6
        LEADER_TRANSFER = 7

    cluster.create(1)

    n1 = cluster.node(1)

    # Just to avoid socket timeouts, populate db in a few steps.
    for i in range(0, 4):
        prefix = '_dummydummykeyname' + str(i)
        n1.raft_debug_exec('debug', 'populate', 4000000, prefix, 20)

    # Create a snapshot
    n1.execute('set', 'x', 1)
    n1.execute('raft.debug', 'compact', 1)
    n1.wait_for_info_param('raft_snapshots_created', 1, timeout=240)

    # Add followers
    cluster.add_node()
    cluster.add_node()

    workload.start(10, cluster, MonotonicIncrCheck)

    for i in range(40):
        # Starting condition: 3 voting nodes, all entries are applied.
        cluster.wait_for_info_param("raft_num_voting_nodes", 3, timeout=240)
        cluster.wait_for_unanimity(timeout=240)

        op = random.choice(list(Operation))

        logging.info("Iteration: %s , Op: %s" % (i, op))

        if op == Operation.ADD_NODE:
            cluster.remove_node(cluster.random_node_id())
            cluster.add_node()

        elif op == Operation.ADD_MULTIPLE_NODES:
            # Remove two nodes and add two new nodes. Snapshot will be
            # delivered to both of them at the same time.
            cluster.remove_node(cluster.random_node_id())
            cluster.wait_for_info_param("raft_num_voting_nodes", 2)

            cluster.remove_node(cluster.random_node_id())
            cluster.wait_for_info_param("raft_num_voting_nodes", 1)

            cluster.add_node()
            cluster.add_node()

        elif op == Operation.RESTART_FOLLOWER:
            n = cluster.follower_node()
            n.restart()

        elif op == Operation.RESTART_FOLLOWER_WITH_DELAY:
            # Sleep a bit before starting it back. Node will lag behind and
            # entries and/or snapshot will be delivered.
            n = cluster.follower_node()
            n.terminate()
            time.sleep(10)
            n.start()

        elif op == Operation.RESTART_LEADER:
            n = cluster.leader_node()
            n.restart()

        elif op == Operation.RESTART_LEADER_WITH_DELAY:
            # Sleep a bit before starting it back. Node will lag behind and
            # entries and/or snapshot will be delivered.
            n = cluster.leader_node()
            n.terminate()
            time.sleep(10)
            n.start()

        elif op == Operation.RESTART_CLUSTER:
            cluster.restart()
            cluster.random_node().wait_for_election()

        elif op == Operation.LEADER_TRANSFER:
            # Leader transfer can fail, e.g. destination node can't catch up
            # the leader. Then, we just skip the operation.
            n = cluster.leader_node()
            try:
                n.transfer_leader()
            except ResponseError:
                continue

            cluster.update_leader()
            assert cluster.leader_node() != n

    workload.stop()

    logging.info('All cycles finished')
    logging.info('Stats: ' + workload.stats())


@pytest.mark.slow
@pytest.mark.timeout(24000)
def test_stability_with_snapshot_delivery(cluster, workload):
    """
    Fill nodes with 1,7 Gb of data (~800 mb RDB file) and constantly deliver
    snapshot to a node under traffic.
    """
    cluster.create(1)

    n1 = cluster.node(1)

    # Just to avoid socket timeouts, populate db in a few steps.
    for i in range(0, 4):
        prefix = '_dummydummykeyname' + str(i)
        n1.raft_debug_exec('debug', 'populate', 4000000, prefix, 20)

    # Create a snapshot
    n1.execute('set', 'x', 1)
    n1.execute('raft.debug', 'compact', 1)
    n1.wait_for_info_param('raft_snapshots_created', 1, timeout=240)

    # Add followers
    cluster.add_node()
    cluster.add_node()

    cluster.wait_for_info_param("raft_num_voting_nodes", 3, timeout=240)
    cluster.wait_for_unanimity(timeout=240)
    cluster.config_set("raft.log-max-file-size", "16mb")

    workload.start(10, cluster, MonotonicIncrCheck)

    for i in range(10):
        leader = cluster.leader_node()
        snapshots = leader.info()["raft_snapshots_created"]

        follower = cluster.follower_node()
        follower.terminate()

        leader.wait_for_info_param("raft_snapshots_created", snapshots,
                                   timeout=240, greater=True)
        follower.start()
        cluster.wait_for_unanimity(timeout=240)

    workload.stop()

    logging.info('All cycles finished')
    logging.info('Stats: ' + workload.stats())
