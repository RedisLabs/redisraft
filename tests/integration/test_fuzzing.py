"""
This file is part of RedisRaft.

Copyright (c) 2020-2021 Redis Ltd.

RedisRaft is licensed under the Redis Source Available License (RSAL).
"""

import random
import logging
import time
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
            'RAFT.CONFIG', 'SET', 'raft-log-max-file-size',
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


@pytest.mark.slow
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
def test_stability_with_snapshots_and_restarts(cluster, workload):
    """
    Test stability of the cluster with frequent snapshoting.
    """

    thread_count = 100
    duration = 300

    cluster.create(5, raft_args={'follower-proxy': 'yes',
                                 'raft-log-max-file-size': '2000'})

    workload.start(thread_count, cluster, MultiWithLargeReply)

    # Monitor progress
    start = time.time()
    last_commit_index = 0
    while start + duration > time.time():
        time.sleep(2)
        cluster.random_node().restart()

    workload.stop()
