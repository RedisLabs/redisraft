import random
import logging
from redis import ResponseError
from fixtures import cluster


def test_fuzzing_with_restarts(cluster):
    """
    Basic Raft fuzzer test
    """

    nodes = 3
    cycles = 100

    cluster.create(nodes)
    for i in range(cycles):
        assert cluster.raft_exec('INCRBY', 'counter', 1) == i + 1
        logging.info('---------- Executed INCRBY # %s', i)
        if i % 7 == 0:
            r = random.randint(1, nodes)
            logging.info('********** Restarting node %s **********', r)
            cluster.node(r).restart()
            cluster.node(r).wait_for_election()
            logging.info('********** Node %s is UP **********', r)

    assert int(cluster.raft_exec('GET', 'counter')) == cycles


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
        assert cluster.raft_exec('INCRBY', 'counter', 1) == i + 1
        logging.info('---------- Executed INCRBY # %s', i)
        if random.randint(1, 7) == 1:
            r = random.randint(1, nodes)
            logging.info('********** Restarting node %s **********', r)
            cluster.node(r).restart()
            cluster.node(r).wait_for_election()
            logging.info('********** Node %s is UP **********', r)

    assert int(cluster.raft_exec('GET', 'counter')) == cycles


def test_fuzzing_with_config_changes(cluster):
    """
    Basic Raft fuzzer test
    """

    nodes = 5
    cycles = 100

    cluster.create(nodes)
    for i in range(cycles):
        assert cluster.raft_exec('INCRBY', 'counter', 1) == i + 1
        if random.randint(1, 7) == 1:
            try:
                node_id = cluster.random_node_id()
                cluster.remove_node(node_id)
            except ResponseError:
                continue
            cluster.add_node().wait_for_node_voting()

    assert int(cluster.raft_exec('GET', 'counter')) == cycles
