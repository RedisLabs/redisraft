import random
import logging
import threading
import time
from redis import ResponseError, RedisError
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


def test_fuzzing_with_proxy_multi_and_restarts(cluster):
    """
    Test proxy with transaction safety and random node restarts.
    """

    nodes = 3
    cycles = 20
    thread_count = 200

    cluster.create(nodes, raft_args={'raftize-all-commands': 'yes',
                                     'follower-proxy': 'yes'})
    results = {'good': 0, 'bad': 0}
    finished = False

    def thread(node, tid, results):
        cname = 'counter-%s' % tid
        lname = 'list-%s' % tid

        run = 1
        while not finished:
            client = cluster.random_node().client
            try:
                pipeline = client.pipeline(transaction=True)
                pipeline.incr(cname)
                pipeline.rpush(lname, run)
                pipeline.lrange(lname, 0, -1)
                pipeline.lrange(lname, 0, -1)
                pipeline.lrange(lname, 0, -1)
                pipeline.lrange(lname, 0, -1)
                pipeline.lrange(lname, 0, -1)
                pipeline.lrange(lname, 0, -1)
                pipeline.lrange(lname, 0, -1)
                pipeline.lrange(lname, 0, -1)
                pipeline.lrange(lname, 0, -1)
                pipeline.lrange(lname, 0, -1)
                pipeline.lrange(lname, 0, -1)
                reply = pipeline.execute()
                run += 1
                assert reply[0] == reply[1] # count == length of list
                results['good'] += 1
            except RedisError as err:
                continue
            except AssertionError:
                logging.error('node: %s tid %s: run %s: Bad reply: %s',
                    node, tid, run, reply)
                results['bad'] += 1
                break

        logging.info('Thread %s exiting after %s runs on node %s', tid, run,
                     node)

    threads = []
    for tid in range(thread_count):
        _thread = threading.Thread(target=thread, args=[
            cluster.random_node_id(), tid + 1, results])
        _thread.start()
        threads.append(_thread)

    for i in range(cycles):
        time.sleep(1)
        try:
            logging.info('Cycle %s: %s', i, results)
            cluster.random_node().restart()
        except ResponseError as err:
            logging.error('Remove node: %s', err)
            continue

    logging.info('All cycles finished')

    finished = True

    while threads:
        threads.pop().join()
    assert results['good'] > 0
    assert results['bad'] == 0


def test_proxy_with_multi_and_reconnections(cluster):
    """
    Test proxy mode with MULTI transactions safety checks and
    reconnections (dropping clients with CLIENT KILL).
    """

    cluster.create(3, raft_args={'follower-proxy': 'yes',
                                 'raftize-all-commands': 'yes'})
    finished = False
    thread_count = 100
    cycles = 20
    results = {'good': 0, 'bad': 0}

    def writer(node, tid, results):
        client = cluster.node(node).client
        cname = 'counter-%s' % tid
        lname = 'list-%s' % tid
        run = 1
        while not finished:
            try:
                pipeline = client.pipeline(transaction=True)
                pipeline.incr(cname)
                pipeline.rpush(lname, run)
                pipeline.lrange(lname, 0, -1)
                pipeline.lrange(lname, 0, -1)
                pipeline.lrange(lname, 0, -1)
                pipeline.lrange(lname, 0, -1)
                pipeline.lrange(lname, 0, -1)
                pipeline.lrange(lname, 0, -1)
                pipeline.lrange(lname, 0, -1)
                pipeline.lrange(lname, 0, -1)
                pipeline.lrange(lname, 0, -1)
                pipeline.lrange(lname, 0, -1)
                reply = pipeline.execute()
                run += 1
                assert reply[0] == reply[1] # count == length of list
                results['good'] += 1
            except RedisError as err:
                continue
            except AssertionError:
                logging.error('node: %s tid %s: run %s: Bad reply: %s',
                    node, tid, run, reply)
                results['bad'] += 1
        logging.info('Thread %s exiting after %s runs on node %s', tid, run,
                     node)

    threads = []
    for tid in range(thread_count):
        _thread = threading.Thread(target=writer, args=[
            cluster.random_node_id(), tid + 1, results])
        _thread.start()
        threads.append(_thread)

    for _ in range(cycles):
        time.sleep(1)
        logging.info('Initiating client kill cycle')
        cluster.leader_node().client.execute_command(
            'CLIENT', 'KILL', 'TYPE', 'normal')

    logging.info('All cycles finished')

    finished = True

    while threads:
        threads.pop().join()
    assert results['good'] > 0
    assert results['bad'] == 0
