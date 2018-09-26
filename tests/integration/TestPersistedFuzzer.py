import sys
import random
import logging
import sandbox
import redis
from nose.tools import eq_, ok_
from test_tools import with_setup_args
from nose.plugins.attrib import attr

def _setup():
    return [sandbox.Cluster()], {}

def _teardown(c):
    c.destroy()

@attr('pfuzz')
@with_setup_args(_setup, _teardown)
def test_counter_fuzzer_with_rewrites(c):
    """
    Counter fuzzer with log rewrites.
    """

    nodes = 3
    cycles = 100

    c.create(nodes, persist_log=True, raft_args={'max_log_entries': '11'})
    for i in range(cycles):
        eq_(c.raft_exec('INCRBY', 'counter', 1), i + 1)
        logging.info('---------- Executed INCRBY # %s', i)
        if i % 7 == 0:
            c.wait_for_replication()
            r = random.randint(1, nodes)
            logging.info('********** Restarting node %s **********', r)
            c.node(r).restart()
            c.node(r).wait_for_election()
            logging.info('********** Node %s is UP **********', r)

    eq_(int(c.raft_exec('GET', 'counter')), cycles)

@attr('pfuzz')
@with_setup_args(_setup, _teardown)
def test_basic_persisted_fuzzer(c):
    """
    Basic Raft fuzzer test
    """

    nodes = 3
    cycles = 100

    c.create(nodes, raft_args={'persist': 'yes'})
    for i in range(cycles):
        eq_(c.raft_exec('INCRBY', 'counter', 1), i + 1)
        if i % 7 == 0:
            c.node(random.randint(1, nodes)).restart()

    eq_(int(c.raft_exec('GET', 'counter')), cycles)

