import sys
import random
import sandbox
import redis
from nose.tools import eq_, ok_
from test_tools import with_setup_args
from nose.plugins.attrib import attr

def _setup():
    return [sandbox.Cluster()], {}

def _teardown(c):
    c.destroy()

@attr('fuzz')
@with_setup_args(_setup, _teardown)
def test_basic_persisted_fuzzer(c):
    """
    Basic Raft fuzzer test
    """

    nodes = 3
    cycles = 100

    c.create(nodes, raft_args={'persist': 'yes', 'max_log_entries': '11'})
    for i in range(cycles):
        ok_(c.raft_exec('INCRBY', 'counter', 1))
        if i % 7 == 0:
            r = random.randint(1, nodes)
            c.node(r).restart()
            c.node(r).wait_for_info_param('state', 'up')

    eq_(int(c.raft_exec('GET', 'counter')), cycles)

@attr('fuzz')
@with_setup_args(_setup, _teardown)
def test_fuzzing_with_config_changes(c):
    """
    Basic Raft fuzzer test
    """

    nodes = 5
    cycles = 100

    c.create(nodes, raft_args={'persist': 'yes'})
    for i in range(cycles):
        ok_(c.raft_exec('INCRBY', 'counter', 1))
        if i % 7 == 0:
            try:
                node_id = c.random_node_id()
                c.remove_node(node_id)
            except redis.ResponseError:
                continue
            c.add_node().wait_for_node_voting()

    eq_(int(c.raft_exec('GET', 'counter')), cycles)
