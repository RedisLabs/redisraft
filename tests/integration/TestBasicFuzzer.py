import sys
import random
import sandbox
from nose.tools import eq_, ok_
from test_tools import with_setup_args

def _setup():
    return [sandbox.Cluster()], {}

def _teardown(c):
    c.destroy()

@with_setup_args(_setup, _teardown)
def test_basic_persisted_fuzzer(c):
    """
    Basic Raft fuzzer test
    """

    nodes = 3
    cycles = 1000

    c.create(nodes, raft_args={'persist': 'yes'})
    for i in range(cycles):
        ok_(c.raft_exec('INCRBY', 'counter', 1))
        if i % 7 == 0:
            c.node(random.randint(1, nodes)).restart()

    eq_(int(c.raft_exec('GET', 'counter')), cycles)
