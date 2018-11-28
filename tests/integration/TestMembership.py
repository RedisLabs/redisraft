import sys
import time
import sandbox
import redis
from nose.tools import eq_, ok_, assert_raises_regex, assert_regex
from test_tools import with_setup_args
from raftlog import RaftLog, RawEntry

def _setup():
    return [sandbox.Cluster()], {}

def _teardown(c):
    c.destroy()

@with_setup_args(_setup, _teardown)
def test_node_join_iterates_all_addrs(c):
    """
    Node join iterates all addresses.
    """
    r1 = c.add_node()
    eq_(r1.raft_exec('SET', 'key', 'value'), b'OK')
    r2 = c.add_node(cluster_setup=False)
    r2.start()
    eq_(r2.cluster('join', 'localhost:1', 'localhost:2',
                        'localhost:{}'.format(c.node_ports()[0])), b'OK')
    r2.wait_for_election()

@with_setup_args(_setup, _teardown)
def test_node_join_redirects_to_leader(c):
    """
    Node join can redirect to leader.
    """
    r1 = c.add_node()
    eq_(r1.raft_exec('SET', 'key', 'value'), b'OK')
    r2 = c.add_node()
    r2.wait_for_election()
    r3 = c.add_node(cluster_setup=False)
    r3.start()
    result = r3.cluster('join', 'localhost:{}'.format(r2.port))
    r3.wait_for_election()

