import sys
import time
import sandbox
from nose.tools import eq_, ok_
from test_tools import with_setup_args

def _setup():
    return [sandbox.Cluster()], {}

def _teardown(c):
    c.destroy()

@with_setup_args(_setup, _teardown)
def test_add_node_as_a_single_leader(c):
    """
    Single node becomes a leader
    """
    # Do some basic sanity
    r1 = c.add_node()
    ok_(r1.raft_exec('SET', 'key', 'value'))
    eq_(r1.raft_info()['current_index'], 2)

@with_setup_args(_setup, _teardown)
def test_node_joins_and_gets_data(c):
    """
    Node joins and gets data
    """
    r1 = c.add_node()
    eq_(r1.raft_exec('SET', 'key', 'value'), b'OK')
    r2 = c.add_node()
    r2.wait_for_election()
    eq_(r2.raft_info().get('leader_id'), 1)
    eq_(r2.client.get('key'), b'value')

@with_setup_args(_setup, _teardown)
def test_single_node_log_is_reapplied(c):
    """Single node log is reapplied on startup"""
    r1 = c.add_node()
    ok_(r1.raft_exec('SET', 'key', 'value'))
    r1.restart()
    r1.wait_for_election()
    eq_(r1.raft_info().get('leader_id'), 1)
    r1.wait_for_log_applied()
    eq_(r1.client.get('key'), b'value')

@with_setup_args(_setup, _teardown)
def test_reelection_basic_flow(c):
    """
    Basic reelection flow
    """
    c.create(3)
    eq_(c.leader, 1)
    eq_(c.raft_exec('SET', 'key', 'value'), b'OK')
    c.node(1).terminate()
    c.node(2).wait_for_election()
    eq_(c.raft_exec('SET', 'key2', 'value2'), b'OK')
    res = c.exec_all('GET', 'key2')
