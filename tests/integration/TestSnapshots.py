import sys
import time
import sandbox
from nose.tools import eq_, ok_, assert_greater
from test_tools import with_setup_args

def _setup():
    return [sandbox.Cluster()], {}

def _teardown(c):
    c.destroy()

@with_setup_args(_setup, _teardown)
def test_compaction_thresholds(c):
    """
    Log compaction behaves according to configuration
    """

    r1 = c.add_node()
    eq_(r1.raft_info()['log_entries'], 1)
    ok_(r1.raft_config_set('max_log_entries', 5))
    for x in range(10):
        ok_(r1.raft_exec('SET', 'testkey', x))
    time.sleep(1)
    assert_greater(5, r1.raft_info()['log_entries'])

@with_setup_args(_setup, _teardown)
def test_cfg_node_added_from_snapshot(c):
    """
    Node able to join cluster and read cfg and data from snapshot.
    """
    c.create(2, raft_args={'persist': 'no'})
    for i in range(100):
        c.node(1).raft_exec('SET', 'key%s' % i, 'val%s' % i)
        c.node(1).raft_exec('INCR', 'counter')

    # Make sure log is compacted
    eq_(c.node(1).client.execute_command('RAFT.DEBUG', 'COMPACT'), b'OK')
    eq_(0, c.node(1).raft_info()['log_entries'])

    # Add new node and wait for things to settle
    r3 = c.add_node()
    r3.wait_for_election()

    # Make sure we have what we expect
    for i in range(100):
        eq_(str(c.node(3).client.get('key%s' % i), 'utf-8'), 'val%s' % i)
    eq_(c.node(3).client.get('counter'), b'100')

@with_setup_args(_setup, _teardown)
def test_cfg_node_removed_from_snapshot(c):
    """
    Node able to learn that another node left by reading the snapshot metadata.
    """
    c.create(5, raft_args={'persist': 'yes'})
    c.node(1).raft_exec('SET', 'key', 'value')
    c.wait_for_unanimity()

    # interrupt
    # we now take down node 4 so it doesn't get updates and remove node 5.
    c.node(4).terminate()
    c.remove_node(5)
    c.wait_for_unanimity(exclude=[4])
    c.node(1).wait_for_log_applied()
    eq_(4, c.node(1).raft_info()['num_nodes'])

    # now compact logs
    c.wait_for_unanimity(exclude=[4])
    eq_(c.node(1).client.execute_command('RAFT.DEBUG', 'COMPACT'), b'OK')
    eq_(0, c.node(1).raft_info()['log_entries'])

    # bring back node 4
    c.node(4).start()
    c.node(4).wait_for_election()
    eq_(5, c.node(4).raft_info()['num_nodes'])
