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
    eq_(0, r1.raft_info()['log_entries'])

@with_setup_args(_setup, _teardown)
def test_cfg_from_snapshot(c):
    """
    Node able to join cluster and read cfg and data from snapshot.
    """
    c.create(2, raft_args={'persist': 'no'})
    for i in range(100):
        eq_(c.node(1).raft_exec('SET', 'key%s' % i, 'val%s' % i), b'OK')

    # Make sure log is compacted
    eq_(c.node(1).client.execute_command('RAFT.DEBUG', 'COMPACT'), b'OK')
    eq_(0, c.node(1).raft_info()['log_entries'])

    # Add new node
    r3 = c.add_node()
    r3.wait_for_election()
