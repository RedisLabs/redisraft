import sys
import time
import sandbox
from nose.tools import eq_, ok_, assert_greater
from test_tools import with_setup_args
from raftlog import RaftLog, LogEntry

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

@with_setup_args(_setup, _teardown)
def test_all_committed_log_rewrite(c):
    """
    Log rewrite operation when all entries are committed, so we expect an
    empty log.
    """
    c.create(3, raft_args={'persist': 'yes'})
    c.node(1).raft_exec('SET', 'key1', 'value')
    c.node(1).raft_exec('SET', 'key2', 'value')
    c.node(1).raft_exec('SET', 'key3', 'value')
    c.wait_for_unanimity()

    eq_(c.node(1).client.execute_command('RAFT.DEBUG', 'COMPACT'), b'OK')
    eq_(0, c.node(1).raft_info()['log_entries'])

    # Make sure we have no log entries!
    log = RaftLog(c.node(1).raftlog)
    log.read()
    eq_(0, log.entry_count(LogEntry.LogType.NORMAL))

@with_setup_args(_setup, _teardown)
def test_uncommitted_log_rewrite(c):
    c.create(3, raft_args={'persist': 'yes'})

    # Take down majority to create uncommitted entries and check rewrite
    c.node(1).raft_exec('SET', 'key', 'value')
    c.node(2).terminate()
    c.node(3).terminate()
    conn = c.node(1).client.connection_pool.get_connection('RAFT')
    conn.send_command('RAFT', 'SET', 'key2', 'value2')

    eq_(1, c.node(1).current_index() - c.node(1).commit_index())
    eq_(c.node(1).client.execute_command('RAFT.DEBUG', 'COMPACT'), b'OK')
    eq_(1, c.node(1).raft_info()['log_entries'])

    import shutil
    shutil.copy(c.node(1).raftlog, 'test.log')

    log = RaftLog(c.node(1).raftlog)
    log.read()
    eq_(1, log.entry_count(LogEntry.LogType.NORMAL))

@with_setup_args(_setup, _teardown)
def test_new_uncommitted_during_rewrite(c):
    c.create(3, raft_args={'persist': 'yes'})

    # Take down majority to create uncommitted entries and check rewrite
    c.node(1).raft_exec('SET', 'key', '1')

    # Initiate compaction and wait to see it's in progress
    conn = c.node(1).client.connection_pool.get_connection('COMPACT')
    conn.send_command('RAFT.DEBUG', 'COMPACT', '2')
    c.node(1).wait_for_info_param('snapshot_in_progress', 'yes')
    eq_('yes', c.node(1).raft_info()['snapshot_in_progress'])

    # Send a bunch of writes
    c.node(1).raft_exec('INCRBY', 'key', '2')
    c.node(1).raft_exec('INCRBY', 'key', '3')
    c.node(1).raft_exec('INCRBY', 'key', '4')

    # Wait for compaction to end
    eq_('yes', c.node(1).raft_info()['snapshot_in_progress'])
    c.node(1).wait_for_info_param('snapshot_in_progress', 'no')

    # Make sure our writes made it to the log
    log = RaftLog(c.node(1).raftlog)
    log.read()
    eq_(3, log.entry_count(LogEntry.LogType.NORMAL))

    # Make sure we can read it back
    c.node(2).terminate()
    c.node(3).terminate()
    c.node(1).restart()

    eq_(b'10', c.node(1).client.get('key'))
