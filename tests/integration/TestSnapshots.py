import sys
import time
import sandbox
from nose.tools import eq_, ok_, assert_greater, nottest
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
def test_snapshot_delivery(c):
    """
    Ability to properly deliver and load a snapshot.
    """

    r1 = c.add_node()
    r1.raft_exec('INCR', 'testkey')
    r1.raft_exec('INCR', 'testkey')
    r1.raft_exec('INCR', 'testkey')
    r1.raft_exec('SETRANGE', 'bigkey', '104857600', 'x')
    r1.raft_exec('INCR', 'testkey')
    eq_(r1.client.get('testkey'), b'4')

    eq_(r1.client.execute_command('RAFT.DEBUG', 'COMPACT'), b'OK')
    eq_(0, r1.raft_info()['log_entries'])

    r2 = c.add_node()
    c.wait_for_unanimity()
    eq_(r2.client.get('testkey'), b'4')

@with_setup_args(_setup, _teardown)
def test_log_fixup_after_snapshot_delivery(c):
    """
    Log must be restarted when loading a snapshot.
    """

    c.create(3, persist_log=True)
    c.node(1).raft_exec('INCR', 'key')
    c.node(1).raft_exec('INCR', 'key')

    c.node(2).terminate()
    c.node(1).raft_exec('INCR', 'key')
    eq_(c.node(1).client.execute_command('RAFT.DEBUG', 'COMPACT'), b'OK')

    # Confirm node 2's log starts from 1
    log = RaftLog(c.node(2).raftlog)
    log.read()
    eq_(log.header().snapshot_index(), 0)

    c.node(2).start()
    c.node(2).wait_for_info_param('state', 'up')

    # node 2 must get a snapshot to sync, make sure this happens and that
    # the log is ok.
    c.node(2).wait_for_current_index(8)
    log = RaftLog(c.node(2).raftlog)
    log.read()
    eq_(log.header().snapshot_index(), 8)

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
    c.node(4).wait_for_log_applied()
    eq_(4, c.node(4).raft_info()['num_nodes'])

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

    # Log contains 5 config entries

    # Take down majority to create uncommitted entries and check rewrite
    c.node(1).raft_exec('SET', 'key', 'value')  # Entry idx 6
    c.node(2).terminate()
    c.node(3).terminate()
    conn = c.node(1).client.connection_pool.get_connection('RAFT')
    conn.send_command('RAFT', 'SET', 'key2', 'value2')  # Entry idx 7

    eq_(c.node(1).current_index(), 7)
    eq_(c.node(1).commit_index(), 6)
    eq_(c.node(1).client.execute_command('RAFT.DEBUG', 'COMPACT'), b'OK')
    eq_(1, c.node(1).raft_info()['log_entries'])

    log = RaftLog(c.node(1).raftlog)
    log.read()
    eq_(1, log.entry_count(LogEntry.LogType.NORMAL))

    c.node(1).kill()
    c.node(1).start()
    c.node(1).wait_for_info_param('state', 'up')
    eq_(c.node(1).current_index(), 7)
    eq_(c.node(1).commit_index(), 6)
    eq_(1, c.node(1).raft_info()['log_entries'])

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

    # Extra check -- Make sure we can read it back. Note that we need to start
    # all nodes because we don't log the commit index.
    c.node(1).terminate()
    c.node(2).terminate()
    c.node(3).terminate()
    c.node(1).start()
    c.node(2).start()
    c.node(3).start()

    c.node(1).wait_for_info_param('state', 'up')

    # TODO: Need a log entry for the commit index to be re-computed; In the
    # future Redis Raft should do that implicitly with a no-op.
    c.raft_exec('set','no-op','no-op')

    # Make sure cluster state is as expected
    eq_(b'10', c.raft_exec('get', 'key'))

    # Make sure node 1 state is as expected
    c.node(1).wait_for_log_applied()
    eq_(b'10', c.node(1).client.get('key'))

@with_setup_args(_setup, _teardown)
def test_identical_snapshot_and_log(c):
    r1 = c.add_node(raft_args={'persist': 'yes'})
    ok_(r1.raft_exec('INCR', 'testkey'))
    ok_(r1.raft_exec('INCR', 'testkey'))
    r1.terminate()
    r1.start()
    r1.wait_for_info_param('state', 'up')

    # Both log and snapshot have all entries
    eq_(r1.client.get('testkey'), b'2')

@with_setup_args(_setup, _teardown)
def test_loading_log_tail(c):
    r1 = c.add_node(raft_args={'persist': 'yes'})
    ok_(r1.raft_exec('INCR', 'testkey'))
    ok_(r1.raft_exec('INCR', 'testkey'))
    ok_(r1.raft_exec('INCR', 'testkey'))
    r1.client.save()
    ok_(r1.raft_exec('INCR', 'testkey'))
    ok_(r1.raft_exec('INCR', 'testkey'))
    ok_(r1.raft_exec('INCR', 'testkey'))
    r1.kill()
    r1.start()
    r1.wait_for_info_param('state', 'up')

    # Log contains all entries
    # Snapshot has all but last 3 entries
    eq_(r1.client.get('testkey'), b'6')

@with_setup_args(_setup, _teardown)
def test_loading_log_tail_after_rewrite(c):
    r1 = c.add_node(raft_args={'persist': 'yes'})
    ok_(r1.raft_exec('INCR', 'testkey'))
    ok_(r1.raft_exec('INCR', 'testkey'))
    ok_(r1.raft_exec('INCR', 'testkey'))
    eq_(r1.client.execute_command('RAFT.DEBUG', 'COMPACT'), b'OK')
    ok_(r1.raft_exec('INCR', 'testkey'))
    ok_(r1.raft_exec('INCR', 'testkey'))
    ok_(r1.raft_exec('INCR', 'testkey'))
    r1.kill()
    r1.start()
    r1.wait_for_info_param('state', 'up')

    # Log contains last 3 entries
    # Snapshot has first 3 entries
    eq_(r1.client.get('testkey'), b'6')
