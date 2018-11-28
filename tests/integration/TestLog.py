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
def test_log_rollback(c):
    """
    Rollback of log entries that were written in the minority.
    """

    c.create(3, persist_log=True)
    eq_(c.leader, 1)
    eq_(c.raft_exec('SET', 'key', 'value1'), b'OK')

    # Break cluster
    c.node(2).terminate()
    c.node(3).terminate()

    # Load a command which can't be committed
    eq_(c.node(1).current_index(), 6)
    conn = c.node(1).client.connection_pool.get_connection('RAFT')
    conn.send_command('RAFT', 'SET', 'key', 'value2')
    eq_(c.node(1).current_index(), 7)
    c.node(1).terminate()

    # We want to be sure the last entry is in the log
    log = RaftLog(c.node(1).raftlog)
    log.read()
    eq_(log.entry_count(), 7)

    # Restart the cluster without node 1, make sure the write was
    # not committed.
    c.node(2).start()
    c.node(3).start()
    c.node(2).wait_for_election()
    eq_(c.node(2).current_index(), 6)

    # Restart node 1
    c.node(1).start()
    c.node(1).wait_for_election()

    # Make another write and make sure it overwrites the previous one in
    # node 1's log
    c.raft_exec('SET', 'key', 'value3')
    eq_(c.node(1).current_index(), 7)

    # Make sure log reflects the change
    log.reset()
    log.read()
    assert_regex(str(log.entries[7].data), '.*SET.*value3')
