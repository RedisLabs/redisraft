"""
This file is part of RedisRaft.

Copyright (c) 2020-2021 Redis Ltd.

RedisRaft is licensed under the Redis Source Available License (RSAL).
"""

import shutil
import os
import time

from redis import ResponseError
from pytest import raises
from .raftlog import RaftLog, LogEntry


def test_snapshot_delivery_to_new_node(cluster):
    """
    Ability to properly deliver and load a snapshot.
    """

    r1 = cluster.add_node()
    r1.client.incr('testkey')
    r1.client.incr('testkey')
    r1.client.incr('testkey')
    r1.client.setrange('bigkey', '104857600', 'x')
    r1.client.incr('testkey')
    assert r1.client.get('testkey') == b'4'

    assert r1.client.execute_command('RAFT.DEBUG', 'COMPACT') == b'OK'
    assert r1.info()['raft_log_entries'] == 0

    r2 = cluster.add_node()
    cluster.wait_for_unanimity()
    assert r2.raft_debug_exec('GET', 'testkey') == b'4'


def test_snapshot_delivery(cluster):
    """
    Ability to properly deliver and load a snapshot.
    """

    cluster.create(3)
    n1 = cluster.node(1)
    n1.client.incr('testkey')
    n1.client.incr('testkey')
    n1.client.incr('testkey')
    for i in range(1000):
        pipe = n1.client.pipeline(transaction=True)
        for j in range(100):
            pipe.rpush('list-%s' % i, 'elem-%s' % j)
        pipe.execute()
    cluster.node(3).terminate()
    n1.client.setrange('bigkey', '104857600', 'x')
    n1.client.incr('testkey')
    assert n1.raft_debug_exec('GET', 'testkey') == b'4'

    assert n1.client.execute_command('RAFT.DEBUG', 'COMPACT') == b'OK'
    assert n1.info()['raft_log_entries'] == 0

    n3 = cluster.node(3)
    n3.start()
    n1.client.incr('testkey')

    n3.wait_for_node_voting()
    cluster.wait_for_unanimity()
    n3.wait_for_log_applied()

    assert n3.raft_debug_exec('GET', 'testkey') == b'5'


def test_log_fixup_after_snapshot_delivery(cluster):
    """
    Log must be restarted when loading a snapshot.
    """

    cluster.create(3)
    cluster.node(1).client.incr('key')
    cluster.node(1).client.incr('key')

    cluster.node(2).terminate()
    cluster.node(1).client.incr('key')
    assert cluster.node(1).client.execute_command(
        'RAFT.DEBUG', 'COMPACT') == b'OK'

    # Confirm node 2's log starts from 1
    log = RaftLog(cluster.node(2).raftlog)
    log.read()
    assert log.header().snapshot_index() == 0

    cluster.node(2).start()
    cluster.node(2).wait_for_info_param('raft_state', 'up')

    # node 2 must get a snapshot to sync, make sure this happens and that
    # the log is ok.
    cluster.node(2).wait_for_current_index(8)
    log = RaftLog(cluster.node(2).raftlog)
    log.read()
    assert log.header().snapshot_index() == 8


def test_cfg_node_added_from_snapshot(cluster):
    """
    Node able to join cluster and read cfg and data from snapshot.
    """
    cluster.create(2)
    for i in range(100):
        cluster.node(1).client.set('key%s' % i, 'val%s' % i)
        cluster.node(1).client.incr('counter')

    # Make sure log is compacted
    assert cluster.node(1).client.execute_command(
        'RAFT.DEBUG', 'COMPACT') == b'OK'
    assert cluster.node(1).info()['raft_log_entries'] == 0

    # Add new node and wait for things to settle
    r3 = cluster.add_node()
    r3.wait_for_election()

    # Make sure we have what we expect
    for i in range(100):
        assert str(cluster.node(3).raft_debug_exec('GET', 'key%s' % i),
                   'utf-8') == 'val%s' % i
    assert cluster.node(3).raft_debug_exec('GET', 'counter') == b'100'


def test_index_correct_right_after_snapshot(cluster):
    cluster.create(1)
    for _ in range(10):
        cluster.node(1).client.incr('counter')
    info = cluster.node(1).info()
    assert info['raft_current_index'] == 11

    # Make sure log is compacted
    assert cluster.node(1).client.execute_command(
        'RAFT.DEBUG', 'COMPACT') == b'OK'
    info = cluster.node(1).info()
    assert info['raft_log_entries'] == 0
    assert info['raft_current_index'] == 11
    assert info['raft_commit_index'] == 11


def test_cfg_node_removed_from_snapshot(cluster):
    """
    Node able to learn that another node left by reading the snapshot metadata.
    """
    cluster.create(5)
    cluster.node(1).client.set('key', 'value')
    cluster.wait_for_unanimity()

    # interrupt
    # we now take down node 4 so it doesn't get updates and remove node 5.
    cluster.node(4).terminate()
    cluster.remove_node(5)
    cluster.wait_for_unanimity(exclude=[4])
    cluster.node(1).wait_for_log_applied()
    assert cluster.node(1).info()['raft_num_nodes'] == 4

    # now compact logs
    cluster.wait_for_unanimity(exclude=[4])
    assert cluster.node(1).client.execute_command(
        'RAFT.DEBUG', 'COMPACT') == b'OK'
    assert cluster.node(1).info()['raft_log_entries'] == 0

    # bring back node 4
    cluster.node(4).start()
    cluster.node(4).wait_for_election()
    cluster.wait_for_unanimity()
    assert cluster.node(4).info()['raft_num_nodes'] == 4


def test_all_committed_log_rewrite(cluster):
    """
    Log rewrite operation when all entries are committed, so we expect an
    empty log.
    """
    cluster.create(3)
    cluster.node(1).client.set('key1', 'value')
    cluster.node(1).client.set('key2', 'value')
    cluster.node(1).client.set('key3', 'value')
    cluster.wait_for_unanimity()

    assert cluster.node(1).client.execute_command(
        'RAFT.DEBUG', 'COMPACT') == b'OK'
    assert cluster.node(1).info()['raft_log_entries'] == 0

    # Make sure we have no log entries!
    log = RaftLog(cluster.node(1).raftlog)
    log.read()
    assert log.entry_count(LogEntry.LogType.NORMAL) == 0


def test_uncommitted_log_rewrite(cluster):
    cluster.create(3)

    # Log contains 5 config entries

    # Take down majority to create uncommitted entries and check rewrite
    cluster.node(1).client.set('key', 'value')  # Entry idx 6
    cluster.node(2).terminate()
    cluster.node(3).terminate()
    conn = cluster.node(1).client.connection_pool.get_connection('RAFT')
    conn.send_command('SET', 'key2', 'value2')  # Entry idx 7

    assert cluster.node(1).current_index() == 7
    assert cluster.node(1).commit_index() == 6
    assert cluster.node(1).client.execute_command(
        'RAFT.DEBUG', 'COMPACT') == b'OK'
    assert cluster.node(1).info()['raft_log_entries'] == 1

    log = RaftLog(cluster.node(1).raftlog)
    log.read()
    assert log.entry_count(LogEntry.LogType.NORMAL) == 1

    cluster.node(1).kill()
    cluster.node(1).start()
    cluster.node(1).wait_for_info_param('raft_state', 'up')
    assert cluster.node(1).current_index() == 7
    assert cluster.node(1).commit_index() == 6
    assert cluster.node(1).info()['raft_log_entries'] == 1


def test_new_uncommitted_during_rewrite(cluster):
    cluster.create(3)

    # Take down majority to create uncommitted entries and check rewrite
    cluster.node(1).client.set('key', '1')

    # Initiate compaction and wait to see it's in progress
    conn = cluster.node(1).client.connection_pool.get_connection('COMPACT')
    conn.send_command('RAFT.DEBUG', 'COMPACT', '2')
    cluster.node(1).wait_for_info_param('raft_snapshot_in_progress', 'yes')
    assert cluster.node(1).info()['raft_snapshot_in_progress'] == 'yes'

    # Send a bunch of writes
    cluster.node(1).client.incrby('key', '2')
    cluster.node(1).client.incrby('key', '3')
    cluster.node(1).client.incrby('key', '4')

    # Wait for compaction to end
    assert cluster.node(1).info()['raft_snapshot_in_progress'] == 'yes'
    cluster.node(1).wait_for_info_param('raft_snapshot_in_progress', 'no')

    # Make sure our writes made it to the log
    log = RaftLog(cluster.node(1).raftlog)
    log.read()
    assert log.entry_count(LogEntry.LogType.NORMAL) == 3

    # Extra check -- Make sure we can read it back. Note that we need to start
    # all nodes because we don't log the commit index.
    cluster.node(1).terminate()
    cluster.node(2).terminate()
    cluster.node(3).terminate()
    cluster.node(1).start()
    cluster.node(2).start()
    cluster.node(3).start()

    cluster.node(1).wait_for_info_param('raft_state', 'up')

    # Make sure cluster state is as expected
    assert cluster.execute('get', 'key') == b'10'

    # Make sure node 1 state is as expected
    cluster.node(1).wait_for_log_applied()
    assert cluster.node(1).raft_debug_exec('GET', 'key') == b'10'


def test_identical_snapshot_and_log(cluster):
    r1 = cluster.add_node()
    assert r1.client.incr('testkey')
    assert r1.client.incr('testkey')
    r1.terminate()
    r1.start()
    r1.wait_for_info_param('raft_state', 'up')

    # Both log and snapshot have all entries
    assert r1.raft_debug_exec('GET', 'testkey') == b'2'


def test_loading_log_tail(cluster):
    r1 = cluster.add_node()
    assert r1.client.incr('testkey')
    assert r1.client.incr('testkey')
    assert r1.client.incr('testkey')
    assert r1.client.execute_command('RAFT.DEBUG', 'COMPACT') == b'OK'
    assert r1.client.incr('testkey')
    assert r1.client.incr('testkey')
    assert r1.client.incr('testkey')
    r1.kill()
    r1.start()
    r1.wait_for_info_param('raft_state', 'up')

    # Log contains all entries
    # Snapshot has all but last 3 entries
    assert r1.raft_debug_exec('GET', 'testkey') == b'6'


def test_loading_log_tail_after_rewrite(cluster):
    r1 = cluster.add_node()
    assert r1.client.incr('testkey')
    assert r1.client.incr('testkey')
    assert r1.client.incr('testkey')
    assert r1.client.execute_command('RAFT.DEBUG', 'COMPACT') == b'OK'
    assert r1.client.incr('testkey')
    assert r1.client.incr('testkey')
    assert r1.client.incr('testkey')
    r1.kill()
    r1.start()
    r1.wait_for_info_param('raft_state', 'up')

    # Log contains last 3 entries
    # Snapshot has first 3 entries
    assert r1.raft_debug_exec('GET', 'testkey') == b'6'


def test_config_from_second_generation_snapshot(cluster):
    """
    A regression test for #44: confirm that if we load a snapshot
    on startup, do nothing, then re-create a snapshot we don't end
    up with a messed up nodes config.
    """
    cluster.create(3)

    # Bump the log a bit
    for _ in range(20):
        assert cluster.execute('INCR', 'testkey')

    # Compact to get rid of logs
    node3 = cluster.node(3)
    assert node3.client.execute_command('RAFT.DEBUG', 'COMPACT') == b'OK'

    # Restart node
    node3.restart()
    node3.wait_for_node_voting()

    # Bump the log a bit
    for _ in range(20):
        assert cluster.execute('INCR', 'testkey')

    # Recompact
    cluster.wait_for_unanimity()
    assert node3.client.execute_command('RAFT.DEBUG', 'COMPACT') == b'OK'

    node3.restart()
    node3.wait_for_node_voting()

    assert node3.info()['raft_num_nodes'] == 3


def test_stale_log_trim(cluster):
    """
    When starting up, if log is older than snapshot it should be trimmed.
    """

    cluster.create(3, prepopulate_log=20)

    # Stop node 3 and advance the log; then overwrite node3
    # with a recent snapshot and start it. This simulates delivery of snapshot
    # and a crash sometime before the log is adjusted.
    cluster.node(3).terminate()
    for _ in range(20):
        assert cluster.execute('INCR', 'testkey')
    assert cluster.node(1).client.execute_command('RAFT.DEBUG', 'COMPACT') == b'OK'
    for _ in range(20):
        assert cluster.execute('INCR', 'testkey')
    shutil.copyfile(os.path.join(os.curdir, cluster.node(1).dbfilename),
        os.path.join(os.curdir, cluster.node(3).dbfilename))

    cluster.node(3).start()
    cluster.node(3).wait_for_node_voting()

    assert cluster.execute('INCR', 'last-key')


def test_log_reset_on_snapshot_load(cluster):
    """
    Test correct reset of log when a snapshot is received.
    """

    cluster.create(3, prepopulate_log=20)

    # Stop node 3, advance the log, then compact.
    cluster.node(3).terminate()
    for _ in range(20):
        assert cluster.execute('INCR', 'testkey')
    assert cluster.node(1).client.execute_command('RAFT.DEBUG', 'COMPACT') == b'OK'

    # Start node 3 and wait for it to receive a snapshot
    cluster.node(3).start()
    cluster.node(3).wait_for_node_voting()

    # Restart node 3 and make sure it correctly started
    cluster.node(3).terminate()
    cluster.node(3).start()
    cluster.node(3).wait_for_node_voting()

    assert cluster.execute('INCR', 'last-key')
    cluster.wait_for_unanimity()

def test_snapshot_fork_failure(cluster):
    """
    Ability to properly handle snapshot fork failure
    """

    r1 = cluster.add_node()
    r1.client.incr('testkey')
    r1.client.incr('testkey')
    r1.client.incr('testkey')
    r1.client.incr('testkey')
    assert r1.client.get('testkey') == b'4'

    assert r1.info()['raft_snapshots_created'] == 0

    with raises(ResponseError):
        r1.client.execute_command('RAFT.DEBUG', 'COMPACT', '0', '1')
    assert r1.info()['raft_snapshots_created'] == 0
    r1.wait_for_info_param('raft_snapshot_in_progress', 'no')

    r1.client.incr('testkey')
    r1.client.incr('testkey')
    assert r1.client.get('testkey') == b'6'

    # Wait a bit between snapshot requests. Redis collects Fork() results
    # with an interval. If we trigger a new snapshot before that, snapshot
    # will fail.
    time.sleep(3)

    assert r1.client.execute_command('RAFT.DEBUG', 'COMPACT', '0', '0') == b'OK'
    assert r1.info()['raft_snapshots_created'] == 1

    r1.client.incr('testkey')
    r1.client.incr('testkey')
    assert r1.client.get('testkey') == b'8'
