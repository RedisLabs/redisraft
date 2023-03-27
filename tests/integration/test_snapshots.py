"""
Copyright Redis Ltd. 2020 - present
Licensed under your choice of the Redis Source Available License 2.0 (RSALv2)
or the Server Side Public License v1 (SSPLv1).
"""

import shutil
import os
import time

from redis import ResponseError, ConnectionError
from pytest import raises
from retry import retry

from .raftlog import RaftLog, LogEntry
from .sandbox import RawConnection


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


def test_snapshot_delivery_in_chunks(cluster):
    """
    Ability to properly deliver snapshot in chunks.
    """

    r1 = cluster.add_node()
    r1.config_set('raft.snapshot-req-max-size', 128)

    for i in range(2000):
        r1.execute('set', i, i)

    assert r1.client.execute_command('RAFT.DEBUG', 'COMPACT') == b'OK'
    assert r1.info()['raft_log_entries'] == 0

    r2 = cluster.add_node()
    cluster.wait_for_unanimity()

    # r2 should receive many chunks as max size is quite low.
    assert r2.info()['raft_snapshotreq_received'] > 100


def test_big_snapshot_delivery(cluster):
    """
    Ability to properly deliver and load a big snapshot file (~70 Mb on disk).
    """

    cluster.create(1, raft_args={'response-timeout': 5000})
    cluster.execute('set', 'x', '1')
    cluster.execute('set', 'x', '1')

    n1 = cluster.node(1)
    n1.raft_debug_exec('debug', 'populate', 2000000, 100000)
    n1.client.execute_command('raft.debug', 'compact')

    cluster.add_node(use_cluster_args=True)
    cluster.add_node(use_cluster_args=True)

    cluster.wait_for_unanimity()
    assert cluster.node(2).info()['raft_snapshots_received'] > 0
    assert cluster.node(3).info()['raft_snapshots_received'] > 0


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
    cluster.node(2).wait_for_current_index(9)
    log = RaftLog(cluster.node(2).raftlog)
    log.read()
    assert log.header().snapshot_index() == 9


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
    cluster.add_node()
    cluster.wait_for_unanimity()

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
    assert info['raft_current_index'] == 12

    # Make sure log is compacted
    assert cluster.node(1).client.execute_command(
        'RAFT.DEBUG', 'COMPACT') == b'OK'
    info = cluster.node(1).info()
    assert info['raft_log_entries'] == 0
    assert info['raft_current_index'] == 12
    assert info['raft_commit_index'] == 12


def test_cfg_node_removed_from_snapshot(cluster):
    """
    Node able to learn that another node left by reading the snapshot metadata.
    """
    cluster.create(5)
    cluster.node(1).client.set('key', 'value')
    cluster.wait_for_unanimity()

    # interrupt
    # we now take down node 4, so it doesn't get updates, and remove node 5.
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

    cluster.node(1).config_set("raft.snapshot-delay", "3")
    conn = cluster.node(1).client.connection_pool.get_connection('compact')
    conn.send_command('raft.debug', 'compact')

    cluster.node(1).wait_for_info_param('raft_snapshot_in_progress', 'yes')

    conn2 = cluster.node(1).client.connection_pool.get_connection('raft')
    conn2.send_command('set', 'key2', 'value2')  # Entry idx 7

    assert cluster.node(1).current_index() == 8
    assert cluster.node(1).commit_index() == 7
    assert conn.read_response() == b'OK'
    assert cluster.node(1).info()['raft_log_entries'] == 1

    log = RaftLog(cluster.node(1).raftlog)
    log.read()
    assert log.entry_count(LogEntry.LogType.NORMAL) == 1

    cluster.node(1).kill()
    cluster.node(1).start()
    cluster.node(1).wait_for_info_param('raft_state', 'up')
    assert cluster.node(1).current_index() == 8
    assert cluster.node(1).commit_index() == 7
    assert cluster.node(1).info()['raft_log_entries'] == 1


def test_new_uncommitted_during_rewrite(cluster):
    cluster.create(3)

    # Take down majority to create uncommitted entries and check rewrite
    cluster.node(1).client.set('key', '1')

    # Initiate compaction and wait to see it's in progress
    cluster.node(1).config_set("raft.snapshot-delay", "2")
    conn = cluster.node(1).client.connection_pool.get_connection('COMPACT')
    conn.send_command('RAFT.DEBUG', 'COMPACT')
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

    assert cluster.node(1).execute('RAFT.DEBUG', 'COMPACT') == b'OK'

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
    assert cluster.node(1).execute('RAFT.DEBUG', 'COMPACT') == b'OK'

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

    r1.config_set("raft.snapshot-fail", "yes")
    with raises(ResponseError):
        r1.client.execute_command('RAFT.DEBUG', 'COMPACT')

    r1.config_set("raft.snapshot-fail", "no")
    assert r1.info()['raft_snapshots_created'] == 0
    r1.wait_for_info_param('raft_snapshot_in_progress', 'no')

    r1.client.incr('testkey')
    r1.client.incr('testkey')
    assert r1.client.get('testkey') == b'6'

    # Wait a bit between snapshot requests. Redis collects Fork() results
    # with an interval. If we trigger a new snapshot before that, snapshot
    # will fail.
    time.sleep(3)

    # Verify that retry was successful
    assert r1.info()['raft_snapshots_created'] == 1

    # Verify in progress stats are cleared
    assert r1.info()['raft_snapshot_in_progress_last_idx'] == -1
    assert r1.info()['raft_snapshot_in_progress_last_term'] == -1

    assert r1.execute('RAFT.DEBUG', 'COMPACT') == b'OK'
    assert r1.info()['raft_snapshots_created'] == 2

    r1.client.incr('testkey')
    r1.client.incr('testkey')
    assert r1.client.get('testkey') == b'8'


def test_snapshot_state_after_restart(cluster):
    """
    Verify snapshot state after loading local snapshot
    """

    cluster.create(3)

    # Bump the log a bit
    for _ in range(20):
        assert cluster.execute('INCR', 'testkey')

    # Compact to get rid of logs
    node3 = cluster.node(3)
    assert node3.client.execute_command('RAFT.DEBUG', 'COMPACT') == b'OK'
    info = node3.info()
    snapshot_idx = info['raft_snapshot_last_idx']
    snapshot_term = info['raft_snapshot_last_term']
    snapshot_size = info['raft_snapshot_size']
    assert snapshot_idx != 0
    assert snapshot_term != 0
    assert snapshot_size != 0
    assert info['raft_snapshot_time_secs'] != -1

    # Restart node
    node3.restart()
    cluster.node(3).wait_for_info_param('raft_state', 'up')

    info = node3.info()
    assert info['raft_snapshot_last_idx'] == snapshot_idx
    assert info['raft_snapshot_last_term'] == snapshot_term
    assert info['raft_snapshot_size'] == snapshot_size
    assert info['raft_snapshot_time_secs'] == -1
    assert info['raft_snapshot_in_progress_last_idx'] == -1
    assert info['raft_snapshot_in_progress_last_term'] == -1


def test_snapshot_state_after_snapshot_receiving(cluster):
    """
    Verify snapshot state after receiving remote snapshot
    """

    cluster.create(3, prepopulate_log=20)

    # Stop node 3, advance the log, then compact.
    cluster.node(3).terminate()
    for _ in range(20):
        assert cluster.execute('INCR', 'testkey')
    assert cluster.node(1).execute('RAFT.DEBUG', 'COMPACT') == b'OK'

    info = cluster.node(1).info()
    snapshot_idx = info['raft_snapshot_last_idx']
    snapshot_term = info['raft_snapshot_last_term']
    snapshot_size = info['raft_snapshot_size']
    assert snapshot_idx != 0
    assert snapshot_term != 0
    assert snapshot_size != 0
    assert info['raft_snapshot_time_secs'] != -1

    # Start node 3 and wait for it to receive a snapshot
    cluster.node(3).start()
    cluster.node(3).wait_for_info_param('raft_state', 'up')
    cluster.node(3).wait_for_info_param('raft_snapshots_received', 1)

    info = cluster.node(3).info()
    assert info['raft_snapshot_last_idx'] == snapshot_idx
    assert info['raft_snapshot_last_term'] == snapshot_term
    assert info['raft_snapshot_size'] == snapshot_size
    assert info['raft_snapshot_time_secs'] == -1


def test_snapshot_state_on_success(cluster):
    """
    Verify snapshot state after taking local snapshot operation successfully
    """

    cluster.create(1)
    for _ in range(10):
        cluster.node(1).client.incr('counter')
    info = cluster.node(1).info()
    assert info['raft_current_index'] == 12

    # Make sure log is compacted
    assert cluster.node(1).client.execute_command(
        'RAFT.DEBUG', 'COMPACT') == b'OK'

    info = cluster.node(1).info()
    assert info['raft_snapshots_created'] == 1
    assert info['raft_snapshot_last_idx'] == 12
    assert info['raft_snapshot_last_term'] == 1
    assert info['raft_snapshot_size'] != 0
    assert info['raft_snapshot_time_secs'] != -1
    assert info['raft_snapshot_in_progress_last_idx'] == -1
    assert info['raft_snapshot_in_progress_last_term'] == -1


def test_snapshot_state_on_failure(cluster):
    """
    Verify snapshot state after local snapshot operation failure
    """

    r1 = cluster.add_node()
    r1.client.incr('testkey')
    r1.client.incr('testkey')
    r1.client.incr('testkey')
    r1.client.incr('testkey')
    assert r1.client.get('testkey') == b'4'

    assert r1.info()['raft_snapshots_created'] == 0

    # Disable snapshot so, the node will not retry after snapshot failure
    r1.config_set('raft.snapshot-disable', 'yes')

    r1.config_set("raft.snapshot-fail", "yes")
    with raises(ResponseError):
        r1.client.execute_command('RAFT.DEBUG', 'COMPACT')

    r1.wait_for_info_param('raft_snapshot_in_progress', 'no')
    info = r1.info()
    assert info['raft_snapshots_created'] == 0
    assert info['raft_snapshot_last_idx'] == 0
    assert info['raft_snapshot_last_term'] == 0
    assert info['raft_snapshot_size'] == 0
    assert info['raft_snapshot_time_secs'] == -1
    assert info['raft_snapshot_in_progress_last_idx'] == -1
    assert info['raft_snapshot_in_progress_last_term'] == -1


def test_snapshot_in_progress_stats(cluster):
    """
    Verify snapshot in progress stats are updated correctly.
    """
    r1 = cluster.add_node()
    r1.client.incr('testkey')

    # Verify initial state stats
    info = r1.info()
    assert info['raft_snapshot_time_secs'] == -1
    assert info['raft_snapshot_in_progress_last_idx'] == -1
    assert info['raft_snapshot_in_progress_last_term'] == -1

    r1.config_set('raft.snapshot-delay', 3)
    r1.execute('RAFT.DEBUG', 'COMPACT', '1')

    # Verify stats while snapshot is in progress
    r1.wait_for_info_param('raft_snapshot_in_progress', 'yes')
    info = r1.info()
    assert info['raft_snapshot_time_secs'] == -1
    assert info['raft_snapshot_in_progress_last_idx'] != -1
    assert info['raft_snapshot_in_progress_last_term'] != -1

    # Verify stats after snapshot process has been completed
    r1.wait_for_info_param('raft_snapshot_in_progress', 'no')
    info = r1.info()
    assert info['raft_snapshot_time_secs'] != -1
    assert info['raft_snapshot_in_progress_last_idx'] == -1
    assert info['raft_snapshot_in_progress_last_term'] == -1


def test_snapshot_sessions(cluster):
    """
    tests that sessions are saved to/loaded from snapshots
    correctly/consistently to cluster
    """
    cluster.create(3)

    cluster.wait_for_info_param("raft_num_sessions", 0)

    conn1 = RawConnection(cluster.leader_node().client)
    conn1.execute("WATCH", "X")
    cluster.wait_for_unanimity()

    cluster.wait_for_info_param("raft_num_sessions", 1)

    conn2 = RawConnection(cluster.leader_node().client)
    conn2.execute("WATCH", "X")
    cluster.wait_for_unanimity()

    cluster.wait_for_info_param("raft_num_sessions", 2)

    n2 = cluster.node(2)

    assert n2.execute('RAFT.DEBUG', 'COMPACT') == b'OK'
    assert n2.info()['raft_log_entries'] == 0
    n2.restart()
    n2.wait_for_node_voting()

    cluster.wait_for_info_param("raft_num_sessions", 2)

    cluster.execute("set", "a", "123")

    conn1.execute("UNWATCH")
    cluster.wait_for_unanimity()
    cluster.wait_for_info_param("raft_num_sessions", 1)

    n2 = cluster.node(2)
    assert n2.execute('RAFT.DEBUG', 'COMPACT') == b'OK'
    assert n2.info()['raft_log_entries'] == 0
    n2.restart()
    n2.wait_for_node_voting()
    cluster.wait_for_info_param("raft_num_sessions", 1)

    cluster.execute("set", "b", "123")

    conn2.execute("UNWATCH")
    cluster.wait_for_unanimity()
    cluster.wait_for_info_param("raft_num_sessions", 0)

    n2 = cluster.node(2)
    assert n2.execute('RAFT.DEBUG', 'COMPACT') == b'OK'
    assert n2.info()['raft_log_entries'] == 0
    n2.restart()
    n2.wait_for_node_voting()
    cluster.wait_for_info_param("raft_num_sessions", 0)


def test_session_cleaned_on_load(cluster):
    """
    Enssure that if we load a snapshot (i.e. rejoin cluster), we clear up
    session state, so nothing remains from before snapshot load
    """
    cluster.create(3)

    cluster.wait_for_info_param("raft_num_sessions", 0)

    conn1 = RawConnection(cluster.leader_node().client)

    conn1.execute("WATCH", "X")
    cluster.wait_for_unanimity()

    cluster.wait_for_info_param("raft_num_sessions", 1)

    old_leader = cluster.pause_leader()
    cluster.node(2).wait_for_leader_change(old_leader)
    cluster.update_leader()
    cluster.execute("set", "x", 1)

    assert cluster.leader_node().execute('RAFT.DEBUG', 'COMPACT') == b'OK'
    assert cluster.leader_node().info()['raft_log_entries'] == 0

    cluster.node(old_leader).resume()
    cluster.wait_for_unanimity()

    cluster.wait_for_info_param("raft_num_sessions", 0)

    with raises(ConnectionError, match="Connection closed by server"):
        conn1.execute("get", "X")
