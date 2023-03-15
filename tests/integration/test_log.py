"""
Copyright Redis Ltd. 2020 - present
Licensed under your choice of the Redis Source Available License 2.0 (RSALv2)
or the Server Side Public License v1 (SSPLv1).
"""

import os.path
import time
from random import seed, randint
from re import match

from pytest import raises
from redis import ResponseError
from .raftlog import RaftLog, LogHeader, LogEntry


def test_log_append_random_size(cluster):
    """
    Append variable length entries to the log and verify correctness
    """

    cluster.create(3)

    seed(7129)
    result = ''

    for _ in range(1321):
        num = randint(0, 10000)
        result += 'a' * num
        cluster.execute('append', 'x', 'a' * num)

    # Verify the value against the calculated result
    val = cluster.execute('get', 'x')
    assert result == val.decode('utf-8')

    # Verify the value after a restart
    cluster.restart()
    cluster.wait_for_unanimity()

    val = cluster.execute('get', 'x')
    assert result == val.decode('utf-8')


def test_log_rollback(cluster):
    """
    Rollback of log entries that were written in the minority.
    """

    cluster.create(3)
    assert cluster.leader == 1
    assert cluster.execute('INCRBY', 'key', '111') == 111

    # Break cluster
    cluster.node(2).terminate()
    cluster.node(3).terminate()

    # Load a command which can't be committed
    assert cluster.node(1).current_index() == 7
    conn = cluster.node(1).client.connection_pool.get_connection('deferred')
    conn.send_command('INCRBY', 'key', '222')
    assert cluster.node(1).current_index() == 8
    cluster.node(1).terminate()

    # We want to be sure the last entry is in the log
    log = RaftLog(cluster.node(1).raftlog)
    log.read()
    assert log.entry_count() == 8

    # Restart the cluster without node 1, make sure the write was
    # not committed.
    cluster.node(2).start()
    cluster.node(3).start()
    cluster.node(2).wait_for_election()
    assert cluster.node(2).current_index() == 8  # 7 + 1 no-op entry

    # Restart node 1
    cluster.node(1).start()
    cluster.node(1).wait_for_election()

    # Make another write and make sure it overwrites the previous one in
    # node 1's log
    assert cluster.execute('INCRBY', 'key', '333') == 444
    cluster.wait_for_unanimity()

    # Make sure log reflects the change
    log.reset()
    log.read()
    assert match(r'.*INCRBY.*333', str(log.entries[-1].data()))

    # make sure after double resume, crc chain has been maintained
    # before second resume get # entries
    node1_log_size = cluster.node(1).info()['raft_log_entries']

    # kill all nodes (1 first, as we don't want any additional log entries)
    cluster.node(1).terminate()
    cluster.node(2).terminate()
    cluster.node(3).terminate()

    # restart 1, won't have an active cluster, but should have loaded log
    cluster.node(1).start()
    cluster.node(1).wait_for_info_param('raft_state', 'up')
    assert node1_log_size == cluster.node(1).info()['raft_log_entries']


def test_log_rollback_entire_log(cluster):
    """
    Rollback of log entries that were written in the minority.
    """

    cluster.create(3)
    assert cluster.leader == 1
    assert cluster.execute('INCRBY', 'key', '111') == 111

    # Break cluster
    cluster.node(2).terminate()
    cluster.node(3).terminate()

    # Load a command which can't be committed
    assert cluster.node(1).current_index() == 7
    cluster.node(1).client.execute_command('raft.debug', 'compact')
    conn = cluster.node(1).client.connection_pool.get_connection('deferred')
    conn.send_command('INCRBY', 'key', '222')
    assert cluster.node(1).current_index() == 8
    assert cluster.node(1).info()['raft_log_entries'] == 1
    cluster.node(1).terminate()

    # We want to be sure the last entry is in the log
    log = RaftLog(cluster.node(1).raftlog)
    log.read()
    assert log.entry_count() == 1

    # Restart the cluster without node 1, make sure the write was
    # not committed.
    cluster.node(2).start()
    cluster.node(3).start()
    cluster.node(2).wait_for_election()
    assert cluster.node(2).current_index() == 8  # 7 + 1 no-op entry

    # Restart node 1
    cluster.node(1).start()
    cluster.node(1).wait_for_election()

    # Make another write and make sure it overwrites the previous one in
    # node 1's log
    assert cluster.execute('INCRBY', 'key', '333') == 444
    cluster.wait_for_unanimity()

    # Make sure log reflects the change
    log.reset()
    log.read()
    assert match(r'.*INCRBY.*333', str(log.entries[-1].data()))

    # make sure after double resume, crc chain has been maintained
    # before second resume get # entries
    node1_log_size = cluster.node(1).info()['raft_log_entries']

    # kill all nodes (1 first, as we don't want any additional log entries)
    cluster.node(1).terminate()
    cluster.node(2).terminate()
    cluster.node(3).terminate()

    # restart 1, won't have an active cluster, but should have loaded log
    cluster.node(1).start()
    cluster.node(1).wait_for_info_param('raft_state', 'up')
    assert node1_log_size == cluster.node(1).info()['raft_log_entries']


def test_raft_log_max_file_size(cluster):
    """
    Raft log size configuration affects compaction.
    """

    r1 = cluster.add_node()
    assert r1.info()['raft_log_entries'] == 2
    assert r1.config_set('raft.log-max-file-size', '1kb')
    for _ in range(10):
        assert r1.client.set('testkey', 'x' * 500)

    r1.wait_for_info_param('raft_snapshots_created', 1)
    assert r1.info()['raft_log_entries'] < r1.info()['raft_current_index']


def test_raft_log_max_cache_size(cluster):
    """
    Raft log cache configuration in effect.
    """

    r1 = cluster.add_node()
    assert r1.info()['raft_cache_entries'] == 2

    assert r1.config_set('raft.log-max-cache-size', '1kb')
    assert r1.client.set('testkey', 'testvalue')

    info = r1.info()
    assert info['raft_cache_entries'] == 3
    assert info['raft_cache_memory_size'] > 0

    for _ in range(10):
        assert r1.client.set('testkey', 'x' * 500)

    time.sleep(1)
    info = r1.info()
    assert info['raft_log_entries'] == 13
    assert info['raft_cache_entries'] < 6


def test_reply_to_cache_invalidated_entry(cluster):
    """
    Reply a RAFT redis command that have its entry already removed
    from the cache.
    """

    cluster.create(3)
    assert cluster.leader == 1

    # Configure a small cache
    assert cluster.node(1).config_set('raft.log-max-cache-size', '1kb')

    # Break cluster to avoid commits
    cluster.node(2).terminate()
    cluster.node(3).terminate()

    # Send commands that are guaranteed to overflow the cache
    conns = []
    for i in range(10):
        n1 = cluster.node(1)
        conn = n1.client.connection_pool.get_connection('deferred')

        conn.send_command('SET', 'key%s' % i, 'x' * 1000)
        conns.append(conn)

    # give periodic job time to handle cache
    time.sleep(0.5)

    # confirm all raft entries were created but some have been evicted
    # from cache already.
    info = cluster.node(1).info()
    assert info['raft_log_entries'] == 16
    assert info['raft_cache_entries'] < 11

    # Repair cluster and wait
    cluster.node(2).start()
    cluster.node(3).start()
    cluster.node(1).wait_for_num_voting_nodes(3)
    time.sleep(1)
    assert cluster.node(1).commit_index() == 16

    # Expect TIMEOUT or OK for all
    for conn in conns:
        assert conn.can_read(timeout=1)
        try:
            assert conn.read_response() == b'OK'
        except ResponseError as err:
            assert str(err).startswith('TIMEOUT')


def test_read_before_commits(cluster):
    """
    """

    cluster.create(3)
    assert cluster.execute('get', 'somekey') is None


def test_stale_reads_on_leader_election(cluster):
    """
    """
    cluster.create(3)

    # Try 10 times
    for _ in range(10):
        val_written = cluster.execute('INCR', 'counter-1')

        leader = cluster.node(cluster.leader)
        leader.terminate()
        leader.start(verify=False)

        val_read = cluster.execute('GET', 'counter-1')
        assert val_read is not None
        assert val_written == int(val_read)
        time.sleep(1)


def test_log_partial_entry(cluster):
    """
    Truncate log file and verify partial entry is discarded.
    """
    cluster.create(1)

    cluster.execute('set', 'x', 1)
    cluster.execute('set', 'x', 2)
    cluster.execute('set', 'x', 3)

    assert cluster.node(1).info()['raft_log_entries'] == 5
    cluster.node(1).kill()

    filename = cluster.node(1).raftlog
    os.open(filename, os.O_RDWR)

    # ----------------------------------------------------
    # Case-1 Truncate 1 byte
    os.truncate(filename, os.path.getsize(filename) - 1)

    cluster.node(1).start()
    cluster.node(1).wait_for_election()

    # There will be 4 valid entries + 1 NOOP entry = 5
    assert cluster.node(1).info()['raft_log_entries'] == 5
    assert cluster.execute('get', 'x') == b'2'

    # ----------------------------------------------------
    # Case-2 Truncate an entry larger than page size (4096)
    cluster.execute('set', 'x', 'a' * 5000)
    cluster.execute('set', 'x', 'b' * 5000)
    assert cluster.node(1).info()['raft_log_entries'] == 7
    cluster.node(1).kill()

    os.truncate(filename, os.path.getsize(filename) - 4500)
    cluster.node(1).start()
    cluster.node(1).wait_for_election()
    # There will be 6 valid entries + 1 NOOP entry = 7
    assert cluster.node(1).info()['raft_log_entries'] == 7
    assert cluster.execute('get', 'x') == ('a' * 5000).encode('utf-8')

    # ----------------------------------------------------
    # Case-3 Truncate two entries
    cluster.execute('set', 'x', 'a' * 15000)
    cluster.execute('set', 'x', 'b' * 15000)
    cluster.execute('set', 'x', 'c' * 5000)
    cluster.execute('set', 'x', 'd' * 5000)
    assert cluster.node(1).info()['raft_log_entries'] == 11
    cluster.node(1).kill()

    os.truncate(filename, os.path.getsize(filename) - 7000)
    cluster.node(1).start()
    cluster.node(1).wait_for_election()
    # There will be 9 valid entries + 1 NOOP entry = 10
    assert cluster.node(1).info()['raft_log_entries'] == 10
    assert cluster.execute('get', 'x') == ('b' * 15000).encode('utf-8')


def corrupt_byte_location(filename, location):
    file = open(filename, "r+b")
    file.seek(location)
    byte = file.read(1)
    file.seek(location)
    if byte == b"1":
        file.write(b"2")
    else:
        file.write(b"1")
    file.close()


def test_log_corrupt_header_dbid(cluster):
    cluster.create(1)

    cluster.execute('set', 'x', 1)
    cluster.execute('set', 'x', 2)
    cluster.execute('set', 'x', 3)
    cluster.execute('set', 'x', 4)
    cluster.execute('set', 'x', 5)
    cluster.execute('set', 'x', 6)

    n1 = cluster.node(1)

    assert n1.info()['raft_log_entries'] == 8
    n1.kill()

    log = RaftLog(n1.raftlog)
    log.read()
    assert log.entry_count() == 8
    assert isinstance(log.entries[0], LogHeader)

    corrupt_byte_location(n1.raftlog, log.header().dbid_location())

    n1.start()
    assert n1.info()["raft_state"] == "uninitialized"


def test_log_corrupt_header_crc(cluster):
    cluster.create(1)

    cluster.execute('set', 'x', 1)
    cluster.execute('set', 'x', 2)
    cluster.execute('set', 'x', 3)
    cluster.execute('set', 'x', 4)
    cluster.execute('set', 'x', 5)
    cluster.execute('set', 'x', 6)

    n1 = cluster.node(1)

    assert n1.info()['raft_log_entries'] == 8
    n1.kill()

    log = RaftLog(cluster.node(1).raftlog)
    log.read()
    assert log.entry_count() == 8
    assert isinstance(log.entries[0], LogHeader)

    corrupt_byte_location(n1.raftlog, log.header().crc_location())

    n1.start()
    assert n1.info()["raft_state"] == "uninitialized"


def test_log_corrupt_last_entry_data(cluster):
    cluster.create(1)

    cluster.execute('set', 'x', 1)
    cluster.execute('set', 'x', 2)
    cluster.execute('set', 'x', 3)
    cluster.execute('set', 'x', 4)
    cluster.execute('set', 'x', 5)
    cluster.execute('set', 'x', 6)

    n1 = cluster.node(1)

    assert n1.info()['raft_log_entries'] == 8
    n1.kill()

    log = RaftLog(cluster.node(1).raftlog)
    log.read()
    assert log.entry_count() == 8
    assert isinstance(log.entries[8], LogEntry)

    corrupt_byte_location(n1.raftlog, log.entries[8].data_location())
    n1.start()

    assert cluster.execute('get', 'x') == b'5'
    assert n1.info()['raft_log_entries'] == 8


def test_log_corrupt_last_entry_crc(cluster):
    cluster.create(1)

    cluster.execute('set', 'x', 1)
    cluster.execute('set', 'x', 2)
    cluster.execute('set', 'x', 3)
    cluster.execute('set', 'x', 4)
    cluster.execute('set', 'x', 5)
    cluster.execute('set', 'x', 6)

    n1 = cluster.node(1)

    assert n1.info()['raft_log_entries'] == 8
    n1.kill()

    log = RaftLog(n1.raftlog)
    log.read()
    assert log.entry_count() == 8
    assert isinstance(log.entries[8], LogEntry)

    corrupt_byte_location(n1.raftlog, log.entries[8].crc_location())
    n1.start()

    assert cluster.execute('get', 'x') == b'5'
    assert n1.info()['raft_log_entries'] == 8


def test_log_corrupt_middle_entry_data(cluster):
    cluster.create(1)

    cluster.execute('set', 'x', 1)
    cluster.execute('set', 'x', 2)
    cluster.execute('set', 'x', 3)
    cluster.execute('set', 'x', 4)
    cluster.execute('set', 'x', 5)
    cluster.execute('set', 'x', 6)

    n1 = cluster.node(1)
    assert n1.info()['raft_log_entries'] == 8
    n1.kill()

    log = RaftLog(n1.raftlog)
    log.read()
    assert log.entry_count() == 8
    assert isinstance(log.entries[5], LogEntry)

    corrupt_byte_location(n1.raftlog, log.entries[5].data_location())
    n1.start()

    assert cluster.execute('get', 'x') == b'2'
    assert n1.info()['raft_log_entries'] == 5


def test_log_corrupt_middle_entry_crc(cluster):
    cluster.create(1)

    cluster.execute('set', 'x', 1)
    cluster.execute('set', 'x', 2)
    cluster.execute('set', 'x', 3)
    cluster.execute('set', 'x', 4)
    cluster.execute('set', 'x', 5)
    cluster.execute('set', 'x', 6)

    n1 = cluster.node(1)
    assert n1.info()['raft_log_entries'] == 8
    n1.kill()

    log = RaftLog(n1.raftlog)
    log.read()
    assert log.entry_count() == 8
    assert isinstance(log.entries[5], LogEntry)

    corrupt_byte_location(n1.raftlog, log.entries[5].crc_location())

    cluster.node(1).start()
    assert cluster.execute('get', 'x') == b'2'
    assert cluster.node(1).info()['raft_log_entries'] == 5


def test_startup_with_multiple_log_files(cluster):
    """
    Initiate log compaction and kill the server before compaction is completed.
    Node will start with multiple log pages.
    """
    cluster.create(1)

    cluster.execute('set', 'x', 1)

    n1 = cluster.node(1)

    # Disable snapshot to prevent another snapshot attempt after the failure.
    n1.config_set('raft.snapshot-disable', 'yes')

    # 'compact' will fail, but it will create the second log file.
    n1.config_set("raft.snapshot-fail", "yes")
    with raises(ResponseError):
        n1.execute('raft.debug', 'compact')

    assert n1.info()['raft_snapshots_created'] == 0

    n1.execute('incr', 'x')
    n1.execute('incr', 'x')
    n1.kill()
    n1.start()

    n1.wait_for_election()
    assert n1.execute('get', 'x') == b'3'

    n1.wait_for_info_param('raft_snapshots_created', 1)
    assert n1.execute('get', 'x') == b'3'


def test_startup_with_empty_log_file(cluster):
    """
    Initiate log compaction and kill the server before compaction is completed.
    Node will start with multiple log pages, second page will be empty and,
    it should be deleted on startup.
    """
    cluster.create(1)

    cluster.execute('set', 'x', 1)
    n1 = cluster.node(1)

    # Disable snapshot to prevent another snapshot attempt after the failure.
    n1.config_set('raft.snapshot-disable', 'yes')

    # 'compact' will fail, but it will create the second log file.
    n1.config_set("raft.snapshot-fail", "yes")
    with raises(ResponseError):
        n1.execute('raft.debug', 'compact')

    assert n1.info()['raft_snapshots_created'] == 0

    n1.kill()
    n1.start()

    n1.wait_for_election()
    assert n1.execute('get', 'x') == b'1'

    # Wait a bit just in case a late snapshot starts (it shouldn't)
    time.sleep(3)
    assert n1.info()['raft_snapshots_created'] == 0


def test_startup_with_more_advanced_snapshot(cluster):
    """
    Test correctness when the node starts with a more advanced snapshot than
    the logs. It can happen when a follower receives a snapshot and crashes
    just after replacing the snapshot and before truncating the logs.
    """
    cluster.create(3)

    cluster.execute('set', 'x', 1)
    cluster.node(3).kill()

    cluster.execute('incr', 'x')
    cluster.execute('incr', 'x')

    assert cluster.node(1).execute('raft.debug', 'compact') == b'OK'
    assert cluster.node(2).execute('raft.debug', 'compact') == b'OK'

    cluster.node(1).kill()
    cluster.node(2).kill()

    n3 = cluster.node(3)
    n3.start()
    n3.wait_for_info_param('raft_state', 'up')

    # Make n3 have multiple log files
    n3.config_set('raft.snapshot-disable', 'yes')
    assert n3.execute('raft.debug', 'compact', 1) == b'OK'

    # Start other nodes, n3 will receive the snapshot but won't load.
    n3.config_set('raft.snapshot-disable-load', 'yes')
    cluster.node(1).start()
    cluster.node(2).start()

    n3.wait_for_info_param('raft_state', 'loading')
    n3.kill()
    cluster.node(1).kill()
    cluster.node(2).kill()

    # n3 will start with multiple log files and a more advanced snapshot.
    n3.start()
    n3.wait_for_info_param('raft_state', 'up')
    assert n3.raft_debug_exec('get', 'x') == b'3'

    cluster.node(1).start()
    cluster.node(2).start()
    cluster.wait_for_unanimity()

    assert cluster.execute('get', 'x') == b'3'
    cluster.execute('incr', 'x')
    assert cluster.execute('get', 'x') == b'4'
    assert n3.info()['raft_current_index'] == 12
