"""
This file is part of RedisRaft.

Copyright (c) 2020 Redis Labs

RedisRaft is dual licensed under the GNU Affero General Public License version 3
(AGPLv3) or the Redis Source Available License (RSAL).
"""

import time
from re import match
from redis import ResponseError
from .raftlog import RaftLog
import pytest

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
    assert cluster.node(1).current_index() == 6
    conn = cluster.node(1).client.connection_pool.get_connection('deferred')
    conn.send_command('INCRBY', 'key', '222')
    assert cluster.node(1).current_index() == 7
    cluster.node(1).terminate()

    # We want to be sure the last entry is in the log
    log = RaftLog(cluster.node(1).raftlog)
    log.read()
    assert log.entry_count() == 7

    # Restart the cluster without node 1, make sure the write was
    # not committed.
    cluster.node(2).start()
    cluster.node(3).start()
    cluster.node(2).wait_for_election()
    assert cluster.node(2).current_index() == 7 # 6 + 1 no-op entry

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


def test_raft_log_max_file_size(cluster):
    """
    Raft log size configuration affects compaction.
    """

    r1 = cluster.add_node()
    assert r1.raft_info()['log_entries'] == 1
    assert r1.raft_config_set('raft-log-max-file-size', '1kb')
    for _ in range(10):
        assert r1.client.set('testkey', 'x'*500)
    time.sleep(1)
    assert r1.raft_info()['log_entries'] < 10


def test_raft_log_max_cache_size(cluster):
    """
    Raft log cache configuration in effect.
    """

    r1 = cluster.add_node()
    assert r1.raft_info()['cache_entries'] == 1

    assert r1.raft_config_set('raft-log-max-cache-size', '1kb')
    assert r1.client.set('testkey', 'testvalue')

    info = r1.raft_info()
    assert info['cache_entries'] == 2
    assert info['cache_memory_size'] > 0

    for _ in range(10):
        assert r1.client.set('testkey', 'x' * 500)

    time.sleep(1)
    info = r1.raft_info()
    assert info['log_entries'] == 12
    assert info['cache_entries'] < 5


def test_reply_to_cache_invalidated_entry(cluster):
    """
    Reply a RAFT redis command that have its entry already removed
    from the cache.
    """

    cluster.create(3)
    assert cluster.leader == 1

    # Configure a small cache
    assert cluster.node(1).raft_config_set('raft-log-max-cache-size', '1kb')

    # Break cluster to avoid commits
    cluster.node(2).terminate()
    cluster.node(3).terminate()

    # Send commands that are guarnateed to overflow the cache
    conns = []
    for i in range(10):
        conn = cluster.node(1).client.connection_pool.get_connection('deferred')
        conn.send_command('SET', 'key%s' % i, 'x' * 1000)
        conns.append(conn)

    # give periodic job time to handle cache
    time.sleep(0.5)

    # confirm all raft entries were created but some have been evicted
    # from cache already.
    info = cluster.node(1).raft_info()
    assert info['log_entries'] == 15
    assert info['cache_entries'] < 10

    # Repair cluster and wait
    cluster.node(2).start()
    cluster.node(3).start()
    cluster.node(1).wait_for_num_voting_nodes(3)
    time.sleep(1)
    assert cluster.node(1).commit_index() == 15

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


@pytest.mark.slow
def test_ae_limit(cluster):
    """
    Test handling delivery of a very large appendentries message,
    if a follower joins after missing many log entries.
    """

    cluster.create(3)
    cluster.node(3).terminate()

    pipeline = cluster.node(1).client.pipeline(transaction=False)
    for _ in range(300000):
        pipeline.incr('mykey')
    pipeline.execute()

    cluster.node(3).start()
    cluster.wait_for_unanimity()
