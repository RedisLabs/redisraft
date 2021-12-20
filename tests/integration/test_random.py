"""
This file is part of RedisRaft.

Copyright (c) 2021 Redis Ltd.

RedisRaft is licensed under the Redis Source Available License (RSAL).
"""
import random
import string

import time
import logging

def test_hash_deterministic_order(cluster):
    """
    Make sure hash keys maintain a deterministic order. We use Lua to
    create hash fields and set their values depending on the order of
    keys.

    Hash keys that exceed the 'hash-max-*' configuration settings
    will be created as unordered hash tables and we want to confirm they
    maintain the same order across the cluster.
    """

    cluster.create(3)
    cluster.config_set('lua-replicate-commands', 'no')

    script = cluster.node(1).client.register_script("""
-- Populate hash KEYS[1] with ARGV[1] fields, then assign each field
-- a value corresponding to its index in HKEYS
local key = KEYS[1]
local i = tonumber(ARGV[1])
while (i > 0) do
    redis.call('HSET', key, 'field:' .. i, 'noval')
    i = i - 1
end

-- Iterate fields and set values according to order
local hash_fields = redis.call('HKEYS', 'myhash')
for i, field in ipairs(hash_fields) do
    redis.call('HSET', key, field, i)
end

return 1
""")
    assert script(keys=['myhash'], args=[1000]) == 1

    cluster.wait_for_unanimity()

    def to_dict(reply):
        return dict(zip(i := iter(reply), i))

    myhash = to_dict(cluster.node(1).raft_debug_exec('hgetall', 'myhash'))
    assert to_dict(cluster.node(2).raft_debug_exec('hgetall', 'myhash')) == myhash
    assert to_dict(cluster.node(3).raft_debug_exec('hgetall', 'myhash')) == myhash


def test_raft_sort_hashes(cluster):
    cluster.create(3)
    for i in range(1000):
        key = ''.join(random.choices(string.ascii_uppercase, k=6))
        val = ''.join(random.choices(string.ascii_uppercase, k=6))
        cluster.execute("hset", "test", key, val)
        cluster.execute("hset", "test", key + "1", val)

    hgetall = cluster.execute("raft.sort", "hgetall", "test")
    hkeys = cluster.execute("raft.sort", "hkeys", "test")
    hvals = cluster.execute("raft.sort", "hvals", "test")

    assert len(hgetall) == 4000
    assert len(hkeys) == 2000
    assert len(hvals) == 2000

    old_val = ""
    for i in range(0, len(hgetall), 2):
        k = hgetall[i]
        if old_val == "":
            old_val = k
            continue
        assert k >= old_val
        old_val = k

    old_val = ""
    for i in range(0, len(hkeys)):
        k = hkeys[i]
        if old_val == "":
            old_val = k
            continue
        assert k >= old_val
        old_val = k

    old_val = ""
    for i in range(0, len(hvals)):
        k = hvals[i]
        if old_val == "":
            old_val = k
            continue
        assert k >= old_val
        old_val = k


def test_raft_sort_sets(cluster):
    cluster.create(3)
    for i in range(1000):
        val = ''.join(random.choices(string.ascii_uppercase, k=6))
        cluster.execute("sadd", "test", val)
        if i % 2 == 0:
            cluster.execute("sadd", "test1", val)

    sinter = cluster.execute("raft.sort", "sinter", "test", "test1")
    sunion = cluster.execute("raft.sort", "sunion", "test", "test1")
    sdiff = cluster.execute("raft.sort", "sdiff", "test", "test1")
    smembers = cluster.execute("raft.sort", "smembers", "test")

    assert len(sinter) == 500
    assert len(sunion) == 1000
    assert len(sdiff) == 500
    assert len(smembers) == 1000

    old_val = ""
    for i in range(0, len(sinter)):
        k = sinter[i]
        if old_val == "":
            old_val = k
            continue
        assert k >= old_val
        old_val = k

    old_val = ""
    for i in range(0, len(sunion)):
        k = sunion[i]
        if old_val == "":
            old_val = k
            continue
        assert k >= old_val
        old_val = k

    old_val = ""
    for i in range(0, len(sdiff)):
        k = sdiff[i]
        if old_val == "":
            old_val = k
            continue
        assert k >= old_val
        old_val = k

    old_val = ""
    for i in range(0, len(smembers)):
        k = smembers[i]
        if old_val == "":
            old_val = k
            continue
        assert k >= old_val
        old_val = k


def test_raft_sort_keys(cluster):
    cluster.create(3)
    for i in range(1000):
        key = ''.join(random.choices(string.ascii_uppercase, k=6))
        val = ''.join(random.choices(string.ascii_uppercase, k=6))
        cluster.execute("set", key, val)

    keys = cluster.execute("raft.sort", "keys", "*")

    assert len(keys) == 1000

    old_val = ""
    for i in range(0, len(keys)):
        k = keys[i]
        if old_val == "":
            old_val = k
            continue
        assert k >= old_val
        old_val = k
