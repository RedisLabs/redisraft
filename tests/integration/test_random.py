"""
This file is part of RedisRaft.

Copyright (c) 2021 Redis Ltd.

RedisRaft is licensed under the Redis Source Available License (RSAL).
"""

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
    #cluster.config_set('lua-replicate-commands', 'no')

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

    myhash = to_dict(cluster.node(1).raft_debug_exec('raft.sort hgetall', 'myhash'))
    assert to_dict(cluster.node(2).raft_debug_exec('raft.sort hgetall', 'myhash')) == myhash
    assert to_dict(cluster.node(3).raft_debug_exec('raft.sort hgetall', 'myhash')) == myhash
