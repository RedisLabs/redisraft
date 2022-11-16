"""
Copyright Redis Ltd. 2020 - present
Licensed under your choice of the Redis Source Available License 2.0 (RSALv2)
or the Server Side Public License v1 (SSPLv1).
"""

import random


def test_hash_deterministic_order(cluster):
    """
    Make sure hash keys maintain a deterministic order. We use Lua to
    create hash fields and then add them to a list in their
    enumerated order.

    Hash keys that exceed the 'hash-max-*' configuration settings
    will be created as unordered hash tables, and we want to confirm they
    maintain the same order across the cluster.
    """

    cluster.create(3)

    script = cluster.node(1).client.register_script("""
-- Populate hash KEYS[1] with ARGV[1] fields, then assign each field
-- a value corresponding to its index in result of cmd
local key1 = "KEYS[1]"
local output_key = KEYS[2]
local i = tonumber(ARGV[1])
local cmd = ARGV[2]

redis.call("del", key1)

-- if we wanted to be able to pass a random seed in
local seed = 123456
math.randomseed(seed)

-- generate a random string of a given length
-- 1. generate the set of chars to string to be generated frm
-- qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM1234567890
local charset = {}
for i = 48,  57 do table.insert(charset, string.char(i)) end
for i = 65,  90 do table.insert(charset, string.char(i)) end
for i = 97, 122 do table.insert(charset, string.char(i)) end

-- 2. recursive function to generate random string of length
local function randomstr(length)
  if length > 0 then
    return randomstr(length - 1) .. charset[math.random(1, #charset)]
  else
    return ""
  end
end

while (i > 0) do
    redis.call('HSET', key1, 'field:' .. i, randomstr(6))
    i = i - 1
end

-- Iterate fields and set values according to order
local vals = redis.call(cmd, key1)
for i, field in ipairs(vals) do
    if (not (cmd == "hgetall") or i % 2 == 0) then
        redis.call('rpush', output_key, field)
    end
end

return 1
""")
    cmd = "hkeys"
    assert script(keys=["test", cmd], args=[1000, cmd]) == 1

    cluster.wait_for_unanimity()

    mylist = cluster.node(1).raft_debug_exec('lrange', cmd, 0, -1)
    assert len(mylist) == 1000
    assert cluster.node(2).raft_debug_exec('lrange', cmd, 0, -1) == mylist
    assert cluster.node(3).raft_debug_exec('lrange', cmd, 0, -1) == mylist

    cmd = "hvals"
    assert script(keys=["test", cmd], args=[1000, cmd]) == 1

    cluster.wait_for_unanimity()

    mylist = cluster.node(1).raft_debug_exec('lrange', cmd, 0, -1)
    assert len(mylist) == 1000
    assert cluster.node(2).raft_debug_exec('lrange', cmd, 0, -1) == mylist
    assert cluster.node(3).raft_debug_exec('lrange', cmd, 0, -1) == mylist

    cmd = "hgetall"
    assert script(keys=["test", cmd], args=[1000, cmd]) == 1

    cluster.wait_for_unanimity()

    mylist = cluster.node(1).raft_debug_exec('lrange', cmd, 0, -1)
    assert len(mylist) == 1000
    assert cluster.node(2).raft_debug_exec('lrange', cmd, 0, -1) == mylist
    assert cluster.node(3).raft_debug_exec('lrange', cmd, 0, -1) == mylist


def test_set_deterministic_order(cluster):
    """
    Make sure sets maintain a deterministic order. We use Lua to
    add eleemnts to sets and then add them to a list in their
    enumerated order.
    """

    cluster.create(3)

    script = cluster.node(1).client.register_script("""
-- Populate hash KEYS[1] with ARGV[1] fields, then assign each field
-- a value corresponding to its index in result of cmd
local key1 = KEYS[1]
local key2 = KEYS[2]
local output_key = KEYS[3]
local i = tonumber(ARGV[1])
local cmd = ARGV[2]

redis.call("del", key1, key2)

-- if we wanted to be able to pass a random seed in
local seed = 123456
math.randomseed(seed)

-- generate a random string of a given length
-- 1. generate the set of chars to string to be generated frm
-- qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM1234567890
local charset = {}
for i = 48,  57 do table.insert(charset, string.char(i)) end
for i = 65,  90 do table.insert(charset, string.char(i)) end
for i = 97, 122 do table.insert(charset, string.char(i)) end

-- 2. recursive function to generate random string of length
local function randomstr(length)
  if length > 0 then
    return randomstr(length - 1) .. charset[math.random(1, #charset)]
  else
    return ""
  end
end

while (i > 0) do
    local str = randomstr(6)
    redis.call('sadd', key1, str)
    if i % 2 == 0 then
        redis.call('sadd', key2, str)
    end
    i = i - 1
end

-- Iterate fields and set values according to order
local vals
if not (cmd == "smembers") then
    vals = redis.call(cmd, key1, key2)
else
    vals = redis.call(cmd, key1)
end
for i, field in ipairs(vals) do
    redis.call('rpush', output_key, field)
end

return 1
""")
    cmd = "sinter"
    assert script(keys=["key1", "key2", cmd], args=[1000, cmd]) == 1

    cluster.wait_for_unanimity()

    mylist = cluster.node(1).raft_debug_exec('lrange', cmd, 0, -1)
    assert len(mylist) == 500
    assert cluster.node(2).raft_debug_exec('lrange', cmd, 0, -1) == mylist
    assert cluster.node(3).raft_debug_exec('lrange', cmd, 0, -1) == mylist

    cmd = "sunion"
    assert script(keys=["key1", "key2", cmd], args=[1000, cmd]) == 1

    cluster.wait_for_unanimity()

    mylist = cluster.node(1).raft_debug_exec('lrange', cmd, 0, -1)
    assert len(mylist) == 1000
    assert cluster.node(2).raft_debug_exec('lrange', cmd, 0, -1) == mylist
    assert cluster.node(3).raft_debug_exec('lrange', cmd, 0, -1) == mylist

    cmd = "sdiff"
    assert script(keys=["key1", "key2", cmd], args=[1000, cmd]) == 1

    cluster.wait_for_unanimity()

    mylist = cluster.node(1).raft_debug_exec('lrange', cmd, 0, -1)
    assert len(mylist) == 500
    assert cluster.node(2).raft_debug_exec('lrange', cmd, 0, -1) == mylist
    assert cluster.node(3).raft_debug_exec('lrange', cmd, 0, -1) == mylist

    cmd = "smembers"
    assert script(keys=["key1", "key2", cmd], args=[1000, cmd]) == 1

    cluster.wait_for_unanimity()

    mylist = cluster.node(1).raft_debug_exec('lrange', cmd, 0, -1)
    assert len(mylist) == 1000
    assert cluster.node(2).raft_debug_exec('lrange', cmd, 0, -1) == mylist
    assert cluster.node(3).raft_debug_exec('lrange', cmd, 0, -1) == mylist


def test_keys_deterministic_order(cluster):
    """
    Make sure redis keys are listed in a deterministic order.
    We use Lua to create keys and add them to a list in their
    enumerated order.
    """

    cluster.create(3)

    script = cluster.node(1).client.register_script("""
-- Populate hash KEYS[1] with ARGV[1] fields, then assign each field
-- a value corresponding to its index in result of cmd
local output_key = KEYS[1]
local i = tonumber(ARGV[1])

local charset = {}
-- qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM1234567890
for i = 48,  57 do table.insert(charset, string.char(i)) end
for i = 65,  90 do table.insert(charset, string.char(i)) end
for i = 97, 122 do table.insert(charset, string.char(i)) end

math.randomseed(123456)

local function randomstr(length)
  if length > 0 then
    return randomstr(length - 1) .. charset[math.random(1, #charset)]
  else
    return ""
  end
end

while (i > 0) do
    local str = randomstr(6)
    redis.call('set', str, 123)
    i = i - 1
end

-- Iterate fields and set values according to order
local vals = redis.call("keys", "*")
for i, field in ipairs(vals) do
    redis.call('rpush', output_key, field)
end

return 1
""")
    cmd = "keys"
    assert script(keys=[cmd], args=[1000]) == 1

    cluster.wait_for_unanimity()

    mylist = cluster.node(1).raft_debug_exec('lrange', cmd, 0, -1)
    assert len(mylist) == 1000
    assert cluster.node(2).raft_debug_exec('lrange', cmd, 0, -1) == mylist
    assert cluster.node(3).raft_debug_exec('lrange', cmd, 0, -1) == mylist


def test_raft_sort_hashes(cluster):
    cluster.create(3)

    nums = random.sample(range(1, 10000), 1000)

    for i in range(0, len(nums)):
        cluster.execute("hset", "test", nums[i], i)
        cluster.execute("hset", "test", nums[i] + 20000, i)

    hgetall = cluster.execute("raft._sort_reply", "hgetall", "test")
    hkeys = cluster.execute("raft._sort_reply", "hkeys", "test")
    hvals = cluster.execute("raft._sort_reply", "hvals", "test")

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

    nums = random.sample(range(1, 10000), 1000)

    for i in range(0, len(nums)):
        cluster.execute("sadd", "test", nums[i])
        if i % 2 == 0:
            cluster.execute("sadd", "test1", nums[i])

    sinter = cluster.execute("raft._sort_reply", "sinter", "test", "test1")
    sunion = cluster.execute("raft._sort_reply", "sunion", "test", "test1")
    sdiff = cluster.execute("raft._sort_reply", "sdiff", "test", "test1")
    smembers1 = cluster.execute("raft._sort_reply", "smembers", "test")
    smembers2 = cluster.execute("raft._sort_reply", "smembers", "test1")

    assert len(smembers1) == 1000
    assert len(smembers2) == 500
    assert len(sunion) == 1000
    assert len(sinter) == 500
    assert len(sdiff) == 500

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
    for i in range(0, len(smembers1)):
        k = smembers1[i]
        if old_val == "":
            old_val = k
            continue
        assert k >= old_val
        old_val = k


def test_raft_sort_keys(cluster):
    cluster.create(3)

    nums = random.sample(range(1, 10000), 1000)

    for i in range(0, len(nums)):
        cluster.execute("set", nums[i], i)

    keys = cluster.execute("raft._sort_reply", "keys", "*")

    assert len(keys) == 1000

    old_val = ""
    for i in range(0, len(keys)):
        k = keys[i]
        if old_val == "":
            old_val = k
            continue
        assert k >= old_val
        old_val = k
