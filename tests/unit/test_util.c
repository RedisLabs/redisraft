/*
 * Copyright Redis Ltd. 2020 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "test.h"

#include "../src/redisraft.h"

#include <assert.h>
#include <stddef.h>

static void test_raftreq_str()
{
    for (int i = 1; i < RR_RAFTREQ_MAX; i++) {
        assert(RaftReqTypeStr[i] != NULL);
    }
}

static void test_parse_slots()
{
    char slots[REDIS_RAFT_HASH_SLOTS] = {0};
    parseHashSlots(slots, "0");
    assert(slots[0] == 1);
    for (size_t i = 1; i < REDIS_RAFT_HASH_SLOTS; i++) {
        assert(slots[i] == 0);
    }
    memset(slots, 0, sizeof(slots));
    parseHashSlots(slots, "0-10");
    for (size_t i = 0; i < 11; i++) {
        assert(slots[i] == 1);
    }
    for (size_t i = 11; i < REDIS_RAFT_HASH_SLOTS; i++) {
        assert(slots[i] == 0);
    }
    memset(slots, 0, sizeof(slots));
    parseHashSlots(slots, "0,5-10,16,58-62,100");
    assert(slots[0] == 1);
    for (size_t i = 1; i < 5; i++) {
        assert(slots[i] == 0);
    }
    for (size_t i = 5; i < 11; i++) {
        assert(slots[i] == 1);
    }
    for (size_t i = 11; i < 16; i++) {
        assert(slots[i] == 0);
    }
    assert(slots[16] == 1);
    for (size_t i = 17; i < 58; i++) {
        assert(slots[i] == 0);
    }
    for (size_t i = 58; i < 63; i++) {
        assert(slots[i] == 1);
    }
    for (size_t i = 63; i < 100; i++) {
        assert(slots[i] == 0);
    }
    assert(slots[100] == 1);
    for (size_t i = 101; i < REDIS_RAFT_HASH_SLOTS; i++) {
        assert(slots[i] == 0);
    }
}

void test_util()
{
    test_run(test_raftreq_str);
    test_run(test_parse_slots);
}
