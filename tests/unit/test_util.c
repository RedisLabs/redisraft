/*
 * Copyright Redis Ltd. 2020 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "../src/redisraft.h"

#include <stddef.h>
#include <strings.h>

#include "cmocka.h"

static void test_raftreq_str(void **state)
{
    for (int i = 1; i < RR_RAFTREQ_MAX; i++) {
        assert_ptr_not_equal(RaftReqTypeStr[i], NULL);
    }
}

static void test_parse_slots(void **state)
{
    char slots[REDIS_RAFT_HASH_SLOTS] = {0};
    parseHashSlots(slots, "0");
    assert_int_equal(slots[0], 1);
    for (size_t i = 1; i < REDIS_RAFT_HASH_SLOTS; i++) {
        assert_int_equal(slots[i], 0);
    }
    memset(slots, 0, sizeof(slots));
    parseHashSlots(slots, "0-10");
    for (size_t i = 0; i < 11; i++) {
        assert_int_equal(slots[i], 1);
    }
    for (size_t i = 11; i < REDIS_RAFT_HASH_SLOTS; i++) {
        assert_int_equal(slots[i], 0);
    }
    memset(slots, 0, sizeof(slots));
    parseHashSlots(slots, "0,5-10,16,58-62,100");
    assert_int_equal(slots[0], 1);
    for (size_t i = 1; i < 5; i++) {
        assert_int_equal(slots[i], 0);
    }
    for (size_t i = 5; i < 11; i++) {
        assert_int_equal(slots[i], 1);
    }
    for (size_t i = 11; i < 16; i++) {
        assert_int_equal(slots[i], 0);
    }
    assert_int_equal(slots[16], 1);
    for (size_t i = 17; i < 58; i++) {
        assert_int_equal(slots[i], 0);
    }
    for (size_t i = 58; i < 63; i++) {
        assert_int_equal(slots[i], 1);
    }
    for (size_t i = 63; i < 100; i++) {
        assert_int_equal(slots[i], 0);
    }
    assert_int_equal(slots[100], 1);
    for (size_t i = 101; i < REDIS_RAFT_HASH_SLOTS; i++) {
        assert_int_equal(slots[i], 0);
    }
}

const struct CMUnitTest util_tests[] = {
    cmocka_unit_test(test_raftreq_str),
    cmocka_unit_test(test_parse_slots),
    {.test_func = NULL},
};
