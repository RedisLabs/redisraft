/*
 * This file is part of RedisRaft.
 *
 * Copyright (c) 2020-2021 Redis Ltd.
 *
 * RedisRaft is licensed under the Redis Source Available License (RSAL).
 */

#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <unistd.h>
#include <string.h>

#include "cmocka.h"

#include "../src/redisraft.h"

static void test_raftreq_str(void **state)
{
    for (int i = 1; i < RR_RAFTREQ_MAX; i++) {
        assert_ptr_not_equal(RaftReqTypeStr[i], NULL);
    }
}

static void test_memory_conversion(void **state)
{
    unsigned long val;

    /* bad values */
    assert_int_equal(parseMemorySize("nan", &val), RR_ERROR);
    assert_int_equal(parseMemorySize("kb", &val), RR_ERROR);

    /* non suffixed */
    assert_int_equal(parseMemorySize("0", &val), RR_OK);
    assert_int_equal(val, 0);

    assert_int_equal(parseMemorySize("17179869184", &val), RR_OK);
    assert_int_equal(val, 17179869184L);

    /* suffixes */
    assert_int_equal(parseMemorySize("0gb", &val), RR_OK);
    assert_int_equal(val, 0);

    assert_int_equal(parseMemorySize("1kb", &val), RR_OK);
    assert_int_equal(val, 1000);

    assert_int_equal(parseMemorySize("1mb", &val), RR_OK);
    assert_int_equal(val, 1000*1000);

    assert_int_equal(parseMemorySize("1gb", &val), RR_OK);
    assert_int_equal(val, 1000*1000*1000);

    assert_int_equal(parseMemorySize("1kib", &val), RR_OK);
    assert_int_equal(val, 1024);

    assert_int_equal(parseMemorySize("1mib", &val), RR_OK);
    assert_int_equal(val, 1024*1024);

    assert_int_equal(parseMemorySize("1gib", &val), RR_OK);
    assert_int_equal(val, 1024L*1024*1024);

    /* Formatting */
    char buf[50];
    assert_int_equal(formatExactMemorySize(1234, buf, sizeof(buf)), RR_OK);
    assert_string_equal(buf, "1234");

    assert_int_equal(formatExactMemorySize(1024, buf, sizeof(buf)), RR_OK);
    assert_string_equal(buf, "1KiB");

    assert_int_equal(formatExactMemorySize(1000, buf, sizeof(buf)), RR_OK);
    assert_string_equal(buf, "1KB");

    assert_int_equal(formatExactMemorySize(10L*1024*1024*1024, buf, sizeof(buf)), RR_OK);
    assert_string_equal(buf, "10GiB");

    assert_int_equal(formatExactMemorySize(10L*1000*1000*1000, buf, sizeof(buf)), RR_OK);
    assert_string_equal(buf, "10GB");

    assert_int_equal(formatExactMemorySize(1000*1000, buf, 4), RR_ERROR);
}

const struct CMUnitTest util_tests[] = {
    cmocka_unit_test(test_raftreq_str),
    cmocka_unit_test(test_memory_conversion),
    { .test_func = NULL }
};
