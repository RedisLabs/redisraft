/*
 * This file is part of RedisRaft.
 *
 * Copyright (c) 2020 Redis Labs
 *
 * RedisRaft is dual licensed under the GNU Affero General Public License version 3
 * (AGPLv3) or the Redis Source Available License (RSAL).
 */

#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <unistd.h>
#include <string.h>

#include "cmocka.h"

#include "../redisraft.h"

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

static void test_redis_info_iterate(void **state)
{
    const char info[] = 
        "# Section\r\n"
        "key1:val1\r\n"
        "key2222:val22\r\n"
        "emptykey:\r\n"
        "\r\n"
        "# Section2\r\n"
        "\r\n"
        "invalidline\r\n"
        "key3:val3\r\n";

    size_t info_len = strlen(info);
    const char *p = info;

    const char *key, *val;
    size_t keylen, vallen;

    assert_int_equal(RedisInfoIterate(&p, &info_len, &key, &keylen, &val, &vallen), 1);
    assert_int_equal(keylen, 4);
    assert_memory_equal(key, "key1", 4);
    assert_int_equal(vallen, 4);
    assert_memory_equal(val, "val1", 4);

    assert_int_equal(RedisInfoIterate(&p, &info_len, &key, &keylen, &val, &vallen), 1);
    assert_int_equal(keylen, 7);
    assert_memory_equal(key, "key2222", 7);
    assert_int_equal(vallen, 5);
    assert_memory_equal(val, "val22", 5);

    assert_int_equal(RedisInfoIterate(&p, &info_len, &key, &keylen, &val, &vallen), 1);
    assert_int_equal(keylen, 8);
    assert_memory_equal(key, "emptykey", 8);
    assert_int_equal(vallen, 0);

    assert_int_equal(RedisInfoIterate(&p, &info_len, &key, &keylen, &val, &vallen), 1);
    assert_int_equal(keylen, 4);
    assert_memory_equal(key, "key3", 4);
    assert_int_equal(vallen, 4);
    assert_memory_equal(val, "val3", 4);

    assert_int_equal(RedisInfoIterate(&p, &info_len, &key, &keylen, &val, &vallen), 0);

    const char badinfo[] = "# Section";
    p = badinfo;
    info_len = strlen(badinfo);
    assert_int_equal(RedisInfoIterate(&p, &info_len, &key, &keylen, &val, &vallen), -1);
}

const struct CMUnitTest util_tests[] = {
    cmocka_unit_test(test_redis_info_iterate),
    cmocka_unit_test(test_memory_conversion),
    { .test_func = NULL }
};
