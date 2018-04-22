#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <unistd.h>
#include <string.h>

#include "cmocka.h"

#include "../redisraft.h"

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
    { .test_func = NULL }
};
