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

static void setupRedisCommand(RaftRedisCommand *target, const char **argv, int argc)
{
    int i;

    target->argc = argc;
    target->argv = test_malloc(sizeof(RedisModuleString *) * argc);
    for (i = 0; i < argc; i++) {
        int argv_len = strlen(argv[i]);
        target->argv[i] = test_malloc(argv_len + 1);
        memcpy(target->argv[i], argv[i], argv_len + 1);
    }
}


static void test_serialize_redis_command(void **state)
{
    const char *cmd1_argv[] = { "SET", "key", "value" };
    const char *cmd2_argv[] = { "GET", "mykey" };
    const char *cmd3_argv[] = { "PING" };

    RaftRedisCommandArray cmd_array = { 0 };
    setupRedisCommand(RaftRedisCommandArrayExtend(&cmd_array), cmd1_argv, 3);
    setupRedisCommand(RaftRedisCommandArrayExtend(&cmd_array), cmd2_argv, 2);
    setupRedisCommand(RaftRedisCommandArrayExtend(&cmd_array), cmd3_argv, 1);
    
    const char *expected = "*3\n*3\n$3\nSET\n$3\nkey\n$5\nvalue\n*2\n$3\nGET\n$5\nmykey\n*1\n$4\nPING\n";

    raft_entry_t *e = RaftRedisCommandArraySerialize(&cmd_array);
    assert_non_null(e);
    assert_int_equal(e->data_len, strlen(expected));
    assert_memory_equal(e->data, expected, strlen(expected));
    raft_entry_release(e);

    RaftRedisCommandArrayFree(&cmd_array);
}

static void test_deserialize_redis_command(void **state)
{
    const char *serialized = "*3\n$3\nSET\n$3\nkey\n$5\nvalue\n";
    int serialized_len = strlen(serialized);

    RaftRedisCommand target = { 0 };
    assert_int_equal(RaftRedisCommandDeserialize(&target, serialized, serialized_len),
            serialized_len);
    RaftRedisCommandFree(&target);
}

static void test_deserialize_redis_command_array(void **state)
{
    const char *serialized = "*3\n*3\n$3\nSET\n$3\nkey\n$5\nvalue\n*2\n$3\nGET\n$5\nmykey\n*1\n$4\nPING\n";
    int serialized_len = strlen(serialized);

    RaftRedisCommandArray cmd_array = { 0 };
    cmd_array.len = 5; /* free would be called to reset it */
    assert_int_equal(RaftRedisCommandArrayDeserialize(&cmd_array, serialized, serialized_len), RR_OK);
    assert_int_equal(cmd_array.len, 3);

    RaftRedisCommandArrayFree(&cmd_array);
}

static void test_deserialize_corrupted_data(void **state)
{
    RaftRedisCommand target = { 0 };

    /* empty */
    const char *d_null = "";
    assert_int_equal(RaftRedisCommandDeserialize(&target, d_null, 0), 0);

    /* ---- mbulk prefix ---- */

    /* bad prefix */
    const char *d_bad_prefix = "$50\n";
    assert_int_equal(RaftRedisCommandDeserialize(&target,
                d_bad_prefix, strlen(d_bad_prefix)), 0);

    /* non-numeric mbulk */
    const char *d_non_numeric_mbulk = "$-50\n";
    assert_int_equal(RaftRedisCommandDeserialize(&target,
                d_non_numeric_mbulk, strlen(d_non_numeric_mbulk)), 0);

    /* zero mbulk */
    const char *d_zero_mbulk = "*0\n";
    assert_int_equal(RaftRedisCommandDeserialize(&target,
                d_zero_mbulk, strlen(d_zero_mbulk)), 0);

    /* truncated mbulk */
    const char *d_truncated_mbulk = "*5";
    assert_int_equal(RaftRedisCommandDeserialize(&target,
                d_truncated_mbulk, strlen(d_truncated_mbulk)), 0);

    /* ---- arguments ---- */

    /* bad argument length */
    const char *d_bad_arg_len = "*1\n$bad\n";
    assert_int_equal(RaftRedisCommandDeserialize(&target,
                d_bad_arg_len, strlen(d_bad_arg_len)), 0);

    /* zero argument length */
    const char *d_zero_arg_len = "*1\n$0\n";
    assert_int_equal(RaftRedisCommandDeserialize(&target,
                d_zero_arg_len, strlen(d_zero_arg_len)), 0);

    /* ---- command array issues ---- */

    RaftRedisCommandArray cmd_array = { 0 };

    /* bad command count */
    const char *d_bad_array_len = "$1\n";
    assert_int_equal(RaftRedisCommandArrayDeserialize(&cmd_array,
                d_bad_array_len, strlen(d_bad_array_len)), RR_ERROR);

    /* empty command array */
    const char *d_zero_array_len = "*0\n";
    assert_int_equal(RaftRedisCommandArrayDeserialize(&cmd_array,
                d_zero_array_len, strlen(d_zero_array_len)), RR_ERROR);

    /* empty command */
    const char *d_array_empty_command = "*1\n*0\n";
    assert_int_equal(RaftRedisCommandArrayDeserialize(&cmd_array,
                d_array_empty_command, strlen(d_array_empty_command)), RR_ERROR);
}

const struct CMUnitTest serialization_tests[] = {
    cmocka_unit_test(test_serialize_redis_command),
    cmocka_unit_test(test_deserialize_redis_command),
    cmocka_unit_test(test_deserialize_redis_command_array),
    cmocka_unit_test(test_deserialize_corrupted_data),
    { .test_func = NULL }
};
