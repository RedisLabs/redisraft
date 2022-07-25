/*
* This file is part of RedisRaft.
*
* Copyright (c) 2020-2022 Redis Ltd.
*
* RedisRaft is licensed under the Redis Source Available License (RSAL).
*/

#include "redismodule.h"

int cmdHello(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    RedisModule_ReplyWithSimpleString(ctx, "HELLO");
    return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    int rc = RedisModule_Init(ctx, "hellomodule", 1, REDISMODULE_APIVER_1);
    if (rc != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    rc = RedisModule_CreateCommand(ctx, "hellomodule", cmdHello, "write", 0, 0, 0);
    if (rc != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}
