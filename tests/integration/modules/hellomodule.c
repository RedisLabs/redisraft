/*
 * Copyright Redis Ltd. 2020 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
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

    rc = RedisModule_CreateCommand(ctx, "hellomodule", cmdHello, "write deny-oom", 0, 0, 0);
    if (rc != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}
