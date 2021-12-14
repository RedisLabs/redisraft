/*
 * This file is part of RedisRaft.
 *
 * Copyright (c) 2020-2021 Redis Ltd.
 *
 * RedisRaft is licensed under the Redis Source Available License (RSAL).
 */

#include <strings.h>

#include "redisraft.h"

static void replySortedArray(RedisModuleCtx *ctx, RedisModuleCallReply * reply)
{
    size_t len = RedisModule_CallReplyLength(reply);
    if (len == 0) {
        RedisModule_ReplyWithNullArray(ctx);
        return;
    }

    RedisModuleDict * dict = RedisModule_CreateDict(ctx);
    for(int i=0; i < len; i++) {
        RedisModuleCallReply * entry = RedisModule_CallReplyArrayElement(reply, i);
        size_t entry_len;
        const char * entry_str = RedisModule_CallReplyStringPtr(entry, &entry_len);
        RedisModule_DictSetC(dict, (char *) entry_str, entry_len, NULL);
    }

    RedisModule_ReplyWithArray(ctx, len);

    char *key;
    size_t key_len;
    void *data;
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(dict, "^", NULL, 0);
    while((key = RedisModule_DictNextC(iter, &key_len, &data)) != NULL) {
        RedisModule_ReplyWithStringBuffer(ctx, key, key_len);
    }
    RedisModule_DictIteratorStop(iter);
    RedisModule_FreeDict(ctx, dict);
}

static void replySortedMap(RedisModuleCtx *ctx, RedisModuleCallReply * reply)
{
    size_t len = RedisModule_CallReplyLength(reply);
    RedisModuleDict * dict = RedisModule_CreateDict(ctx);

    for(int i=0; i < len; i++) {
        RedisModuleCallReply * key;
        RedisModuleCallReply * value;

        RedisModule_CallReplyMapElement(reply, i, &key, &value);
        size_t key_len;
        const char * key_str = RedisModule_CallReplyStringPtr(key, &key_len);

        RedisModule_DictSetC(dict, (char *) key_str, key_len, value);
    }

    RedisModule_ReplyWithMap(ctx, len);

    char *key;
    size_t key_len;
    void *value;
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(dict, "^", NULL, 0);
    while((key = RedisModule_DictNextC(iter, &key_len, &value)) != NULL) {
        RedisModule_ReplyWithStringBuffer(ctx, key, key_len);
        RedisModule_ReplyWithCallReply(ctx, value);
    }
}

void handleSort(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    if (!sortableCommand(argv[0])) {
        RedisModule_ReplyWithError(ctx, "ERR: not a sortable command");
        return;
    }

    size_t cmd_len;
    const char * cmd_str = RedisModule_StringPtrLen(argv[0], &cmd_len);

    enterRedisModuleCall();
    int entered_eval = redis_raft.entered_eval;
    redis_raft.entered_eval = 0;
    RedisModule_Log(redis_raft_log_ctx, REDIS_NOTICE, "Calling RM_Call for: %.*s", (int) cmd_len, cmd_str);
    RedisModuleCallReply *reply = RedisModule_Call(redis_raft.ctx, cmd_str, "3v", &argv[1], argc-1);
    exitRedisModuleCall();
    redis_raft.entered_eval = entered_eval;

    if (!reply) {
        RedisModule_Log(redis_raft_log_ctx, REDIS_NOTICE, "Bad command or failed to execute: %.*s", (int) cmd_len, cmd_str);
        RedisModule_ReplyWithError(ctx, "Bad command or failed to execute");
    } else {
        int reply_type = RedisModule_CallReplyType(reply);
        switch (reply_type) {
            case REDISMODULE_REPLY_ARRAY:
                RedisModule_Log(redis_raft_log_ctx, REDIS_NOTICE, "got an ARRAY");
                replySortedArray(ctx, reply);
                break;
            case REDISMODULE_REPLY_MAP:
                RedisModule_Log(redis_raft_log_ctx, REDIS_NOTICE, "got a MAP");
                replySortedMap(ctx, reply);
                break;
            default:
                RedisModule_Log(redis_raft_log_ctx, REDIS_NOTICE, "got something else");
                RedisModule_ReplyWithCallReply(ctx, reply);
        }

        RedisModule_FreeCallReply(reply);
    }
}
