/*
 * This file is part of RedisRaft.
 *
 * Copyright (c) 2020-2021 Redis Ltd.
 *
 * RedisRaft is licensed under the Redis Source Available License (RSAL).
 */

#include <strings.h>

#include "redisraft.h"

static void replySortedArray(RedisModuleCtx *ctx, RedisModuleCallReply * reply, int reply_type)
{
    size_t len = RedisModule_CallReplyLength(reply);

    RedisModuleDict *dict = RedisModule_CreateDict(ctx);
    for(size_t i = 0; i < len; i++) {
        long *val;
        RedisModuleCallReply * entry;
        switch (reply_type) {
            case REDISMODULE_REPLY_ARRAY:
                entry = RedisModule_CallReplyArrayElement(reply, i);
                break;
            case REDISMODULE_REPLY_SET:
                entry = RedisModule_CallReplySetElement(reply, i);
                break;
        }

        size_t entry_len;
        const char * entry_str = RedisModule_CallReplyStringPtr(entry, &entry_len);
        val = RedisModule_DictGetC(dict, (char *) entry_str, entry_len, NULL);
        if (!val) {
            val = RedisModule_Calloc(1, sizeof(long));
        }
        (*val)++;
        RedisModule_DictSetC(dict, (char *) entry_str, entry_len, val);
    }

    switch (reply_type)
    {
        case REDISMODULE_REPLY_ARRAY:
            RedisModule_ReplyWithArray(ctx, len);
            break;
        case REDISMODULE_REPLY_SET:
            RedisModule_ReplyWithSet(ctx, len);
            break;
    }

    char *key;
    size_t key_len;
    long *val;
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(dict, "^", NULL, 0);
    while((key = RedisModule_DictNextC(iter, &key_len, (void **) &val)) != NULL) {
        for(long i=0; i < *val; i++) {
            RedisModule_ReplyWithStringBuffer(ctx, key, key_len);
        }
        RedisModule_Free(val);
    }
    RedisModule_DictIteratorStop(iter);
    RedisModule_FreeDict(ctx, dict);
}

static void replySortedMap(RedisModuleCtx *ctx, RedisModuleCallReply * reply)
{
    size_t len = RedisModule_CallReplyLength(reply);
    RedisModuleDict *dict = RedisModule_CreateDict(ctx);

    for(size_t i = 0; i < len; i++) {
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
    RedisModule_Log(redis_raft_log_ctx, REDIS_NOTICE, "in handle sort");
    const CommandSpec *cs = CommandSpecGet(argv[0]);
    if (!cs || !(cs->flags & CMD_SPEC_SORT_REPLY)) {
        RedisModule_ReplyWithError(ctx, "ERR: not a sortable command");
        return;
    }

    size_t cmd_len;
    const char *cmd_str = RedisModule_StringPtrLen(argv[0], &cmd_len);

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
            case REDISMODULE_REPLY_SET:
                replySortedArray(ctx, reply, reply_type);
                break;
            case REDISMODULE_REPLY_MAP:
                replySortedMap(ctx, reply);
                break;
            default:
                RedisModule_ReplyWithCallReply(ctx, reply);
        }

        RedisModule_FreeCallReply(reply);
    }
}
