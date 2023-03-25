/*
 * Copyright Redis Ltd. 2020 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "redisraft.h"

#include <strings.h>

/* Sort Redis array type responses (arrays, sets....) */
static void replySortedArray(RedisModuleCtx *ctx, RedisModuleCallReply *reply)
{
    size_t len = RedisModule_CallReplyLength(reply);
    int reply_type = RedisModule_CallReplyType(reply);

    RedisModuleDict *dict = RedisModule_CreateDict(ctx);
    for (size_t i = 0; i < len; i++) {
        RedisModuleCallReply *entry;
        switch (reply_type) {
            /* if one adds more supported type here,
             * have to add the proper ReplyWith function below
             */
            case REDISMODULE_REPLY_ARRAY:
                entry = RedisModule_CallReplyArrayElement(reply, i);
                break;
            case REDISMODULE_REPLY_SET:
                entry = RedisModule_CallReplySetElement(reply, i);
                break;
            default:
                RedisModule_ReplyWithError(ctx, "ERR unknown type to sort");
                goto early_exit;
        }

        size_t entry_len;
        const char *entry_str = RedisModule_CallReplyStringPtr(entry, &entry_len);
        unsigned long val = (unsigned long) RedisModule_DictGetC(dict, (char *) entry_str, entry_len, NULL);
        val++;
        RedisModule_DictReplaceC(dict, (char *) entry_str, entry_len, (void *) val);
    }

    switch (reply_type) {
        case REDISMODULE_REPLY_ARRAY:
            if (RedisModule_ReplyWithArray(ctx, len) != REDISMODULE_OK) {
                RedisModule_ReplyWithError(ctx, "Failed to generate sorted reply");
                goto early_exit;
            }
            break;
        case REDISMODULE_REPLY_SET:
            if (RedisModule_ReplyWithSet(ctx, len) != REDISMODULE_OK) {
                RedisModule_ReplyWithError(ctx, "Failed to generate sorted reply");
                goto early_exit;
            }
            break;
        default:
            /* shouldn't get here, as should be protected above */
            RedisModule_Assert(0);
    }

    char *key;
    size_t key_len;
    unsigned long val;
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(dict, "^", NULL, 0);
    while ((key = RedisModule_DictNextC(iter, &key_len, (void **) &val)) != NULL) {
        for (unsigned long i = 0; i < val; i++) {
            RedisModule_ReplyWithStringBuffer(ctx, key, key_len);
        }
    }
    RedisModule_DictIteratorStop(iter);

early_exit:
    RedisModule_FreeDict(ctx, dict);
}

/* Sort Redis key/value responses, that are sorted on the key */
static void replySortedMap(RedisModuleCtx *ctx, RedisModuleCallReply *reply)
{
    size_t len = RedisModule_CallReplyLength(reply);
    RedisModuleDict *dict = RedisModule_CreateDict(ctx);

    for (size_t i = 0; i < len; i++) {
        RedisModuleCallReply *key;
        RedisModuleCallReply *value;

        RedisModule_CallReplyMapElement(reply, i, &key, &value);
        size_t key_len;
        const char *key_str = RedisModule_CallReplyStringPtr(key, &key_len);

        if (RedisModule_DictSetC(dict, (char *) key_str, key_len, value) != REDISMODULE_OK) {
            RedisModule_ReplyWithError(ctx, "Failed to generate sorted reply");
            goto early_exit;
        }
    }

    if (RedisModule_ReplyWithMap(ctx, len) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx, "Failed to generate sorted reply");
        goto early_exit;
    }

    char *key;
    size_t key_len;
    void *value;

    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(dict, "^", NULL, 0);
    while ((key = RedisModule_DictNextC(iter, &key_len, &value)) != NULL) {
        RedisModule_ReplyWithStringBuffer(ctx, key, key_len);
        RedisModule_ReplyWithCallReply(ctx, value);
    }
    RedisModule_DictIteratorStop(iter);

early_exit:
    RedisModule_FreeDict(ctx, dict);
}

/* Calls Redis commands whose results can be sorted without semantically breaking them */
void handleSort(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    int flags = CommandSpecTableGetFlags(redis_raft.commands_spec_table, redis_raft.subcommand_spec_tables, argv[0], NULL);
    if (flags == -1 || !(flags & CMD_SPEC_SORT_REPLY)) {
        RedisModule_ReplyWithError(ctx, "ERR not a sortable command");
        return;
    }

    size_t cmd_len;
    const char *cmd_str = RedisModule_StringPtrLen(argv[0], &cmd_len);

    enterRedisModuleCall();
    int entered_eval = redis_raft.entered_eval;
    redis_raft.entered_eval = 0;
    RedisModuleCallReply *reply = RedisModule_Call(redis_raft.ctx, cmd_str, "3vE", &argv[1], argc - 1);
    exitRedisModuleCall();
    redis_raft.entered_eval = entered_eval;

    int reply_type = RedisModule_CallReplyType(reply);
    switch (reply_type) {
        case REDISMODULE_REPLY_ARRAY:
        case REDISMODULE_REPLY_SET:
            replySortedArray(ctx, reply);
            break;
        case REDISMODULE_REPLY_MAP:
            replySortedMap(ctx, reply);
            break;
        default:
            RedisModule_ReplyWithCallReply(ctx, reply);
    }

    RedisModule_FreeCallReply(reply);
}
