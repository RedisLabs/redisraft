/*
 * This file is part of RedisRaft.
 *
 * Copyright (c) 2021 Redis Ltd.
 *
 * RedisRaft is licensed under the Redis Source Available License (RSAL).
 */
/* ------------------------------------ Command Classification ------------------------------------ */

#include <string.h>
#include <ctype.h>
#include "redisraft.h"

static RedisModuleDict *commandSpecDict = NULL;

RRStatus CommandSpecInit(RedisModuleCtx *ctx, RedisRaftConfig *config)
{
    static CommandSpec commands[] = {
            /* Core Redis Commands */
            { "get",                    CMD_SPEC_READONLY },
            { "strlen",                 CMD_SPEC_READONLY },
            { "exists",                 CMD_SPEC_READONLY },
            { "getbit",                 CMD_SPEC_READONLY },
            { "getrange",               CMD_SPEC_READONLY },
            { "substr",                 CMD_SPEC_READONLY },
            { "mget",                   CMD_SPEC_READONLY },
            { "llen",                   CMD_SPEC_READONLY },
            { "lindex",                 CMD_SPEC_READONLY },
            { "lrange",                 CMD_SPEC_READONLY },
            { "scard",                  CMD_SPEC_READONLY },
            { "sismember",              CMD_SPEC_READONLY },
            { "srandmember",            CMD_SPEC_READONLY | CMD_SPEC_RANDOM },
            { "sinter",                 CMD_SPEC_READONLY | CMD_SPEC_SORT_REPLY },
            { "sunion",                 CMD_SPEC_READONLY | CMD_SPEC_SORT_REPLY },
            { "sdiff",                  CMD_SPEC_READONLY | CMD_SPEC_SORT_REPLY },
            { "smembers",               CMD_SPEC_READONLY | CMD_SPEC_SORT_REPLY },
            { "sscan",                  CMD_SPEC_READONLY | CMD_SPEC_RANDOM },
            { "zrange",                 CMD_SPEC_READONLY },
            { "zrangebyscore",          CMD_SPEC_READONLY },
            { "zrevrangebyscore",       CMD_SPEC_READONLY },
            { "zrangebylex",            CMD_SPEC_READONLY },
            { "zrevrangebylex",         CMD_SPEC_READONLY },
            { "zcount",                 CMD_SPEC_READONLY },
            { "zlexcount",              CMD_SPEC_READONLY },
            { "zrevrange",              CMD_SPEC_READONLY },
            { "zcard",                  CMD_SPEC_READONLY },
            { "zscore",                 CMD_SPEC_READONLY },
            { "zrank",                  CMD_SPEC_READONLY },
            { "zrevrank",               CMD_SPEC_READONLY },
            { "zscan",                  CMD_SPEC_READONLY | CMD_SPEC_RANDOM },
            { "hmget",                  CMD_SPEC_READONLY },
            { "hlen",                   CMD_SPEC_READONLY },
            { "hstrlen",                CMD_SPEC_READONLY },
            { "hkeys",                  CMD_SPEC_READONLY | CMD_SPEC_SORT_REPLY },
            { "hvals",                  CMD_SPEC_READONLY | CMD_SPEC_SORT_REPLY },
            { "hgetall",                CMD_SPEC_READONLY | CMD_SPEC_SORT_REPLY },
            { "hexists",                CMD_SPEC_READONLY },
            { "hscan",                  CMD_SPEC_READONLY | CMD_SPEC_RANDOM },
            { "randomkey",              CMD_SPEC_READONLY | CMD_SPEC_RANDOM },
            { "keys",                   CMD_SPEC_READONLY | CMD_SPEC_SORT_REPLY },
            { "scan",                   CMD_SPEC_READONLY | CMD_SPEC_RANDOM },
            { "dbsize",                 CMD_SPEC_READONLY },
            { "ttl",                    CMD_SPEC_READONLY | CMD_SPEC_RANDOM },
            { "pttl",                   CMD_SPEC_READONLY | CMD_SPEC_RANDOM },
            { "expiretime",             CMD_SPEC_READONLY | CMD_SPEC_RANDOM },
            { "pexiretime",             CMD_SPEC_READONLY | CMD_SPEC_RANDOM },
            { "time",                   CMD_SPEC_DONT_INTERCEPT | CMD_SPEC_RANDOM },
            { "bitcount",               CMD_SPEC_READONLY },
            { "georadius_ro",           CMD_SPEC_READONLY },
            { "georadiusbymember_ro",   CMD_SPEC_READONLY },
            { "geohash",                CMD_SPEC_READONLY },
            { "geopos",                 CMD_SPEC_READONLY },
            { "geodist",                CMD_SPEC_READONLY },
            { "pfcount",                CMD_SPEC_READONLY },
            { "spop",                   CMD_SPEC_RANDOM },
            { "zrandmember",            CMD_SPEC_RANDOM },
            { "hrandfield",             CMD_SPEC_RANDOM },
            { "sync",                   CMD_SPEC_UNSUPPORTED },
            { "psync",                  CMD_SPEC_UNSUPPORTED },
            { "reset",                  CMD_SPEC_UNSUPPORTED },
            { "bgrewriteaof",           CMD_SPEC_UNSUPPORTED },
            { "slaveof",                CMD_SPEC_UNSUPPORTED },
            { "replicaof",              CMD_SPEC_UNSUPPORTED },
            { "debug",                  CMD_SPEC_UNSUPPORTED },
            { "watch",                  CMD_SPEC_UNSUPPORTED },
            { "unwatch",                CMD_SPEC_UNSUPPORTED },
            { "save",                   CMD_SPEC_UNSUPPORTED },
            { "bgsave",                 CMD_SPEC_UNSUPPORTED },
            /* Blocking commands not supported */
            { "brpop",                  CMD_SPEC_UNSUPPORTED },
            { "brpoplpush",             CMD_SPEC_UNSUPPORTED },
            { "blmove",                 CMD_SPEC_UNSUPPORTED },
            { "blpop",                  CMD_SPEC_UNSUPPORTED },
            { "blmpop",                 CMD_SPEC_UNSUPPORTED },
            { "bzpopmin",               CMD_SPEC_UNSUPPORTED },
            { "bzpopmax",               CMD_SPEC_UNSUPPORTED },
            { "bzmpop",                 CMD_SPEC_UNSUPPORTED },
            /* Stream commands not supported */
            { "xadd",                   CMD_SPEC_UNSUPPORTED | CMD_SPEC_RANDOM },
            { "xrange",                 CMD_SPEC_UNSUPPORTED },
            { "xrevrange",              CMD_SPEC_UNSUPPORTED },
            { "xlen",                   CMD_SPEC_UNSUPPORTED },
            { "xread",                  CMD_SPEC_UNSUPPORTED },
            { "xreadgroup",             CMD_SPEC_UNSUPPORTED },
            { "xgroup",                 CMD_SPEC_UNSUPPORTED },
            { "xsetid",                 CMD_SPEC_UNSUPPORTED },
            { "xack",                   CMD_SPEC_UNSUPPORTED | CMD_SPEC_RANDOM },
            { "xpending",               CMD_SPEC_UNSUPPORTED | CMD_SPEC_RANDOM },
            { "xclaim",                 CMD_SPEC_UNSUPPORTED | CMD_SPEC_RANDOM },
            { "xautoclaim",             CMD_SPEC_UNSUPPORTED | CMD_SPEC_RANDOM },
            { "xinfo",                  CMD_SPEC_UNSUPPORTED },
            { "xdel",                   CMD_SPEC_UNSUPPORTED },
            { "xtrim",                  CMD_SPEC_UNSUPPORTED | CMD_SPEC_RANDOM },
            /* Admin commands - bypassed */
            { "auth",                   CMD_SPEC_DONT_INTERCEPT },
            { "ping",                   CMD_SPEC_DONT_INTERCEPT },
            { "hello",                  CMD_SPEC_DONT_INTERCEPT },
            { "module",                 CMD_SPEC_DONT_INTERCEPT },
            { "client",                 CMD_SPEC_DONT_INTERCEPT },
            { "config",                 CMD_SPEC_DONT_INTERCEPT },
            { "monitor",                CMD_SPEC_DONT_INTERCEPT },
            { "command",                CMD_SPEC_DONT_INTERCEPT },
            { "shutdown",               CMD_SPEC_DONT_INTERCEPT },
            { "quit",                   CMD_SPEC_DONT_INTERCEPT },
            { "subscribe",              CMD_SPEC_DONT_INTERCEPT },
            { "psubscribe",             CMD_SPEC_DONT_INTERCEPT },
            { "unsubscribe",            CMD_SPEC_DONT_INTERCEPT },
            { "punsubscribe",           CMD_SPEC_DONT_INTERCEPT },
            { "publish",                CMD_SPEC_DONT_INTERCEPT },
            { "pubsub",                 CMD_SPEC_DONT_INTERCEPT },
            { "slowlog",                CMD_SPEC_DONT_INTERCEPT },
            { "acl",                    CMD_SPEC_DONT_INTERCEPT },

            /* RedisRaft Commands */
            { "raft",                         CMD_SPEC_DONT_INTERCEPT },
            { "raft.entry",                   CMD_SPEC_DONT_INTERCEPT },
            { "raft.config",                  CMD_SPEC_DONT_INTERCEPT },
            { "raft.cluster",                 CMD_SPEC_DONT_INTERCEPT },
            { "raft.shardgroup",              CMD_SPEC_DONT_INTERCEPT },
            { "raft.node",                    CMD_SPEC_DONT_INTERCEPT },
            { "raft.ae",                      CMD_SPEC_DONT_INTERCEPT },
            { "raft.requestvote",             CMD_SPEC_DONT_INTERCEPT },
            { "raft.snapshot",                CMD_SPEC_DONT_INTERCEPT },
            { "raft.debug",                   CMD_SPEC_DONT_INTERCEPT },
            { "raft.nodeshutdown",            CMD_SPEC_DONT_INTERCEPT },
            { "raft.transfer_leader",         CMD_SPEC_DONT_INTERCEPT },
            { "raft.timeout_now",             CMD_SPEC_DONT_INTERCEPT },
            { "raft._sort_reply",             CMD_SPEC_DONT_INTERCEPT },
            { "raft._reject_random_command",  CMD_SPEC_DONT_INTERCEPT },

            { NULL,0 }
    };

    commandSpecDict = RedisModule_CreateDict(ctx);
    for (int i = 0; commands[i].name != NULL; i++) {
        if (RedisModule_DictSetC(commandSpecDict, commands[i].name,
            strlen(commands[i].name), &commands[i]) != REDISMODULE_OK) {
                RedisModule_FreeDict(ctx, commandSpecDict);
                return RR_ERROR;
        }
    }

    if (config->ignored_commands) {
        char *temp = RedisModule_Strdup(config->ignored_commands);
        char *tok = strtok(temp, ",");
        while (tok != NULL) {
            CommandSpec * dont_intercept = RedisModule_Alloc(sizeof(CommandSpec));
            dont_intercept->name = RedisModule_Strdup(tok);
            dont_intercept->flags = CMD_SPEC_DONT_INTERCEPT;

            if (RedisModule_DictSetC(commandSpecDict, tok,
                                     strlen(tok), dont_intercept) != REDISMODULE_OK) {
                RedisModule_Free(temp);
                return RR_ERROR;
            }
            tok = strtok(NULL, ",");
        }
        RedisModule_Free(temp);
    }

    return RR_OK;
}

/* Look up the specified command in the command spec table and return the
 * CommandSpec associated with it, or NULL.
 */
const CommandSpec *CommandSpecGet(const RedisModuleString *cmd)
{
    size_t cmd_len;
    const char *cmd_str = RedisModule_StringPtrLen(cmd, &cmd_len);
    char buf[64];
    char *lcmd = buf;

    if (cmd_len > sizeof(buf)) {
        lcmd = RedisModule_Alloc(cmd_len);
    }

    for (size_t i = 0; i < cmd_len; i++) {
        lcmd[i] = (char) tolower(cmd_str[i]);
    }

    CommandSpec *cs = RedisModule_DictGetC(commandSpecDict, lcmd, cmd_len, NULL);
    if (lcmd != buf) {
        RedisModule_Free(lcmd);
    }

    return cs;
}

/* For a given RaftRedisCommandArray, return a flags value that represents
 * the aggregate flags of all commands. If a command is not listed in the
 * command table, use default_flags.
 */
unsigned int CommandSpecGetAggregateFlags(RaftRedisCommandArray *array, unsigned int default_flags)
{
    unsigned int flags = 0;
    for (int i = 0; i < array->len; i++) {
        /* we get the flag for the command that is asking, not the asking */
        RedisModuleString * cmd = array->commands[i]->argv[0];
        size_t cmd_len;
        const char * cmd_str = RedisModule_StringPtrLen(cmd, &cmd_len);

        if (!strncasecmp("asking", cmd_str, cmd_len)) {
            /* TODO: when asking <magic> support is added, need to adjust to argv[2] */
            if (array->commands[i]->argc > 1) {
                cmd = array->commands[i]->argv[1];
            } else {
                /* necessary to protect/ignore a bare "asking" command */
                continue;
            }
        }

        const CommandSpec *cs = CommandSpecGet(cmd);
        if (cs) {
            flags |= cs->flags;
        } else {
            flags |= default_flags;
        }
    }

    return flags;
}
