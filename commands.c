/*
 * This file is part of RedisRaft.
 *
 * Copyright (c) 2021 Redis Labs
 *
 * RedisRaft is dual licensed under the GNU Affero General Public License version 3
 * (AGPLv3) or the Redis Source Available License (RSAL).
 */
/* ------------------------------------ Command Classification ------------------------------------ */

#include <string.h>
#include <ctype.h>
#include "redisraft.h"

static RedisModuleDict *commandSpecDict = NULL;

RRStatus CommandSpecInit(RedisModuleCtx *ctx)
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
            { "srandmember",            CMD_SPEC_READONLY },
            { "sinter",                 CMD_SPEC_READONLY },
            { "sunion",                 CMD_SPEC_READONLY },
            { "sdiff",                  CMD_SPEC_READONLY },
            { "smembers",               CMD_SPEC_READONLY },
            { "sscan",                  CMD_SPEC_READONLY },
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
            { "zscan",                  CMD_SPEC_READONLY },
            { "hmget",                  CMD_SPEC_READONLY },
            { "hlen",                   CMD_SPEC_READONLY },
            { "hstrlen",                CMD_SPEC_READONLY },
            { "hkeys",                  CMD_SPEC_READONLY },
            { "hvals",                  CMD_SPEC_READONLY },
            { "hgetall",                CMD_SPEC_READONLY },
            { "hexists",                CMD_SPEC_READONLY },
            { "hscan",                  CMD_SPEC_READONLY },
            { "randomkey",              CMD_SPEC_READONLY },
            { "keys",                   CMD_SPEC_READONLY },
            { "scan",                   CMD_SPEC_READONLY },
            { "dbsize",                 CMD_SPEC_READONLY },
            { "ttl",                    CMD_SPEC_READONLY },
            { "bitcount",               CMD_SPEC_READONLY },
            { "georadius_ro",           CMD_SPEC_READONLY },
            { "georadiusbymember_ro",   CMD_SPEC_READONLY },
            { "geohash",                CMD_SPEC_READONLY },
            { "geopos",                 CMD_SPEC_READONLY },
            { "geodist",                CMD_SPEC_READONLY },
            { "pfcount",                CMD_SPEC_READONLY },
            { "sync",                   CMD_SPEC_UNSUPPORTED },
            { "psync",                  CMD_SPEC_UNSUPPORTED },
            { "reset",                  CMD_SPEC_UNSUPPORTED },
            { "bgrewriteaof",           CMD_SPEC_UNSUPPORTED },
            { "slaveof",                CMD_SPEC_UNSUPPORTED },
            { "replicaof",              CMD_SPEC_UNSUPPORTED },
            { "debug",                  CMD_SPEC_UNSUPPORTED },
            { "auth",                   CMD_SPEC_DONT_INTERCEPT },
            { "ping",                   CMD_SPEC_DONT_INTERCEPT },
            { "save",                   CMD_SPEC_DONT_INTERCEPT },
            { "bgsave",                 CMD_SPEC_DONT_INTERCEPT },
            { "module",                 CMD_SPEC_DONT_INTERCEPT },
            { "info",                   CMD_SPEC_DONT_INTERCEPT },
            { "client",                 CMD_SPEC_DONT_INTERCEPT },
            { "config",                 CMD_SPEC_DONT_INTERCEPT },
            { "monitor",                CMD_SPEC_DONT_INTERCEPT },
            { "command",                CMD_SPEC_DONT_INTERCEPT },
            { "shutdown",               CMD_SPEC_DONT_INTERCEPT },
            { "watch",                  CMD_SPEC_DONT_INTERCEPT },
            { "unwatch",                CMD_SPEC_DONT_INTERCEPT },
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
            { "raft",                   CMD_SPEC_DONT_INTERCEPT },
            { "raft.entry",             CMD_SPEC_DONT_INTERCEPT },
            { "raft.config",            CMD_SPEC_DONT_INTERCEPT },
            { "raft.cluster",           CMD_SPEC_DONT_INTERCEPT },
            { "raft.shardgroup",        CMD_SPEC_DONT_INTERCEPT },
            { "raft.node",              CMD_SPEC_DONT_INTERCEPT },
            { "raft.ae",                CMD_SPEC_DONT_INTERCEPT },
            { "raft.requestvote",       CMD_SPEC_DONT_INTERCEPT },
            { "raft.loadsnapshot",      CMD_SPEC_DONT_INTERCEPT },
            { "raft.debug",             CMD_SPEC_DONT_INTERCEPT },
            { "raft.info",              CMD_SPEC_DONT_INTERCEPT },
            { NULL,                     0 }
    };

    commandSpecDict = RedisModule_CreateDict(ctx);
    for (int i = 0; commands[i].name != NULL; i++) {
        if (RedisModule_DictSetC(commandSpecDict, commands[i].name,
            strlen(commands[i].name), &commands[i]) != REDISMODULE_OK) {
                RedisModule_FreeDict(ctx, commandSpecDict);
                return RR_ERROR;
        }
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
        const CommandSpec *cs = CommandSpecGet(array->commands[i]->argv[0]);
        if (cs) {
            flags |= cs->flags;
        } else {
            flags |= default_flags;
        }
    }

    return flags;
}
