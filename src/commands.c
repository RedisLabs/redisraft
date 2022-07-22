/*
 * This file is part of RedisRaft.
 *
 * Copyright (c) 2021 Redis Ltd.
 *
 * RedisRaft is licensed under the Redis Source Available License (RSAL).
 */

#include "redisraft.h"

#include <ctype.h>
#include <string.h>
#include <strings.h>

static const CommandSpec commands[] = {
  /* Core Redis Commands */
    {"time",                        CMD_SPEC_DONT_INTERCEPT | CMD_SPEC_RANDOM},
    {"sync",                        CMD_SPEC_UNSUPPORTED                     },
    {"psync",                       CMD_SPEC_UNSUPPORTED                     },
    {"reset",                       CMD_SPEC_UNSUPPORTED                     },
    {"bgrewriteaof",                CMD_SPEC_UNSUPPORTED                     },
    {"slaveof",                     CMD_SPEC_UNSUPPORTED                     },
    {"replicaof",                   CMD_SPEC_UNSUPPORTED                     },
    {"debug",                       CMD_SPEC_UNSUPPORTED                     },
    {"watch",                       CMD_SPEC_UNSUPPORTED                     },
    {"unwatch",                     CMD_SPEC_UNSUPPORTED                     },
    {"save",                        CMD_SPEC_UNSUPPORTED                     },
    {"bgsave",                      CMD_SPEC_UNSUPPORTED                     },

 /* Blocking commands not supported */
    {"brpop",                       CMD_SPEC_UNSUPPORTED                     },
    {"brpoplpush",                  CMD_SPEC_UNSUPPORTED                     },
    {"blmove",                      CMD_SPEC_UNSUPPORTED                     },
    {"blpop",                       CMD_SPEC_UNSUPPORTED                     },
    {"blmpop",                      CMD_SPEC_UNSUPPORTED                     },
    {"bzpopmin",                    CMD_SPEC_UNSUPPORTED                     },
    {"bzpopmax",                    CMD_SPEC_UNSUPPORTED                     },
    {"bzmpop",                      CMD_SPEC_UNSUPPORTED                     },

 /* Stream commands not supported */
    {"xadd",                        CMD_SPEC_UNSUPPORTED | CMD_SPEC_RANDOM   },
    {"xrange",                      CMD_SPEC_UNSUPPORTED                     },
    {"xrevrange",                   CMD_SPEC_UNSUPPORTED                     },
    {"xlen",                        CMD_SPEC_UNSUPPORTED                     },
    {"xread",                       CMD_SPEC_UNSUPPORTED                     },
    {"xreadgroup",                  CMD_SPEC_UNSUPPORTED                     },
    {"xgroup",                      CMD_SPEC_UNSUPPORTED                     },
    {"xsetid",                      CMD_SPEC_UNSUPPORTED                     },
    {"xack",                        CMD_SPEC_UNSUPPORTED | CMD_SPEC_RANDOM   },
    {"xpending",                    CMD_SPEC_UNSUPPORTED | CMD_SPEC_RANDOM   },
    {"xclaim",                      CMD_SPEC_UNSUPPORTED | CMD_SPEC_RANDOM   },
    {"xautoclaim",                  CMD_SPEC_UNSUPPORTED | CMD_SPEC_RANDOM   },
    {"xinfo",                       CMD_SPEC_UNSUPPORTED                     },
    {"xdel",                        CMD_SPEC_UNSUPPORTED                     },
    {"xtrim",                       CMD_SPEC_UNSUPPORTED | CMD_SPEC_RANDOM   },

 /* Pubsub commands not supported */
    {"subscribe",                   CMD_SPEC_DONT_INTERCEPT                  },
    {"psubscribe",                  CMD_SPEC_DONT_INTERCEPT                  },
    {"unsubscribe",                 CMD_SPEC_DONT_INTERCEPT                  },
    {"punsubscribe",                CMD_SPEC_DONT_INTERCEPT                  },
    {"publish",                     CMD_SPEC_DONT_INTERCEPT                  },
    {"pubsub",                      CMD_SPEC_DONT_INTERCEPT                  },

 /* Admin commands - bypassed */
    {"auth",                        CMD_SPEC_DONT_INTERCEPT                  },
    {"ping",                        CMD_SPEC_DONT_INTERCEPT                  },
    {"hello",                       CMD_SPEC_DONT_INTERCEPT                  },
    {"module",                      CMD_SPEC_DONT_INTERCEPT                  },
    {"client",                      CMD_SPEC_DONT_INTERCEPT                  },
    {"config",                      CMD_SPEC_DONT_INTERCEPT                  },
    {"monitor",                     CMD_SPEC_DONT_INTERCEPT                  },
    {"command",                     CMD_SPEC_DONT_INTERCEPT                  },
    {"shutdown",                    CMD_SPEC_DONT_INTERCEPT                  },
    {"quit",                        CMD_SPEC_DONT_INTERCEPT                  },
    {"slowlog",                     CMD_SPEC_DONT_INTERCEPT                  },
    {"acl",                         CMD_SPEC_DONT_INTERCEPT                  },

 /* RedisRaft Commands */
    {"raft",                        CMD_SPEC_DONT_INTERCEPT                  },
    {"raft.entry",                  CMD_SPEC_DONT_INTERCEPT                  },
    {"raft.cluster",                CMD_SPEC_DONT_INTERCEPT                  },
    {"raft.shardgroup",             CMD_SPEC_DONT_INTERCEPT                  },
    {"raft.node",                   CMD_SPEC_DONT_INTERCEPT                  },
    {"raft.ae",                     CMD_SPEC_DONT_INTERCEPT                  },
    {"raft.requestvote",            CMD_SPEC_DONT_INTERCEPT                  },
    {"raft.snapshot",               CMD_SPEC_DONT_INTERCEPT                  },
    {"raft.debug",                  CMD_SPEC_DONT_INTERCEPT                  },
    {"raft.nodeshutdown",           CMD_SPEC_DONT_INTERCEPT                  },
    {"raft.transfer_leader",        CMD_SPEC_DONT_INTERCEPT                  },
    {"raft.timeout_now",            CMD_SPEC_DONT_INTERCEPT                  },
    {"raft._sort_reply",            CMD_SPEC_DONT_INTERCEPT                  },
    {"raft._reject_random_command", CMD_SPEC_DONT_INTERCEPT                  },
    {"raft.import",                 CMD_SPEC_DONT_INTERCEPT                  },
    {"raft.scan",                   CMD_SPEC_READONLY                        },
    {NULL,                          0                                        }
};

static RedisModuleDict *commandSpecDict = NULL;

/* Look up the specified command in the command spec table and return the
 * CommandSpec associated with it. If create is true, a new entry will
 * be created if one does not exist. Otherwise, NULL is returned.
 */
static CommandSpec *getOrCreateCommandSpec(const RedisModuleString *cmd, bool create)
{
    size_t cmd_len;
    const char *cmd_str = RedisModule_StringPtrLen(cmd, &cmd_len);
    char buf[64];
    char *lcmd = buf;

    if (cmd_len >= sizeof(buf)) {
        lcmd = RedisModule_Alloc(cmd_len + 1);
    }

    for (size_t i = 0; i < cmd_len; i++) {
        lcmd[i] = (char) tolower(cmd_str[i]);
    }
    lcmd[cmd_len] = '\0';

    CommandSpec *cs = RedisModule_DictGetC(commandSpecDict, lcmd, cmd_len, NULL);
    if (!cs && create) {
        cs = RedisModule_Alloc(sizeof(CommandSpec));
        cs->name = RedisModule_Strdup(lcmd);
        cs->flags = 0;

        int ret = RedisModule_DictSetC(commandSpecDict, lcmd, cmd_len, cs);
        RedisModule_Assert(ret == REDISMODULE_OK);
    }

    if (lcmd != buf) {
        RedisModule_Free(lcmd);
    }

    return cs;
}

/* Use COMMAND to fetch all Redis commands and update the CommandSpec. */
static void populateCommandSpecFromRedis(RedisModuleCtx *ctx)
{
    RedisModuleCallReply *reply = NULL;

    reply = RedisModule_Call(ctx, "COMMAND", "");
    RedisModule_Assert(reply != NULL);
    RedisModule_Assert(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ARRAY);

    for (size_t i = 0; i < RedisModule_CallReplyLength(reply); i++) {
        unsigned int cmdspec_flags = 0;

        RedisModuleCallReply *cmd = RedisModule_CallReplyArrayElement(reply, i);
        RedisModule_Assert(cmd != NULL);
        RedisModule_Assert(RedisModule_CallReplyType(cmd) == REDISMODULE_REPLY_ARRAY);

        /* Scan flags (element #3) and map:
         * "readonly" => CMD_SPEC_READONLY
         * "denyoom" => CMD_SPEC_DENYOOM
         */
        const char *readonly_flag = "readonly";
        const char *denyoom_flag = "denyoom";

        RedisModuleCallReply *flags = RedisModule_CallReplyArrayElement(cmd, 2);
        RedisModule_Assert(flags != NULL);
        RedisModule_Assert(RedisModule_CallReplyType(flags) == REDISMODULE_REPLY_ARRAY);

        for (size_t j = 0; j < RedisModule_CallReplyLength(flags); j++) {
            RedisModuleCallReply *flag = RedisModule_CallReplyArrayElement(flags, j);
            RedisModule_Assert(flag != NULL);
            RedisModule_Assert(RedisModule_CallReplyType(flag) == REDISMODULE_REPLY_STRING);

            size_t len;
            const char *str = RedisModule_CallReplyStringPtr(flag, &len);

            if (strncmp(str, readonly_flag, len) == 0) {
                cmdspec_flags |= CMD_SPEC_READONLY;
            } else if (strncmp(str, denyoom_flag, len) == 0) {
                cmdspec_flags |= CMD_SPEC_DENYOOM;
            }
        }

        /* Scan hints (element #8) and map:
         * "nondeterministic_output" => CMD_SPEC_RANDOM
         * "nondeterministic_output_order" => CMD_SPEC_SORT_REPLY
         */
        const char *random_hint = "nondeterministic_output";
        const char *sort_reply_hint = "nondeterministic_output_order";

        RedisModuleCallReply *hints = RedisModule_CallReplyArrayElement(cmd, 7);
        RedisModule_Assert(hints != NULL);
        RedisModule_Assert(RedisModule_CallReplyType(hints) == REDISMODULE_REPLY_ARRAY);

        for (size_t j = 0; j < RedisModule_CallReplyLength(hints); j++) {
            RedisModuleCallReply *hint = RedisModule_CallReplyArrayElement(hints, j);
            RedisModule_Assert(hint != NULL);
            RedisModule_Assert(RedisModule_CallReplyType(hint) == REDISMODULE_REPLY_STRING);

            size_t len;
            const char *str = RedisModule_CallReplyStringPtr(hint, &len);

            if (strncmp(str, random_hint, len) == 0) {
                cmdspec_flags |= CMD_SPEC_RANDOM;
            } else if (strncmp(str, sort_reply_hint, len) == 0) {
                cmdspec_flags |= CMD_SPEC_SORT_REPLY;
            }
        }

        /* Ignore commands with non-default flags */
        if (!cmdspec_flags) {
            continue;
        }

        RedisModuleCallReply *name = RedisModule_CallReplyArrayElement(cmd, 0);
        RedisModule_Assert(name != NULL);
        RedisModule_Assert(RedisModule_CallReplyType(name) == REDISMODULE_REPLY_STRING);

        RedisModuleString *name_str = RedisModule_CreateStringFromCallReply(name);
        CommandSpec *cs = getOrCreateCommandSpec(name_str, true);
        RedisModule_Assert(cs != NULL);
        RedisModule_FreeString(NULL, name_str);

        cs->flags |= cmdspec_flags;
    }

    RedisModule_FreeCallReply(reply);
}

static void freeCommandSpecDict(RedisModuleDict *dict)
{
    CommandSpec *cs;

    if (!dict) {
        return;
    }

    RedisModuleDictIter *it = RedisModule_DictIteratorStartC(dict, "^", NULL, 0);
    while (RedisModule_DictNextC(it, NULL, (void **) &cs) != NULL) {
        RedisModule_Free(cs->name);
        RedisModule_Free(cs);
    }
    RedisModule_DictIteratorStop(it);
    RedisModule_FreeDict(NULL, dict);
}

RRStatus CommandSpecSet(RedisModuleCtx *ctx, const char *ignored_commands)
{
    RedisModuleDict *dict = RedisModule_CreateDict(ctx);

    for (int i = 0; commands[i].name != NULL; i++) {
        CommandSpec *cs = RedisModule_Alloc(sizeof(*cs));
        cs->name = RedisModule_Strdup(commands[i].name);
        cs->flags = commands[i].flags;

        int ret = RedisModule_DictSetC(dict, cs->name, strlen(cs->name), cs);
        if (ret != REDISMODULE_OK) {
            goto error;
        }
    }

    if (ignored_commands) {
        char *tok, *s = NULL;
        char *tmp = RedisModule_Strdup(ignored_commands);

        for (tok = strtok_r(tmp, ",", &s); tok; tok = strtok_r(NULL, ",", &s)) {
            int nokey = 0;
            CommandSpec *cs;

            cs = RedisModule_DictGetC(dict, tok, strlen(tok), &nokey);
            if (!nokey) {
                cs->flags = CMD_SPEC_DONT_INTERCEPT;
                continue;
            }

            cs = RedisModule_Calloc(1, sizeof(*cs));
            cs->name = RedisModule_Strdup(tok);
            cs->flags = CMD_SPEC_DONT_INTERCEPT;

            int ret = RedisModule_DictSetC(dict, tok, strlen(tok), cs);
            if (ret != REDISMODULE_OK) {
                RedisModule_Free(cs->name);
                RedisModule_Free(cs);
                RedisModule_Free(tmp);
                goto error;
            }
        }
        RedisModule_Free(tmp);
    }

    freeCommandSpecDict(commandSpecDict);
    commandSpecDict = dict;
    populateCommandSpecFromRedis(ctx);

    return RR_OK;

error:
    freeCommandSpecDict(dict);
    return RR_ERROR;
}

/* Look up the specified command in the command spec table and return the
 * CommandSpec associated with it, or NULL.
 */
const CommandSpec *CommandSpecGet(const RedisModuleString *cmd)
{
    return getOrCreateCommandSpec(cmd, false);
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
