/*
 * Copyright Redis Ltd. 2021 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "redisraft.h"

#include <ctype.h>
#include <string.h>
#include <strings.h>

static const CommandSpec clientCommands[] = {
    {"unblock", 0},
    {NULL,      0},
};

static const CommandSpec commands[] = {
  /* Core Redis Commands */
    {"time",                        CMD_SPEC_DONT_INTERCEPT | CMD_SPEC_RANDOM    },
    {"sync",                        CMD_SPEC_UNSUPPORTED                         },
    {"psync",                       CMD_SPEC_UNSUPPORTED                         },
    {"reset",                       CMD_SPEC_UNSUPPORTED                         },
    {"bgrewriteaof",                CMD_SPEC_UNSUPPORTED                         },
    {"slaveof",                     CMD_SPEC_UNSUPPORTED                         },
    {"replicaof",                   CMD_SPEC_UNSUPPORTED                         },
    {"debug",                       CMD_SPEC_UNSUPPORTED                         },
    {"save",                        CMD_SPEC_UNSUPPORTED                         },
    {"bgsave",                      CMD_SPEC_UNSUPPORTED                         },

    {"eval",                        CMD_SPEC_SCRIPTS                             },
    {"evalsha",                     CMD_SPEC_SCRIPTS                             },
    {"eval_ro",                     CMD_SPEC_SCRIPTS                             },
    {"evalsha_ro",                  CMD_SPEC_SCRIPTS                             },
    {"fcall",                       CMD_SPEC_SCRIPTS                             },
    {"fcall_ro",                    CMD_SPEC_SCRIPTS                             },

 /* Blocking commands not supported */
    {"brpop",                       CMD_SPEC_BLOCKING                            },
    {"brpoplpush",                  CMD_SPEC_BLOCKING                            },
    {"blmove",                      CMD_SPEC_BLOCKING                            },
    {"blpop",                       CMD_SPEC_BLOCKING                            },
    {"blmpop",                      CMD_SPEC_BLOCKING                            },
    {"bzpopmin",                    CMD_SPEC_BLOCKING                            },
    {"bzpopmax",                    CMD_SPEC_BLOCKING                            },
    {"bzmpop",                      CMD_SPEC_BLOCKING                            },

 /* Stream commands not supported */
    {"xadd",                        CMD_SPEC_UNSUPPORTED | CMD_SPEC_RANDOM       },
    {"xrange",                      CMD_SPEC_UNSUPPORTED                         },
    {"xrevrange",                   CMD_SPEC_UNSUPPORTED                         },
    {"xlen",                        CMD_SPEC_UNSUPPORTED                         },
    {"xread",                       CMD_SPEC_UNSUPPORTED                         },
    {"xreadgroup",                  CMD_SPEC_UNSUPPORTED                         },
    {"xgroup",                      CMD_SPEC_UNSUPPORTED                         },
    {"xsetid",                      CMD_SPEC_UNSUPPORTED                         },
    {"xack",                        CMD_SPEC_UNSUPPORTED | CMD_SPEC_RANDOM       },
    {"xpending",                    CMD_SPEC_UNSUPPORTED | CMD_SPEC_RANDOM       },
    {"xclaim",                      CMD_SPEC_UNSUPPORTED | CMD_SPEC_RANDOM       },
    {"xautoclaim",                  CMD_SPEC_UNSUPPORTED | CMD_SPEC_RANDOM       },
    {"xinfo",                       CMD_SPEC_UNSUPPORTED                         },
    {"xdel",                        CMD_SPEC_UNSUPPORTED                         },
    {"xtrim",                       CMD_SPEC_UNSUPPORTED | CMD_SPEC_RANDOM       },

 /* Pubsub commands not supported */
    {"subscribe",                   CMD_SPEC_DONT_INTERCEPT                      },
    {"psubscribe",                  CMD_SPEC_DONT_INTERCEPT                      },
    {"unsubscribe",                 CMD_SPEC_DONT_INTERCEPT                      },
    {"punsubscribe",                CMD_SPEC_DONT_INTERCEPT                      },
    {"publish",                     CMD_SPEC_DONT_INTERCEPT                      },
    {"pubsub",                      CMD_SPEC_DONT_INTERCEPT                      },

 /* Admin commands - bypassed */
    {"auth",                        CMD_SPEC_DONT_INTERCEPT                      },
    {"ping",                        CMD_SPEC_DONT_INTERCEPT                      },
    {"hello",                       CMD_SPEC_DONT_INTERCEPT                      },
    {"module",                      CMD_SPEC_DONT_INTERCEPT                      },
    {"config",                      CMD_SPEC_DONT_INTERCEPT                      },
    {"monitor",                     CMD_SPEC_DONT_INTERCEPT                      },
    {"command",                     CMD_SPEC_DONT_INTERCEPT                      },
    {"shutdown",                    CMD_SPEC_DONT_INTERCEPT                      },
    {"quit",                        CMD_SPEC_DONT_INTERCEPT                      },
    {"slowlog",                     CMD_SPEC_DONT_INTERCEPT                      },
    {"acl",                         CMD_SPEC_DONT_INTERCEPT                      },

    {"multi",                       CMD_SPEC_MULTI                               },

 /* RedisRaft Commands */
    {"raft",                        CMD_SPEC_DONT_INTERCEPT                      },
    {"raft.entry",                  CMD_SPEC_DONT_INTERCEPT                      },
    {"raft.cluster",                CMD_SPEC_DONT_INTERCEPT                      },
    {"raft.shardgroup",             CMD_SPEC_DONT_INTERCEPT                      },
    {"raft.node",                   CMD_SPEC_DONT_INTERCEPT                      },
    {"raft.ae",                     CMD_SPEC_DONT_INTERCEPT                      },
    {"raft.requestvote",            CMD_SPEC_DONT_INTERCEPT                      },
    {"raft.snapshot",               CMD_SPEC_DONT_INTERCEPT                      },
    {"raft.debug",                  CMD_SPEC_DONT_INTERCEPT                      },
    {"raft.nodeshutdown",           CMD_SPEC_DONT_INTERCEPT                      },
    {"raft.transfer_leader",        CMD_SPEC_DONT_INTERCEPT                      },
    {"raft.timeout_now",            CMD_SPEC_DONT_INTERCEPT                      },
    {"raft._sort_reply",            CMD_SPEC_DONT_INTERCEPT                      },
    {"raft._reject_random_command", CMD_SPEC_DONT_INTERCEPT                      },
    {"raft.import",                 CMD_SPEC_DONT_INTERCEPT                      },
    {"raft.scan",                   CMD_SPEC_READONLY                            },
    {"client",                      CMD_SPEC_SUBCOMMAND | CMD_SPEC_DONT_INTERCEPT},
    {NULL,                          0                                            }
};

/* Look up the specified command in the command spec table and return the
 * CommandSpec associated with it. If create is true, a new entry will
 * be created if one does not exist. Otherwise, NULL is returned.
 */
static CommandSpec *getOrCreateCommandSpec(CommandSpecTable *cmd_spec_table, const RedisModuleString *cmd, bool create)
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

    CommandSpec *cs = CommandSpecTableGetC(cmd_spec_table, lcmd, cmd_len, NULL);
    if (!cs && create) {
        cs = RedisModule_Alloc(sizeof(CommandSpec));
        cs->name = RedisModule_Strdup(lcmd);
        cs->flags = 0;

        int ret = CommandSpecTableSetC(cmd_spec_table, lcmd, cmd_len, cs);
        RedisModule_Assert(ret == REDISMODULE_OK);
    }

    if (lcmd != buf) {
        RedisModule_Free(lcmd);
    }

    return cs;
}

/* Use COMMAND to fetch all Redis commands and update the CommandSpec. */
static void populateCommandSpecFromRedis(RedisModuleCtx *ctx, CommandSpecTable *cmd_spec_table)
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
         */
        const char *readonly_flag = "readonly";

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
        CommandSpec *cs = getOrCreateCommandSpec(cmd_spec_table, name_str, true);
        RedisModule_Assert(cs != NULL);
        RedisModule_FreeString(NULL, name_str);

        cs->flags |= cmdspec_flags;
    }

    RedisModule_FreeCallReply(reply);
}

static void updateIgnoredCommands(CommandSpecTable *cmd_spec_table, const char *commands_str)
{
    char *tok, *s = NULL;
    char *tmp = RedisModule_Strdup(commands_str);

    for (tok = strtok_r(tmp, ",", &s); tok; tok = strtok_r(NULL, ",", &s)) {
        int nokey = 0;
        CommandSpec *cs;

        cs = CommandSpecTableGetC(cmd_spec_table, tok, strlen(tok), &nokey);
        if (!nokey) {
            cs->flags |= CMD_SPEC_DONT_INTERCEPT;
            continue;
        }

        cs = RedisModule_Calloc(1, sizeof(*cs));
        cs->name = RedisModule_Strdup(tok);
        cs->flags = CMD_SPEC_DONT_INTERCEPT;

        int ret = CommandSpecTableSetC(cmd_spec_table, tok, strlen(tok), cs);
        RedisModule_Assert(ret == REDISMODULE_OK);
    }
    RedisModule_Free(tmp);
}

static void buildCommandSpecTable(RedisModuleCtx *ctx, CommandSpecTable *cmd_spec_table, const CommandSpec *command_list, const char *ignored_commands, bool from_redis)
{
    RedisModule_Assert(CommandSpecTableSize(cmd_spec_table) == 0);

    for (int i = 0; command_list[i].name != NULL; i++) {
        CommandSpec *cs = RedisModule_Alloc(sizeof(*cs));
        cs->name = RedisModule_Strdup(command_list[i].name);
        cs->flags = command_list[i].flags;

        // Only to validate commands has no duplication
        int nokey = 0;
        CommandSpecTableGetC(cmd_spec_table, cs->name, strlen(cs->name), &nokey);
        RedisModule_Assert(nokey);
        RRStatus ret = CommandSpecTableSetC(cmd_spec_table, cs->name, strlen(cs->name), cs);
        RedisModule_Assert(ret == RR_OK);
    }

    if (ignored_commands) {
        updateIgnoredCommands(cmd_spec_table, ignored_commands);
    }
    if (from_redis) {
        populateCommandSpecFromRedis(ctx, cmd_spec_table);
    }
}

static void initCommandSpecTableInternals(CommandSpecTable *cmd_spec_table)
{
    if (cmd_spec_table->table) {
        CommandSpecTableClear(cmd_spec_table);
    }
    cmd_spec_table->table = RedisModule_CreateDict(NULL);
}

/* Rebuild the command spec table with raft and redis commands spec and update ignored cmds spec, if ignored_commands != NULL */
void CommandSpecTableRebuild(RedisModuleCtx *ctx, CommandSpecTable *cmd_spec_table, const char *ignored_commands)
{
    RedisModule_Assert(cmd_spec_table->table);

    initCommandSpecTableInternals(cmd_spec_table);
    buildCommandSpecTable(ctx, cmd_spec_table, commands, ignored_commands, true);
}

/* Init the command spec table to contain raft and redis commands spec */
void CommandSpecTableInit(RedisModuleCtx *ctx, CommandSpecTable **cmd_spec_table)
{
    *cmd_spec_table = RedisModule_Calloc(1, sizeof(cmd_spec_table));
    initCommandSpecTableInternals(*cmd_spec_table);
    buildCommandSpecTable(ctx, *cmd_spec_table, commands, NULL, true);
}

void SubCommandsSpecTableInit(RedisModuleCtx *ctx, RedisModuleDict **subcommandspec_dict)
{
    *subcommandspec_dict = RedisModule_CreateDict(ctx);

    /* can duplicate for other command containers, for now just the client command container */
    CommandSpecTable *clientSubCommandTable = RedisModule_Calloc(1, sizeof(CommandSpecTable));

    initCommandSpecTableInternals(clientSubCommandTable);
    buildCommandSpecTable(ctx, clientSubCommandTable, clientCommands, NULL, false);
    RedisModule_DictSetC(*subcommandspec_dict, "client", strlen("client"), clientSubCommandTable);
}

void FreeSubCommandSpecTables(RedisRaftCtx *rr, RedisModuleDict *tables)
{
    CommandSpecTable *subCommand;
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(tables, "^", NULL, 0);
    while (RedisModule_DictNextC(iter, NULL, (void **) &subCommand) != NULL) {
        CommandSpecTableClear(subCommand);
        RedisModule_Free(subCommand);
    }
    RedisModule_DictIteratorStop(iter);
    RedisModule_FreeDict(rr->ctx, tables);
}

/* Clear the command spec table */
void CommandSpecTableClear(CommandSpecTable *cmd_spec_table)
{
    if (!cmd_spec_table->table) {
        return;
    }

    RedisModuleDictIter *it = RedisModule_DictIteratorStartC(cmd_spec_table->table, "^", NULL, 0);
    CommandSpec *cs;
    while (RedisModule_DictNextC(it, NULL, (void **) &cs) != NULL) {
        RedisModule_Free(cs->name);
        RedisModule_Free(cs);
    }
    RedisModule_DictIteratorStop(it);

    RedisModule_FreeDict(NULL, cmd_spec_table->table);
    cmd_spec_table->table = NULL;
}

/* Return the command spec table size */
uint64_t CommandSpecTableSize(CommandSpecTable *cmd_spec_table)
{
    RedisModule_Assert(cmd_spec_table->table);
    return RedisModule_DictSize(cmd_spec_table->table);
}

/* Return the CommandSpec stored at the specified key. The function returns NULL
 * both in the case the key does not exist, or if you actually stored
 * NULL at key. So, optionally, if the 'nokey' pointer is not NULL, it will
 * be set by reference to 1 if the key does not exist, or to 0 if the key
 * exists. */
CommandSpec *CommandSpecTableGetC(CommandSpecTable *cmd_spec_table, void *key, size_t keylen, int *nokey)
{
    return RedisModule_DictGetC(cmd_spec_table->table, key, keylen, nokey);
}

/* Store the specified key into the command spec table, setting its value to the
 * CommandSpec* 'cs'. If the key was added with success, since it did not
 * already exist, RR_OK is returned. Otherwise if the key already
 * exists the function returns RR_ERROR. */
RRStatus CommandSpecTableSetC(CommandSpecTable *cmd_spec_table, void *key, size_t keylen, CommandSpec *cs)
{
    int ret = RedisModule_DictSetC(cmd_spec_table->table, key, keylen, cs);
    return ret == REDISMODULE_OK ? RR_OK : RR_ERROR;
}

/* Look up the specified command in the command spec table and return the
 * CommandSpec associated with it, or NULL.
 */
int CommandSpecTableGetFlags(CommandSpecTable *cmd_spec_table, RedisModuleDict *sub_command_tables, const RedisModuleString *cmd, const RedisModuleString *subcmd)
{
    CommandSpec *cs = getOrCreateCommandSpec(cmd_spec_table, cmd, false);

    if (!cs) {
        return -1;
    }

    int flags = cs->flags;

    if (!(cs->flags & CMD_SPEC_SUBCOMMAND) || subcmd == NULL) {
        return flags;
    }

    /* flags are now for subcommand, so mask it out of flags */
    flags &= ~CMD_SPEC_SUBCOMMAND; /* flags is now the default set of flags for all subcommands */

    CommandSpecTable *sub_cmd_spec_table = RedisModule_DictGetC(sub_command_tables, cs->name, strlen(cs->name), NULL);
    if (sub_cmd_spec_table == NULL) {
        return flags; /* no table, return default set of flags for subcommand */
    }

    CommandSpec *sub_cs = getOrCreateCommandSpec(sub_cmd_spec_table, subcmd, false);
    if (sub_cs == NULL) {
        return flags; /* command not specified in table, return default set of flags */
    }

    return sub_cs->flags; /* command is in table, return its flags */
}

/* For a given RaftRedisCommandArray, return a flags value that represents
 * the aggregate flags of all commands. If a command is not listed in the
 * command spec table, use default_flags.
 */
unsigned int CommandSpecTableGetAggregateFlags(CommandSpecTable *cmd_spec_table, RedisModuleDict *sub_command_tables, RaftRedisCommandArray *array, unsigned int default_flags)
{
    unsigned int flags = 0;
    for (int i = 0; i < array->len; i++) {
        RedisModuleString *cmd = array->commands[i]->argv[0];
        RedisModuleString *subcmd = NULL;
        if (array->commands[i]->argc > 1) {
            subcmd = array->commands[i]->argv[1];
        }
        int flag = CommandSpecTableGetFlags(cmd_spec_table, sub_command_tables, cmd, subcmd);
        if (flag != -1) {
            flags |= flag;
        } else {
            flags |= default_flags;
        }
    }

    return flags;
}
