#include <string.h>
#include <stdlib.h>
#include <unistd.h>

#include "redisraft.h"

int redis_raft_loglevel = 5;
FILE *redis_raft_logfile;

static RedisRaftCtx redis_raft = { 0 };
static RedisRaftConfig config;

#define VALID_NODE_ID(x)    ((x) > 0)

static int cmdRaftAddNode(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    RedisRaftCtx *rr = &redis_raft;

    if (argc != 3) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    /* Validate node id */
    long long node_id;
    if (RedisModule_StringToLongLong(argv[1], &node_id) != REDISMODULE_OK ||
        !VALID_NODE_ID(node_id)) {
            RedisModule_ReplyWithError(ctx, "invalid node id");
            return REDISMODULE_OK;
    }

    /* Parse address */
    NodeAddr node_addr;
    size_t node_addr_len;
    const char *node_addr_str = RedisModule_StringPtrLen(argv[2], &node_addr_len);
    if (!NodeAddrParse(node_addr_str, node_addr_len, &node_addr)) {
        RedisModule_ReplyWithError(ctx, "invalid node address");
        return REDISMODULE_OK;
    }

    RaftReq *req = RaftReqInit(ctx, RR_CFGCHANGE_ADDNODE);
    req->r.cfgchange.id = node_id;
    req->r.cfgchange.addr = node_addr;
    RaftReqSubmit(rr, req);

    return REDISMODULE_OK;
}

static int cmdRaftRemoveNode(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    RedisRaftCtx *rr = &redis_raft;

    if (argc != 2) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    /* Validate node id */
    long long node_id;
    if (RedisModule_StringToLongLong(argv[1], &node_id) != REDISMODULE_OK ||
        !VALID_NODE_ID(node_id)) {
            RedisModule_ReplyWithError(ctx, "invalid node id");
            return REDISMODULE_OK;
    }

    /* Also validate it exists */
    if (!raft_get_node(rr->raft, node_id)) {
        RedisModule_ReplyWithError(ctx, "node id does not exist");
        return REDISMODULE_OK;
    }

    RaftReq *req = RaftReqInit(ctx, RR_CFGCHANGE_REMOVENODE);
    req->r.cfgchange.id = node_id;
    RaftReqSubmit(rr, req);

    return REDISMODULE_OK;
}

static int cmdRaftRequestVote(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    RedisRaftCtx *rr = &redis_raft;

    /* RAFT.REQUESTVOTE <src_node_id> <term>:<candidate_id>:<last_log_idx>:<last_log_term> */
    if (argc != 3) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    RaftReq *req = RaftReqInit(ctx, RR_REQUESTVOTE);
    if (RedisModuleStringToInt(argv[1], &req->r.requestvote.src_node_id) == REDISMODULE_ERR) {
        RedisModule_ReplyWithError(ctx, "invalid source node id");
        goto error_cleanup;
    }

    size_t tmplen;
    const char *tmpstr = RedisModule_StringPtrLen(argv[2], &tmplen);
    if (sscanf(tmpstr, "%d:%d:%d:%d",
                &req->r.requestvote.msg.term,
                &req->r.requestvote.msg.candidate_id,
                &req->r.requestvote.msg.last_log_idx,
                &req->r.requestvote.msg.last_log_term) != 4) {
        RedisModule_ReplyWithError(ctx, "invalid message");
        goto error_cleanup;
    }

    RaftReqSubmit(rr, req);
    return REDISMODULE_OK;

error_cleanup:
    RaftReqFree(req);
    return REDISMODULE_OK;
}

static int cmdRaft(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    if (argc < 2) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    RaftReq *req = RaftReqInit(ctx, RR_REDISCOMMAND);
    req->r.redis.cmd.argc = argc - 1;
    req->r.redis.cmd.argv = RedisModule_Alloc((argc - 1) * sizeof(RedisModuleString *));

    int i;
    for (i = 0; i < argc - 1; i++) {
        req->r.redis.cmd.argv[i] =  argv[i + 1];
        RedisModule_RetainString(ctx, req->r.redis.cmd.argv[i]);
    }
    RaftReqSubmit(&redis_raft, req);

    return REDISMODULE_OK;
}

static int cmdRaftInfo(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    RaftReq *req = RaftReqInit(ctx, RR_INFO);
    RaftReqSubmit(&redis_raft, req);
}


static int cmdRaftAppendEntries(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    RedisRaftCtx *rr = &redis_raft;

    /* RAFT.APPENDENTRIES <src_node_id> <term>:<prev_log_idx>:<prev_log_term>:<leader_commit>
     *      <n_entries> {<term>:<id>:<type> <entry>}...
     */

    if (argc < 4) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    long long n_entries;
    if (RedisModule_StringToLongLong(argv[3], &n_entries) != REDIS_OK) {
        RedisModule_ReplyWithError(ctx, "invalid n_entries value");
        return REDISMODULE_OK;
    }
    if (argc != 4 + 2 * n_entries) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    RaftReq *req = RaftReqInit(ctx, RR_APPENDENTRIES);
    if (RedisModuleStringToInt(argv[1], &req->r.appendentries.src_node_id) == REDISMODULE_ERR) {
        RedisModule_ReplyWithError(ctx, "invalid source node id");
        goto error_cleanup;
    }

    int i;
    size_t tmplen;
    const char *tmpstr = RedisModule_StringPtrLen(argv[2], &tmplen);
    if (sscanf(tmpstr, "%d:%d:%d:%d",
                &req->r.appendentries.msg.term,
                &req->r.appendentries.msg.prev_log_idx,
                &req->r.appendentries.msg.prev_log_term,
                &req->r.appendentries.msg.leader_commit) != 4) {
        RedisModule_ReplyWithError(ctx, "invalid message");
        goto error_cleanup;
    }

    req->r.appendentries.msg.n_entries = n_entries;
    if (n_entries > 0) {
        req->r.appendentries.msg.entries = RedisModule_Calloc(n_entries, sizeof(req->r.appendentries.msg.entries[0]));
    }
    for (i = 0; i < n_entries; i++) {
        msg_entry_t *e = &req->r.appendentries.msg.entries[i];

        tmpstr = RedisModule_StringPtrLen(argv[4 + 2*i], &tmplen);
        if (sscanf(tmpstr, "%d:%d:%d",
                    &e->term,
                    &e->id,
                    &e->type) != 3) {
            RedisModule_ReplyWithError(ctx, "invalid entry");
            goto error_cleanup;
        }

        tmpstr = RedisModule_StringPtrLen(argv[5 + 2*i], &tmplen);
        e->data.buf = RedisModule_Alloc(tmplen);
        e->data.len = tmplen;
        memcpy(e->data.buf, tmpstr, tmplen);
    }

    RaftReqSubmit(rr, req);
    return REDISMODULE_OK;

error_cleanup:
    RaftReqFree(req);
    return REDISMODULE_OK;
}

static int processConfigParam(const char *keyword, const char *value,
        RedisRaftConfig *target, bool on_init, char *errbuf, int errbuflen)
{
    /* Parameters we don't accept as config set */
    if (!on_init && (!strcmp(keyword, "id") || !strcmp(keyword, "join") ||
                !strcmp(keyword, "addr") || !strcmp(keyword, "init") ||
                !strcmp(keyword, "raftlog"))) {
        snprintf(errbuf, errbuflen-1, "'%s' only supported at load time", keyword);
        return REDISMODULE_ERR;
    }

    /* Process flags without values */
    if (!strcmp(keyword, "init")) {
        target->init = true;
        return REDISMODULE_OK;
    }

    if (!value) {
        snprintf(errbuf, errbuflen-1, "'%s' requires a value", keyword);
        return REDISMODULE_ERR;
    }

    if (!strcmp(keyword, "id")) {
        char *errptr;
        unsigned long idval = strtoul(value, &errptr, 10);
        if (*errptr != '\0' || !idval) {
            snprintf(errbuf, errbuflen-1, "invalid 'id' value");
            return REDISMODULE_ERR;
        }
        target->id = idval;
    } else if (!strcmp(keyword, "join")) {
        NodeAddrListElement *n = RedisModule_Alloc(sizeof(NodeAddrListElement));
        if (!NodeAddrParse(value, strlen(value), &n->addr)) {
            RedisModule_Free(n);
            snprintf(errbuf, errbuflen-1, "invalid join address '%s'", value);
            return REDISMODULE_ERR;
        }
        n->next = target->join;
        target->join = n;
    } else if (!strcmp(keyword, "addr")) {
        if (!NodeAddrParse(value, strlen(value), &target->addr)) {
            snprintf(errbuf, errbuflen-1, "invalid addr '%s'", value);
            return REDISMODULE_ERR;
        }
    } else if (!strcmp(keyword, "raftlog")) {
        if (target->raftlog) {
            RedisModule_Free(target->raftlog);
        }
        target->raftlog = RedisModule_Strdup(value);
    } else if (!strcmp(keyword, "raft_interval")) {
        char *errptr;
        unsigned long val = strtoul(value, &errptr, 10);
        if (*errptr != '\0' || !val) {
            snprintf(errbuf, errbuflen-1, "invalid 'raft_interval' value");
            return REDISMODULE_ERR;
        }
        target->raft_interval = val;
    } else if (!strcmp(keyword, "request_timeout")) {
        char *errptr;
        unsigned long val = strtoul(value, &errptr, 10);
        if (*errptr != '\0' || val <= 0) {
            snprintf(errbuf, errbuflen-1, "invalid 'request_timeout' value");
            return REDISMODULE_ERR;
        }
        target->request_timeout = val;
    } else if (!strcmp(keyword, "election_timeout")) {
        char *errptr;
        unsigned long val = strtoul(value, &errptr, 10);
        if (*errptr != '\0' || val <= 0) {
            snprintf(errbuf, errbuflen-1, "invalid 'election_timeout' value");
            return REDISMODULE_ERR;
        }
        target->election_timeout = val;
    } else if (!strcmp(keyword, "reconnect_interval")) {
        char *errptr;
        unsigned long val = strtoul(value, &errptr, 10);
        if (*errptr != '\0' || val <= 0) {
            snprintf(errbuf, errbuflen-1, "invalid 'reconnect_interval' value");
            return REDISMODULE_ERR;
        }
        target->reconnect_interval = val;
    } else if (!strcmp(keyword, "max_log_entries")) {
        char *errptr;
        unsigned long val = strtoul(value, &errptr, 10);
        if (*errptr != '\0' || val <= 0) {
            snprintf(errbuf, errbuflen-1, "invalid 'max_log_entries' value");
            return REDISMODULE_ERR;
        }
        target->max_log_entries = val;
    } else {
        snprintf(errbuf, errbuflen-1, "invalid parameter '%s'", keyword);
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}

static int handleConfigSet(RedisModuleCtx *ctx, RedisRaftConfig *config, RedisModuleString **argv, int argc)
{
    size_t key_len;
    const char *key = RedisModule_StringPtrLen(argv[2], &key_len);
    char keybuf[key_len + 1];
    memcpy(keybuf, key, key_len);
    keybuf[key_len] = '\0';

    size_t value_len;
    const char *value = RedisModule_StringPtrLen(argv[3], &value_len);
    char valuebuf[value_len + 1];
    memcpy(valuebuf, value, value_len);
    valuebuf[value_len] = '\0';

    char errbuf[256] = "ERR ";
    if (processConfigParam(keybuf, valuebuf, config, false,
                errbuf + strlen(errbuf), sizeof(errbuf) - strlen(errbuf)) == REDISMODULE_OK) {
        RedisModule_ReplyWithSimpleString(ctx, "OK");
    } else {
        RedisModule_ReplyWithError(ctx, errbuf);
    }

    return REDISMODULE_OK;
}

static void replyConfigStr(RedisModuleCtx *ctx, const char *name, const char *str)
{
    RedisModule_ReplyWithStringBuffer(ctx, name, strlen(name));
    RedisModule_ReplyWithStringBuffer(ctx, str, strlen(str));
}

static void replyConfigInt(RedisModuleCtx *ctx, const char *name, int val)
{
    char str[64];
    snprintf(str, sizeof(str) - 1, "%d", val);

    RedisModule_ReplyWithStringBuffer(ctx, name, strlen(name));
    RedisModule_ReplyWithStringBuffer(ctx, str, strlen(str));
}

static int handleConfigGet(RedisModuleCtx *ctx, RedisRaftConfig *config, RedisModuleString **argv, int argc)
{
    int len = 0;
    size_t pattern_len;
    const char *pattern = RedisModule_StringPtrLen(argv[2], &pattern_len);

    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    if (stringmatch(pattern, "raftlog", 1)) {
        len++;
        replyConfigStr(ctx, "raftlog", config->raftlog);
    }
    if (stringmatch(pattern, "raft_interval", 1)) {
        len++;
        replyConfigInt(ctx, "raft_interval", config->raft_interval);
    }
    if (stringmatch(pattern, "request_timeout", 1)) {
        len++;
        replyConfigInt(ctx, "request_timeout", config->request_timeout);
    }
    if (stringmatch(pattern, "election_timeout", 1)) {
        len++;
        replyConfigInt(ctx, "election_timeout", config->election_timeout);
    }
    if (stringmatch(pattern, "reconnect_interval", 1)) {
        len++;
        replyConfigInt(ctx, "reconnect_interval", config->reconnect_interval);
    }
    if (stringmatch(pattern, "max_log_entries", 1)) {
        len++;
        replyConfigInt(ctx, "max_log_entries", config->max_log_entries);
    }

    RedisModule_ReplySetArrayLength(ctx, len * 2);
    return REDISMODULE_OK;
}

static int cmdRaftConfig(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    RedisRaftCtx *rr = &redis_raft;

    if (argc < 2) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    size_t cmd_len;
    const char *cmd = RedisModule_StringPtrLen(argv[1], &cmd_len);
    if (!strcasecmp(cmd, "SET") && argc >= 4) {
        return handleConfigSet(ctx, rr->config, argv, argc);
    } else if (!strcasecmp(cmd, "GET") && argc == 3) {
        return handleConfigGet(ctx, rr->config, argv, argc);
    } else {
        RedisModule_ReplyWithError(ctx, "ERR Unknown RAFT.CONFIG subcommand or wrong number of arguments");
    }

    return REDISMODULE_OK;
}

static int parseConfigArgs(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, RedisRaftConfig *target)
{
    int i;

    for (i = 0; i < argc; i++) {
        size_t arglen;
        const char *arg = RedisModule_StringPtrLen(argv[i], &arglen);

        char argbuf[arglen + 1];
        memcpy(argbuf, arg, arglen);
        argbuf[arglen] = '\0';

        const char *val = NULL;
        char *eq = strchr(argbuf, '=');
        if (eq != NULL) {
            *eq = '\0';
            val = eq + 1;
        }

        char errbuf[256];
        if (processConfigParam(argbuf, val, target, true,
                    errbuf, sizeof(errbuf)) != REDISMODULE_OK) {
            RedisModule_Log(ctx, REDIS_WARNING, errbuf);
            return REDISMODULE_ERR;
        }
    }

    return REDISMODULE_OK;
}

#ifdef USE_COMMAND_FILTER
void raftize_commands(RedisModuleCtx *ctx, RedisModuleFilteredCommand *cmd)
{
    size_t cmdname_len;
    const char *cmdname = RedisModule_StringPtrLen(cmd->argv[0], &cmdname_len);

    /* Don't process RAFT commands */
    if (cmdname_len >= 4 && (
                !strncasecmp(cmdname, "raft", 4) ||
                !strncasecmp(cmdname, "info", 4))) {
        return;
    }
    if (cmdname_len >= 5 && (
                !strncasecmp(cmdname, "client", 5) ||
                !strncasecmp(cmdname, "config", 5))) {
        return;
    }
    if (cmdname_len == 7 && (
                !strncasecmp(cmdname, "monitor", 7) ||
                !strcasecmp(cmdname, "command"))) {
        return;
    }
    if (cmdname_len == 8 && !strncasecmp(cmdname, "shutdown", 8)) {
        return;
    }
#ifdef USE_UNSAFE_READS
    if (cmdname_len == 3 && !strncasecmp(cmdname, "get", 3)) {
        return;
    }
#endif

    /* Prepend RAFT to the original command */
    cmd->argv = RedisModule_Realloc(cmd->argv, (cmd->argc+1)*sizeof(RedisModuleString *));
    int i;
    for (i = cmd->argc; i > 0; i--) {
        cmd->argv[i] = cmd->argv[i-1];
    }
    cmd->argv[0] = RedisModule_CreateString(ctx, "RAFT", 4);
    cmd->argc++;
}
#endif


static int validateConfig(RedisModuleCtx *ctx, RedisRaftConfig *config)
{
    if (config->init && config->join) {
        RedisModule_Log(ctx, REDIS_WARNING, "'init' and 'join' are mutually exclusive");
        return REDISMODULE_ERR;
    }
    if (config->init) {
        if (!config->addr.port) {
            RedisModule_Log(ctx, REDIS_WARNING, "'init' specified without an 'addr'");
            return REDISMODULE_ERR;
        }
        if (!config->id) {
            RedisModule_Log(ctx, REDIS_WARNING, "'init' requires an 'id'");
            return REDISMODULE_ERR;
        }
    }
    if (config->join) {
        if (!config->addr.port) {
            RedisModule_Log(ctx, REDIS_WARNING, "'join' specified without an 'addr'");
            return REDISMODULE_ERR;
        }
    }
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    redis_raft_logfile = stdout;

    if (RedisModule_Init(ctx, "redisraft", 1, REDISMODULE_APIVER_1) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    /* Initialize and validate configuration */
    memset(&config, 0, sizeof(config));
    config.raft_interval = REDIS_RAFT_DEFAULT_INTERVAL;
    config.request_timeout = REDIS_RAFT_DEFAULT_REQUEST_TIMEOUT;
    config.election_timeout = REDIS_RAFT_DEFAULT_ELECTION_TIMEOUT;
    config.reconnect_interval = REDIS_RAFT_DEFAULT_RECONNECT_INTERVAL;
    config.max_log_entries = REDIS_RAFT_DEFAULT_MAX_LOG_ENTRIES;

    if (parseConfigArgs(ctx, argv, argc, &config) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (validateConfig(ctx, &config) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    /* Register commands */
    if (RedisModule_CreateCommand(ctx, "raft",
                cmdRaft, "write", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "raft.info",
                cmdRaftInfo, "admin", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "raft.config",
                cmdRaftConfig, "admin", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "raft.addnode",
                cmdRaftAddNode, "admin", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "raft.removenode",
                cmdRaftRemoveNode, "admin", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "raft.appendentries",
                cmdRaftAppendEntries, "write", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "raft.requestvote",
                cmdRaftRequestVote, "write", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

#ifdef USE_COMMAND_FILTER
    if (RedisModule_RegisterCommandFilter(ctx, raftize_commands) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }
#endif

    raft_set_heap_functions(RedisModule_Alloc,
                            RedisModule_Calloc,
                            RedisModule_Realloc,
                            RedisModule_Free);
    uv_replace_allocator(RedisModule_Alloc,
                         RedisModule_Realloc,
                         RedisModule_Calloc,
                         RedisModule_Free);

    if (RedisRaftInit(ctx, &redis_raft, &config) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    /* Start Raft thread */
    if (RedisRaftStart(ctx, &redis_raft) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}
