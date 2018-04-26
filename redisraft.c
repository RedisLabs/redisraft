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

static int cmdRaftLoadSnapshot(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    RedisRaftCtx *rr = &redis_raft;

    if (argc != 4) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    long long term;
    long long idx;
    if (RedisModule_StringToLongLong(argv[1], &term) != REDISMODULE_OK ||
        RedisModule_StringToLongLong(argv[2], &idx) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx, "-ERR invalid numeric values");
        return REDISMODULE_OK;
    }

    /* Early bail out if we already have this */
    if (raft_get_current_term(rr->raft) == term &&
        idx <= raft_get_current_idx(rr->raft)) {
        RedisModule_ReplyWithLongLong(ctx, 0);
        return REDISMODULE_OK;
    }

    size_t addrlen;
    const char *addr = RedisModule_StringPtrLen(argv[3], &addrlen);
    RaftReq *req = RaftReqInit(NULL, RR_LOADSNAPSHOT);
    if (!NodeAddrParse(addr, addrlen, &req->r.loadsnapshot.addr)) {
        RaftReqFree(req);
        RedisModule_ReplyWithError(ctx, "-ERR invalid address");
        return REDISMODULE_OK;
    }

    /* Unlike all other RAFT commands, we reply to this one immediately
     * to avoid the -UNBLOCKED error which result from changing stat.e
     */
    if (rr->loading_snapshot) {
        RedisModule_ReplyWithError(ctx, "-LOADING loading snapshot");
    } else {
        RedisModule_ReplyWithLongLong(ctx, 1);
        RaftReqSubmit(&redis_raft, req);
    }

    return REDISMODULE_OK;
}

static int cmdRaftDebug(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    RedisRaftCtx *rr = &redis_raft;

    if (argc < 2) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    size_t cmdlen;
    const char *cmdstr = RedisModule_StringPtrLen(argv[1], &cmdlen);
    char cmd[cmdlen+1];

    memcpy(cmd, cmdstr, cmdlen);
    cmd[cmdlen] = '\0';

    if (!strcasecmp(cmd, "compact")) {
        RaftReq *req = RaftReqInit(ctx, RR_COMPACT);
        RaftReqSubmit(&redis_raft, req);
    } else {
        RedisModule_ReplyWithError(ctx, "ERR invalid debug subcommand");
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
    config.persist = true;

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

    if (RedisModule_CreateCommand(ctx, "raft.loadsnapshot",
                cmdRaftLoadSnapshot, "admin", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "raft.debug",
                cmdRaftDebug, "admin", 0, 0, 0) == REDISMODULE_ERR) {
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
