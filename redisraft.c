#include <string.h>
#include <stdlib.h>
#include <unistd.h>

#include "redisraft.h"

int redis_raft_loglevel = 5;
FILE *redis_raft_logfile;

void raft_module_log(const char *fmt, ...)
{
    va_list ap;
    struct timeval tv;
    struct tm tm;
    char _fmt[strlen(fmt) + 50];
    int n;

    if (!redis_raft_logfile) {
        return;
    }

    gettimeofday(&tv, NULL);
    localtime_r(&tv.tv_sec, &tm);

    n = snprintf(_fmt, sizeof(_fmt), "%u:", (unsigned int) getpid());
    n += strftime(_fmt + n, sizeof(_fmt) - n, "%d %b %H:%M:%S", &tm);
    snprintf(_fmt + n, sizeof(_fmt) - n, ".%03u %s", (unsigned int) tv.tv_usec / 1000, fmt);

    va_start(ap, fmt);
    vfprintf(redis_raft_logfile, _fmt, ap);
    va_end(ap);
    fflush(redis_raft_logfile);
}

RedisRaftCtx redis_raft = { 0 };
static RedisRaftConfig config;

/* This is needed for newer pthread versions to properly link and work */
void *__dso_handle;

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
    NodeAddr node_addr = { 0 };
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
    if (sscanf(tmpstr, "%ld:%d:%ld:%ld",
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
        RedisModule_RetainString(req->ctx, req->r.redis.cmd.argv[i]);
    }
    RaftReqSubmit(&redis_raft, req);

    return REDISMODULE_OK;
}

static int cmdRaftInfo(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    RaftReq *req = RaftReqInit(ctx, RR_INFO);
    RaftReqSubmit(&redis_raft, req);

    return REDISMODULE_OK;
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
    if (sscanf(tmpstr, "%ld:%ld:%ld:%ld",
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
        if (sscanf(tmpstr, "%ld:%d:%d",
                    &e->term,
                    &e->id,
                    &e->type) != 3) {
            RedisModule_ReplyWithError(ctx, "invalid entry");
            goto error_cleanup;
        }

        /* Note: There is an extra allocation that can be avoided here, but
         * we then need to keep the list of retained strings so RaftReqFree()
         * can later release them. TODO later on.
         */
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

    RaftReq *req = RaftReqInit(ctx, RR_LOADSNAPSHOT);
    req->r.loadsnapshot.snapshot = argv[3];
    RedisModule_RetainString(ctx, req->r.loadsnapshot.snapshot);

    RaftReqSubmit(&redis_raft, req);

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
        long long delay = 0;
        if (argc == 3) {
            if (RedisModule_StringToLongLong(argv[2], &delay) != REDISMODULE_OK) {
                RedisModule_ReplyWithError(ctx, "ERR invalid compact delay value");
                return REDISMODULE_OK;
            }
        }
        rr->config->compact_delay = delay;

        RaftReq *req = RaftReqInit(ctx, RR_COMPACT);
        RaftReqSubmit(&redis_raft, req);
    } else {
        RedisModule_ReplyWithError(ctx, "ERR invalid debug subcommand");
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

static int registerRaftCommands(RedisModuleCtx *ctx)
{
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
                cmdRaftRequestVote, "admin", 0, 0, 0) == REDISMODULE_ERR) {
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

    if ((RedisRaftType = RedisModule_CreateDataType(ctx, REDIS_RAFT_DATATYPE_NAME, REDIS_RAFT_DATATYPE_ENCVER,
            &RedisRaftTypeMethods)) == NULL) {
        return REDISMODULE_ERR;
    }

#ifdef USE_COMMAND_FILTER
    if (RedisModule_RegisterCommandFilter(ctx, raftize_commands) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }
#endif

}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    redis_raft_logfile = stdout;

    if (RedisModule_Init(ctx, "redisraft", 1, REDISMODULE_APIVER_1) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    /* Report arguments */
    size_t str_len = 1024;
    char *str = RedisModule_Calloc(1, str_len);
    int i;
    for (i = 0; i < argc; i++) {
        size_t slen;
        const char *s = RedisModule_StringPtrLen(argv[i], &slen);
        str = catsnprintf(str, &str_len, "%s%.*s", i == 0 ? "" : " ", slen, s);
    }

    RedisModule_Log(ctx, REDIS_NOTICE, "RedisRaft starting, arguments: %s", str);
    RedisModule_Free(str);

    /* Initialize and validate configuration */
    if (ConfigInit(ctx, &config) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (ConfigParseArgs(ctx, argv, argc, &config) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (ConfigValidate(ctx, &config) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (registerRaftCommands(ctx) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

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

    RedisModule_Log(ctx, REDIS_VERBOSE, "Raft module loaded, state is '%s'\n",
            getStateStr(&redis_raft));
    return REDISMODULE_OK;
}
