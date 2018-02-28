#include <string.h>
#include <stdlib.h>
#include <unistd.h>

#include "redisraft.h"

int redis_raft_loglevel = LOGLEVEL_DEBUG;
FILE *redis_raft_logfile;

static redis_raft_t redis_raft = { 0 };
static redis_raft_config_t config;

#define VALID_NODE_ID(x)    ((x) > 0)

int cmd_raft_addnode(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    redis_raft_t *rr = &redis_raft;

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
    node_addr_t node_addr;
    size_t node_addr_len;
    const char *node_addr_str = RedisModule_StringPtrLen(argv[2], &node_addr_len);
    if (!node_addr_parse(node_addr_str, node_addr_len, &node_addr)) {
        RedisModule_ReplyWithError(ctx, "invalid node address");
        return REDISMODULE_OK;
    }

    raft_req_t *req = raft_req_init(ctx, RAFT_REQ_ADDNODE);
    req->r.addnode.id = node_id;
    req->r.addnode.addr = node_addr;
    raft_req_submit(rr, req);

    return REDISMODULE_OK;
}

int cmd_raft_requestvote(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    redis_raft_t *rr = &redis_raft;

    /* RAFT.REQUESTVOTE <src_node_id> <term>:<candidate_id>:<last_log_idx>:<last_log_term> */
    if (argc != 3) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    raft_req_t *req = raft_req_init(ctx, RAFT_REQ_REQUESTVOTE);
    if (rmstring_to_int(argv[1], &req->r.requestvote.src_node_id) == REDISMODULE_ERR) {
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

    raft_req_submit(rr, req);
    return REDISMODULE_OK;

error_cleanup:
    raft_req_free(req);
    return REDISMODULE_OK;
}

int cmd_raft(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    if (argc < 2) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    raft_req_t *req = raft_req_init(ctx, RAFT_REQ_REDISCOMMAND);
    req->r.raft.argc = argc - 1;
    req->r.raft.argv = RedisModule_Alloc((argc - 1) * sizeof(RedisModuleString *));
    
    int i;
    for (i = 0; i < argc - 1; i++) {
        req->r.raft.argv[i] =  argv[i + 1];
        RedisModule_RetainString(ctx, req->r.raft.argv[i]);
    }
    raft_req_submit(&redis_raft, req);

    return REDISMODULE_OK;
}

int cmd_raft_info(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    raft_req_t *req = raft_req_init(ctx, RAFT_REQ_INFO);
    raft_req_submit(&redis_raft, req);
}


int cmd_raft_appendentries(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    redis_raft_t *rr = &redis_raft;

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

    raft_req_t *req = raft_req_init(ctx, RAFT_REQ_APPENDENTRIES);
    if (rmstring_to_int(argv[1], &req->r.appendentries.src_node_id) == REDISMODULE_ERR) {
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

    raft_req_submit(rr, req);
    return REDISMODULE_OK;

error_cleanup:
    raft_req_free(req);
    return REDISMODULE_OK;
}

static int parse_config_args(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, redis_raft_config_t *target)
{
    int i;

    memset(target, 0, sizeof(*target));
    for (i = 0; i < argc; i++) {
        size_t arglen;
        const char *arg = RedisModule_StringPtrLen(argv[i], &arglen);

        /* Handle flags */
        if (arglen == 4 && !memcmp(arg, "init", 4)) {
            target->init = true;
            continue;
        }

        /* Handle arguments */
        const char *eq = memchr(arg, '=', arglen);

        if (!eq) {
            RedisModule_Log(ctx, REDIS_WARNING, "invalid argument: '%.*s'", arglen, arg);
            return REDISMODULE_ERR;
        }

        size_t kwlen = eq - arg;
        size_t vlen = arglen - kwlen - 1;

        char valbuf[vlen + 1];
        memcpy(valbuf, eq + 1, vlen);
        valbuf[vlen] = '\0';

        if (kwlen == 2 && !memcmp(arg, "id", kwlen)) {
            char *errptr;
            unsigned long idval = strtoul(valbuf, &errptr, 10);
            if (*errptr != '\0' || !idval) {
                RedisModule_Log(ctx, REDIS_WARNING, "invalid 'id' value");
                return REDISMODULE_ERR;
            }
            target->id = idval;
        } else if (kwlen == 4 && !memcmp(arg, "node", kwlen)) {
            node_config_t *n = RedisModule_Alloc(sizeof(node_config_t));
            if (!node_config_parse(ctx, valbuf, n)) {
                RedisModule_Free(n);
                RedisModule_Log(ctx, REDIS_WARNING, "invalid node configuration: '%s'", valbuf);
                return REDISMODULE_ERR;
            }
            n->next = target->nodes;
            target->nodes = n;
        } else if (kwlen == 4 && !memcmp(arg, "addr", kwlen)) {
            if (!node_addr_parse(valbuf, vlen, &target->addr)) {
                RedisModule_Log(ctx, REDIS_WARNING, "invalid 'addr' value");
                return REDISMODULE_ERR;
            }
        } else {
            RedisModule_Log(ctx, REDIS_WARNING, "invalid config keyword: '%.*s'", kwlen, arg);
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

static void dump_config(RedisModuleCtx *ctx, redis_raft_config_t *config)
{
    node_config_t *nc;

    RedisModule_Log(ctx, REDIS_NOTICE, "Load time configuration:");
    RedisModule_Log(ctx, REDIS_NOTICE, "Id: %d", config->id);
    
    nc = config->nodes;
    while (nc != NULL) {
        RedisModule_Log(ctx, REDIS_NOTICE, "Node: Id=%d, Addr=%s:%d", 
                nc->id, nc->addr.host, nc->addr.port);
        nc = nc->next;
    }
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    redis_raft_logfile = stderr;

    if (RedisModule_Init(ctx, "redisraft", 1, REDISMODULE_APIVER_1) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    /* Initialize and validate configuration */
    if (parse_config_args(ctx, argv, argc, &config) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }
    if (!VALID_NODE_ID(config.id)) {
        RedisModule_Log(ctx, REDIS_WARNING, "Invalid or missing node id (id= param)");
        return REDISMODULE_ERR;
    }
    if (config.init && !config.addr.port) {
        RedisModule_Log(ctx, REDIS_WARNING, "'init' specified without an 'addr'");
        return REDISMODULE_ERR;
    }

    dump_config(ctx, &config);

    /* Register commands */ 
    if (RedisModule_CreateCommand(ctx, "raft",
                cmd_raft, "write", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "raft.info",
                cmd_raft_info, "admin", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "raft.addnode",
                cmd_raft_addnode, "admin", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "raft.appendentries",
                cmd_raft_appendentries, "write", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "raft.requestvote",
                cmd_raft_requestvote, "write", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

#ifdef USE_COMMAND_FILTER
    if (RedisModule_RegisterCommandFilter(ctx, raftize_commands) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }
#endif

    if (redis_raft_init(ctx, &redis_raft, &config) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    /* Start Raft thread */
    if (redis_raft_start(ctx, &redis_raft) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}
