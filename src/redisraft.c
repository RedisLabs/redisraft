/*
 * This file is part of RedisRaft.
 *
 * Copyright (c) 2020-2021 Redis Ltd.
 *
 * RedisRaft is licensed under the Redis Source Available License (RSAL).
 */

#include "redisraft.h"

#include "entrycache.h"

#include <dlfcn.h>
#include <inttypes.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

RedisModuleCtx *redisraft_log_ctx = NULL;

int redisraft_trace = TRACE_OFF;
int redisraft_loglevel = LOG_LEVEL_NOTICE;

const char *redisraft_loglevels[] = {
    REDISMODULE_LOGLEVEL_DEBUG,
    REDISMODULE_LOGLEVEL_VERBOSE,
    REDISMODULE_LOGLEVEL_NOTICE,
    REDISMODULE_LOGLEVEL_WARNING,
};

int redisraft_loglevel_enums[] = {
    LOG_LEVEL_DEBUG,
    LOG_LEVEL_VERBOSE,
    LOG_LEVEL_NOTICE,
    LOG_LEVEL_WARNING,
    LOG_LEVEL_COUNT,
};

RedisRaftCtx redis_raft = {0};

/* This is needed for newer pthread versions to properly link and work */
#ifdef LINUX
void *__dso_handle;
#endif

#define VALID_NODE_ID(x) ((x) > 0)

static void redirectCommand(RedisRaftCtx *rr, RedisModuleCtx *ctx, Node *leader);
static void handleNonLeaderCommand(RedisRaftCtx *rr, RedisModuleCtx *ctx, Node *leader, RaftRedisCommandArray *cmds);

/* Parse a node address from a RedisModuleString */
static RRStatus getNodeAddrFromArg(RedisModuleCtx *ctx, RedisModuleString *arg, NodeAddr *addr)
{
    size_t node_addr_len;
    const char *node_addr_str = RedisModule_StringPtrLen(arg, &node_addr_len);
    if (!NodeAddrParse(node_addr_str, node_addr_len, addr)) {
        RedisModule_ReplyWithError(ctx, "invalid node address");
        return RR_ERROR;
    }

    return RR_OK;
}

/* RAFT.NODE ADD [id] [address:port]
 *   Add a new node to the cluster.  The [id] can be an explicit non-zero value,
 *   or zero to let the cluster choose one.
 * Reply:
 *   -NOCLUSTER ||
 *   -LOADING ||
 *   -CLUSTERDOWN ||
 *   -MOVED <slot> <addr>:<port> ||
 *   *2
 *   :<new node id>
 *   :<dbid>
 *
 * RAFT.NODE REMOVE [id]
 *   Remove an existing node from the cluster.
 * Reply:
 *   -NOCLUSTER ||
 *   -LOADING ||
 *   -CLUSTERDOWN ||
 *   -MOVED <slot> <addr>:<port> ||
 *   +OK
 */
static int cmdRaftNode(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    RedisRaftCtx *rr = &redis_raft;

    if (argc < 2) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    if (checkRaftState(rr, ctx) == RR_ERROR) {
        return REDISMODULE_OK;
    }

    if (!raft_is_leader(rr->raft)) {
        Node *leader = getLeaderNodeOrReply(rr, ctx);
        if (leader) {
            redirectCommand(rr, ctx, leader);
        }
        return REDISMODULE_OK;
    }

    size_t cmd_len;
    const char *cmd = RedisModule_StringPtrLen(argv[1], &cmd_len);

    if (!strncasecmp(cmd, "ADD", cmd_len)) {
        if (argc != 4) {
            RedisModule_WrongArity(ctx);
            return REDISMODULE_OK;
        }

        /* Validate node id */
        long long node_id;
        if (RedisModule_StringToLongLong(argv[2], &node_id) != REDISMODULE_OK ||
            (node_id && !VALID_NODE_ID(node_id))) {
            RedisModule_ReplyWithError(ctx, "invalid node id");
            return REDISMODULE_OK;
        }

        if (hasNodeIdBeenUsed(rr, (raft_node_id_t) node_id)) {
            RedisModule_ReplyWithError(ctx, "node id has already been used in this cluster");
            return REDISMODULE_OK;
        }

        if (node_id == 0) {
            node_id = makeRandomNodeId(rr);
        }

        RaftCfgChange cfg = {
            .id = (raft_node_id_t) node_id,
        };

        /* Parse address */
        if (getNodeAddrFromArg(ctx, argv[3], &cfg.addr) == RR_ERROR) {
            /* Error already produced */
            return REDISMODULE_OK;
        }

        raft_entry_resp_t resp;
        RaftReq *req = RaftReqInit(ctx, RR_CFGCHANGE_ADDNODE);

        raft_entry_req_t *entry = raft_entry_new(sizeof(cfg));
        entry->id = rand();
        entry->type = RAFT_LOGTYPE_ADD_NONVOTING_NODE;
        memcpy(entry->data, &cfg, sizeof(cfg));
        entryAttachRaftReq(rr, entry, req);

        int e = raft_recv_entry(rr->raft, entry, &resp);
        if (e != 0) {
            entryDetachRaftReq(rr, entry);
            replyRaftError(req->ctx, e);
            raft_entry_release(entry);
            RaftReqFree(req);
            return REDISMODULE_OK;
        }

        raft_entry_release(entry);

    } else if (!strncasecmp(cmd, "REMOVE", cmd_len)) {
        if (argc != 3) {
            RedisModule_WrongArity(ctx);
            return REDISMODULE_OK;
        }

        long long node_id;
        if (RedisModule_StringToLongLong(argv[2], &node_id) != REDISMODULE_OK ||
            !VALID_NODE_ID(node_id)) {
            RedisModule_ReplyWithError(ctx, "invalid node id");
            return REDISMODULE_OK;
        }

        /* Validate it exists */
        if (!raft_get_node(rr->raft, (raft_node_id_t) node_id)) {
            RedisModule_ReplyWithError(ctx, "node id does not exist");
            return REDISMODULE_OK;
        }

        RaftCfgChange cfg = {
            .id = (raft_node_id_t) node_id,
        };

        raft_entry_resp_t resp;
        RaftReq *req = RaftReqInit(ctx, RR_CFGCHANGE_REMOVENODE);

        raft_entry_req_t *entry = raft_entry_new(sizeof(cfg));
        entry->id = rand();
        entry->type = RAFT_LOGTYPE_REMOVE_NODE;
        memcpy(entry->data, &cfg, sizeof(cfg));
        entryAttachRaftReq(rr, entry, req);

        int e = raft_recv_entry(rr->raft, entry, &resp);
        if (e != 0) {
            entryDetachRaftReq(rr, entry);
            replyRaftError(req->ctx, e);
            raft_entry_release(entry);
            RaftReqFree(req);
            return REDISMODULE_OK;
        }

        raft_entry_release(entry);

        /* Special case, we are the only node and our node has been removed */
        if (cfg.id == raft_get_nodeid(rr->raft) &&
            raft_get_num_voting_nodes(rr->raft) == 0) {
            shutdownAfterRemoval(rr);
        }
    } else {
        RedisModule_ReplyWithError(ctx, "RAFT.NODE supports ADD / REMOVE only");
    }

    return REDISMODULE_OK;
}

/* RAFT.TRANSFER_LEADER [target_node_id]
  *   Attempt to transfer raft cluster leadership to targeted node
 * Reply:
 *   -NOCLUSTER ||
 *   -LOADING ||
 *   -ERR
 *   +OK
 */
static int cmdRaftTransferLeader(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    RedisRaftCtx *rr = &redis_raft;
    raft_node_id_t target = RAFT_NODE_ID_NONE;

    if (argc > 2) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    if (argc == 2) {
        if (RedisModuleStringToInt(argv[1], &target) == REDISMODULE_ERR) {
            RedisModule_ReplyWithError(ctx, "invalid target node id");
            return REDISMODULE_OK;
        }
    }

    if (checkRaftState(rr, ctx) == RR_ERROR) {
        return REDISMODULE_OK;
    }

    int err = raft_transfer_leader(rr->raft, target, 0);
    if (err != 0) {
        replyRaftError(ctx, err);
        return REDISMODULE_OK;
    }

    rr->transfer_req = RaftReqInit(ctx, RR_TRANSFER_LEADER);

    return REDISMODULE_OK;
}

/* RAFT.TIMEOUT_NOW
 *   instruct this node to force an election
 * Reply:
 *   -NOCLUSTER ||
 *   -LOADING ||
 *   +OK
 */
static int cmdRaftTimeoutNow(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    RedisRaftCtx *rr = &redis_raft;

    if (argc != 1) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    if (checkRaftState(rr, ctx) == RR_ERROR) {
        return REDISMODULE_OK;
    }

    raft_timeout_now(rr->raft);
    RedisModule_ReplyWithSimpleString(ctx, "OK");

    return REDISMODULE_OK;
}

/* RAFT.REQUESTVOTE [target_node_id] [src_node_id] [prevote]:[term]:[candidate_id]:[last_log_idx]:[last_log_term]
 *   Request a node's vote (per Raft paper).
 * Reply:
 *   -NOCLUSTER ||
 *   -LOADING ||
 *   *2
 *   :<term>
 *   :<granted> (0 or 1)
 */
static int cmdRaftRequestVote(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    RedisRaftCtx *rr = &redis_raft;

    if (argc != 4) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    if (checkRaftState(rr, ctx) == RR_ERROR) {
        return REDISMODULE_OK;
    }

    int target_node_id;
    if (RedisModuleStringToInt(argv[1], &target_node_id) == REDISMODULE_ERR ||
        target_node_id != rr->config.id) {

        RedisModule_ReplyWithError(ctx, "invalid or incorrect target node id");
        return REDISMODULE_OK;
    }

    int src_node_id;
    if (RedisModuleStringToInt(argv[2], &src_node_id) == REDISMODULE_ERR) {
        RedisModule_ReplyWithError(ctx, "invalid source node id");
        return REDISMODULE_OK;
    }

    const char *tmpstr = RedisModule_StringPtrLen(argv[3], NULL);
    raft_requestvote_req_t req = {0};

    if (sscanf(tmpstr, "%d:%ld:%d:%ld:%ld",
               &req.prevote,
               &req.term,
               &req.candidate_id,
               &req.last_log_idx,
               &req.last_log_term) != 5) {
        RedisModule_ReplyWithError(ctx, "invalid message");
        return REDISMODULE_OK;
    }

    raft_requestvote_resp_t resp = {0};
    raft_node_t *node = raft_get_node(rr->raft, src_node_id);

    if (raft_recv_requestvote(rr->raft, node, &req, &resp) != 0) {
        RedisModule_ReplyWithError(ctx, "ERR operation failed");
        return REDISMODULE_OK;
    }

    RedisModule_ReplyWithArray(ctx, 4);
    RedisModule_ReplyWithLongLong(ctx, resp.prevote);
    RedisModule_ReplyWithLongLong(ctx, resp.request_term);
    RedisModule_ReplyWithLongLong(ctx, resp.term);
    RedisModule_ReplyWithLongLong(ctx, resp.vote_granted);

    return REDISMODULE_OK;
}

static void handleReadOnlyCommand(void *arg, int can_read)
{
    RaftReq *req = arg;

    if (!can_read) {
        RedisModule_ReplyWithError(req->ctx, "TIMEOUT no quorum for read");
        goto exit;
    }

    RaftExecuteCommandArray(&redis_raft, req->ctx, req->ctx, &req->r.redis.cmds);

exit:
    RaftReqFree(req);
}

static void handleInfoCommand(RedisRaftCtx *rr,
                              RedisModuleCtx *ctx, RaftRedisCommand *cmd)
{
    RedisModuleCallReply *reply;

    /* Skip "INFO" string */
    int argc = cmd->argc - 1;
    RedisModuleString **argv = cmd->argc == 1 ? NULL : &cmd->argv[1];

    enterRedisModuleCall();
    reply = RedisModule_Call(ctx, "INFO", "v", argv, argc);
    exitRedisModuleCall();

    size_t info_len;
    const char *info = RedisModule_CallReplyStringPtr(reply, &info_len);

    char *pos = strstr(info, "cluster_enabled:0");
    if (pos) {
        /* Always return cluster_enabled:1 */
        *(pos + strlen("cluster_enabled:")) = '1';
    }

    RedisModule_ReplyWithStringBuffer(ctx, info, info_len);
    RedisModule_FreeCallReply(reply);
}

#define MIGRATE_CMD_OPTIONAL_ARG_IDX 6

typedef struct MigrationInfo {
    char username[MAX_AUTH_STRING_ARG_LENGTH + 1];
    char password[MAX_AUTH_STRING_ARG_LENGTH + 1];
} MigrationInfo;

static MigrationInfo *extractMigrationInfo(RaftRedisCommand *cmd)
{
    MigrationInfo *ret = RedisModule_Calloc(1, sizeof(MigrationInfo));

    /* default values if auth isn't specified */
    strncpy(ret->username, "default", sizeof(ret->username));

    int idx;
    for (idx = MIGRATE_CMD_OPTIONAL_ARG_IDX; idx < cmd->argc; idx++) {
        size_t auth_str_len;
        const char *auth_str = RedisModule_StringPtrLen(cmd->argv[idx], &auth_str_len);
        if (auth_str_len == 5 && !strncasecmp("AUTH2", auth_str, 5)) {
            if (idx + 2 > cmd->argc) {
                LOG_WARNING("couldn't parse username/password");
                goto fail;
            }

            size_t username_len;
            const char *username = RedisModule_StringPtrLen(cmd->argv[idx + 1], &username_len);
            if (username_len > MAX_AUTH_STRING_ARG_LENGTH) {
                LOG_WARNING("username is too long: limited to 255 characters");
                goto fail;
            }
            strncpy(ret->username, username, username_len);

            size_t password_len;
            const char *password = RedisModule_StringPtrLen(cmd->argv[idx + 2], &password_len);
            if (password_len > MAX_AUTH_STRING_ARG_LENGTH) {
                LOG_WARNING("password is too long: limited to 255 characters");
                goto fail;
            }
            strncpy(ret->password, password, password_len);
            break;
        }
    }

    return ret;

fail:
    RedisModule_Free(ret);
    return NULL;
}

static RaftReq *cmdToMigrate(RedisRaftCtx *rr, RedisModuleCtx *ctx, RaftRedisCommand *cmd)
{
    RaftReq *req = NULL;

    /* 0. assert minimum size here */
    if (cmd->argc < 8) {
        RedisModule_WrongArity(ctx);
        return req;
    }

    /* 1. Extract migration info and store it in a dict rr->migrate_info */
    MigrationInfo *migrationInfo = extractMigrationInfo(cmd);
    if (migrationInfo == NULL) {
        RedisModule_ReplyWithError(ctx, "ERR check logs");
        goto exit;
    }

    /* 2. assert single key is empty */
    size_t key_len;
    RedisModule_StringPtrLen(cmd->argv[3], &key_len);
    if (key_len != 0) {
        RedisModule_ReplyWithError(ctx, "ERR RedisRaft doesn't support old style single key migration");
        goto exit;
    }

    /* find "keys" for parsing out keys */
    int idx = MIGRATE_CMD_OPTIONAL_ARG_IDX;
    for (; idx < cmd->argc; idx++) {
        size_t str_len;
        const char *str = RedisModule_StringPtrLen(cmd->argv[idx], &str_len);
        if (str_len == 4 && !strcasecmp("keys", str)) {
            break;
        }
    }

    /* move idx past "keys" argv */
    idx++;

    if (idx >= cmd->argc) {
        RedisModule_ReplyWithError(ctx, "ERR Didn't provide any keys to migrate");
        goto exit;
    }

    RedisModuleString **keys = RedisModule_Alloc(sizeof(RedisModuleString *) * (cmd->argc - idx));
    int num_keys = cmd->argc - idx;
    for (int i = 0; i < num_keys; i++) {
        keys[i] = RedisModule_HoldString(ctx, cmd->argv[idx + i]);
    }

    req = RaftReqInit(ctx, RR_MIGRATE_KEYS);

    /* Overwrite with new data */
    req->r.migrate_keys.num_keys = num_keys;
    req->r.migrate_keys.keys = keys;
    req->r.migrate_keys.keys_serialized = RedisModule_Calloc(num_keys, sizeof(RedisModuleString *));
    memcpy(req->r.migrate_keys.auth_username, migrationInfo->username, MAX_AUTH_STRING_ARG_LENGTH);
    memcpy(req->r.migrate_keys.auth_password, migrationInfo->password, MAX_AUTH_STRING_ARG_LENGTH);

exit:
    RedisModule_Free(migrationInfo);
    return req;
}

static void handleMigrateCommand(RedisRaftCtx *rr, RedisModuleCtx *ctx, RaftRedisCommand *cmd)
{
    if (rr->migrate_req != NULL) {
        RedisModule_ReplyWithError(ctx, "ERR RedisRaft only supports one concurrent migration currently");
        return;
    }

    if (!raft_is_leader(rr->raft)) {
        Node *leader = getLeaderNodeOrReply(rr, ctx);
        if (leader) {
            redirectCommand(rr, ctx, leader);
        }
        return;
    }

    RaftReq *req = cmdToMigrate(rr, ctx, cmd);
    if (req == NULL) {
        return;
    }

    raft_entry_t *entry = RaftRedisLockKeysSerialize(req->r.migrate_keys.keys, req->r.migrate_keys.num_keys);
    entry->id = rand();
    entry->type = RAFT_LOGTYPE_LOCK_KEYS;
    entryAttachRaftReq(rr, entry, req);
    int e = raft_recv_entry(rr->raft, entry, NULL);
    if (e != 0) {
        replyRaftError(req->ctx, e);
        RaftReqFree(req);
        entryDetachRaftReq(rr, entry);
    }

    raft_entry_release(entry);
}

static bool getAskingState(RedisRaftCtx *rr, RedisModuleCtx *ctx)
{
    ClientState *clientState = ClientStateGet(rr, ctx);
    return clientState->asking;
}

static void setAskingState(RedisRaftCtx *rr, RedisModuleCtx *ctx, bool val)
{
    ClientState *clientState = ClientStateGet(rr, ctx);
    clientState->asking = val;
}

static void handleAsking(RedisRaftCtx *rr, RedisModuleCtx *ctx)
{
    setAskingState(rr, ctx, true);

    RedisModule_ReplyWithSimpleString(ctx, "OK");
}

/* Handle interception of Redis commands that have a different
 * implementation in RedisRaft.
 *
 * This is logically similar to handleMultiExec but implemented
 * separately for readability purposes.
 *
 * Currently intercepted commands:
 * - CLUSTER
 * - INFO
 * - MIGRATE
 * - ASKING
 *
 * Returns true if the command was intercepted, in which case the client has been replied to
 */
static bool handleInterceptedCommands(RedisRaftCtx *rr,
                                      RedisModuleCtx *ctx,
                                      RaftRedisCommandArray *cmds)
{
    RaftRedisCommand *cmd = cmds->commands[0];
    size_t len;
    const char *cmd_str = RedisModule_StringPtrLen(cmd->argv[0], &len);

    if (len == strlen("CLUSTER") && strncasecmp(cmd_str, "CLUSTER", len) == 0) {
        ShardingHandleClusterCommand(rr, ctx, cmd);
        return true;
    }

    if (len == strlen("INFO") && strncasecmp(cmd_str, "INFO", len) == 0) {
        handleInfoCommand(rr, ctx, cmd);
        return true;
    }

    if (len == strlen("MIGRATE") && strncasecmp(cmd_str, "MIGRATE", len) == 0) {
        handleMigrateCommand(rr, ctx, cmd);
        return true;
    }

    if (len == strlen("ASKING") && strncasecmp(cmd_str, "ASKING", len) == 0) {
        handleAsking(rr, ctx);
        return true;
    }

    return false;
}

static void handleRedisCommandAppend(RedisRaftCtx *rr,
                                     RedisModuleCtx *ctx,
                                     RaftRedisCommandArray *cmds)
{
    /* Check that we're part of a bootstrapped cluster and not in the middle of
     * joining or loading data.
     */
    if (checkRaftState(rr, ctx) == RR_ERROR) {
        return;
    }

    if (!raft_is_leader(rr->raft)) {
        Node *leader = getLeaderNodeOrReply(rr, ctx);
        if (!leader) {
            return;
        }

        handleNonLeaderCommand(rr, ctx, leader, cmds);
        return;
    }

    /* Do not accept new commands until an entry from the current term is
     * applied. Applying the first entry will replay previous entries, so, it'll
     * advance the state machine to the latest state. It helps with:
     *
     * 1- If non-quorum reads are enabled, we might process a request before
     * replaying logs. The state machine might be in an older state. Reading
     * from it might look like going backward in time. Waiting for the state
     * machine to advance will provide a better user experience in this case.
     *
     * 2- As we know we've built the latest state machine, we'll have more
     * accurate info about memory usage for 'maxmemory' handling. */
    raft_term_t term = raft_get_current_term(rr->raft);
    if (term != rr->snapshot_info.last_applied_term) {
        replyClusterDown(ctx);
        return;
    }

    /* Normally we can expect a single command in the request, unless it is a
     * MULTI/EXEC transaction in which case all queued commands are handled at
     * once.
     */
    unsigned int cmd_flags = CommandSpecTableGetAggregateFlags(rr->commands_spec_table, cmds, CMD_SPEC_WRITE);
    if (cmd_flags & CMD_SPEC_UNSUPPORTED) {
        RedisModule_ReplyWithError(ctx, "ERR not supported by RedisRaft");
        return;
    }

    /* dry run */
    for (int i = 0; i < cmds->len; i++) {
        RaftRedisCommand *cmd = cmds->commands[i];
        size_t cmdlen;
        const char *cmdstr = RedisModule_StringPtrLen(cmd->argv[0], &cmdlen);
        /* skip multi */
        if (i == 0 && cmdlen == 5 && !strncasecmp(cmdstr, "MULTI", 5)) {
            continue;
        }

        char *rm_call_flags;

        if (cmd_flags & CMD_SPEC_DENYOOM && (RedisModule_GetUsedMemoryRatio() > 1.0)) {
            rm_call_flags = "DCEMv";
        } else {
            rm_call_flags = "DCEv";
        }

        enterRedisModuleCall();
        RedisModuleCallReply *reply = RedisModule_Call(ctx, cmdstr, rm_call_flags, cmd->argv + 1, cmd->argc - 1);;
        exitRedisModuleCall();
        if (reply != NULL) {
            const char *err_str = RedisModule_CallReplyStringPtr(reply, NULL);
            RedisModule_ReplyWithError(ctx, err_str);
            RedisModule_FreeCallReply(reply);
            return;
        }
    }

    /* Handle the special case of read-only commands here: if quorum reads
     * are enabled schedule the request to be processed when we have a guarantee
     * we're still a leader. Otherwise, just process the reads. */
    if (cmd_flags & CMD_SPEC_READONLY && !(cmd_flags & CMD_SPEC_WRITE)) {
        if (!rr->config.quorum_reads) {
            RaftExecuteCommandArray(rr, ctx, ctx, cmds);
            return;
        }

        RaftReq *req = RaftReqInit(ctx, RR_REDISCOMMAND);
        RaftRedisCommandArrayMove(&req->r.redis.cmds, cmds);

        int rc = raft_recv_read_request(rr->raft, handleReadOnlyCommand, req);
        if (rc != 0) {
            replyRaftError(ctx, rc);
        }
        return;
    }

    RaftReq *req = RaftReqInit(ctx, RR_REDISCOMMAND);
    RaftRedisCommandArrayMove(&req->r.redis.cmds, cmds);

    if (cmd_flags & CMD_SPEC_SCRIPTS) {
        RedisModuleString *user_name = RedisModule_GetCurrentUserName(ctx);
        RedisModuleUser *user = RedisModule_GetModuleUserFromUserName(user_name);
        req->r.redis.cmds.acl = RedisModule_GetModuleUserACLString(user);
        RedisModule_FreeModuleUser(user);
        RedisModule_FreeString(ctx, user_name);
    }

    raft_entry_t *entry = RaftRedisCommandArraySerialize(&req->r.redis.cmds);
    entry->id = rand();
    entry->type = RAFT_LOGTYPE_NORMAL;
    entryAttachRaftReq(rr, entry, req);

    int e = raft_recv_entry(rr->raft, entry, NULL);
    if (e != 0) {
        replyRaftError(ctx, e);
        entryDetachRaftReq(rr, entry);
        raft_entry_release(entry);
        RaftReqFree(req);
        return;
    }

    raft_entry_release(entry);
}

static void handleRedisCommand(RedisRaftCtx *rr,
                               RedisModuleCtx *ctx, RaftRedisCommandArray *cmds)
{
    /* update cmds array with the client's saved "asking" state */
    cmds->asking = getAskingState(rr, ctx);
    setAskingState(rr, ctx, false);

    /* Handle intercepted commands. We do this also on non-leader nodes or if we
     * don't have a leader, so it's up to the commands to check these conditions
     * if they have to.
     */
    if (handleInterceptedCommands(rr, ctx, cmds)) {
        return;
    }

    /* Check if MULTI/EXEC bundling is required. */
    if (MultiHandleCommand(rr, ctx, cmds)) {
        return;
    }

    handleRedisCommandAppend(rr, ctx, cmds);
}

static void redirectCommand(RedisRaftCtx *rr, RedisModuleCtx *ctx, Node *leader)
{
    RedisModule_Assert(!raft_is_leader(rr->raft));

    /* One anomaly here is that may redirect a client to the leader even for
     * commands have no keys, which is something Redis Cluster never does. We
     * still need to consider how this impacts clients which may not expect it.
     */
    replyRedirect(ctx, 0, &leader->addr);
}

static void handleNonLeaderCommand(RedisRaftCtx *rr, RedisModuleCtx *ctx, Node *leader, RaftRedisCommandArray *cmds)
{
    RedisModule_Assert(!raft_is_leader(rr->raft));
    RedisModule_Assert(cmds);

    /* reply ASK for ASKING state commands */
    if (cmds->asking) {
        int slot;
        if (HashSlotCompute(rr, cmds, &slot) != RR_OK) {
            replyCrossSlot(ctx);
            return;
        }

        if (slot == -1) {
            /* ASKING mode requests should always have keys */
            RedisModule_ReplyWithError(ctx, "ERR cmd without keys in asking mode");
            return;
        }

        replyAsk(ctx, (unsigned int) slot, &leader->addr);
        return;
    }

    /* forward commands to the leader in follower_proxy state */
    if (rr->config.follower_proxy) {
        if (ProxyCommand(rr, ctx, cmds, leader) != RR_OK) {
            RedisModule_ReplyWithError(ctx, "NOTLEADER Failed to proxy command");
        }
        return;
    }

    /* redirect otherwise */
    redirectCommand(rr, ctx, leader);
}

/* RAFT [Redis command to execute]
 *   Submit a Redis command to be appended to the Raft log and applied.
 *   The command blocks until it has been committed to the log by the majority
 *   and applied locally.
 * Reply:
 *   -NOCLUSTER ||
 *   -LOADING ||
 *   -CLUSTERDOWN ||
 *   -MOVED <slot> <addr>:<port> ||
 *   Any standard Redis reply, depending on the command.
 */
static int cmdRaft(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    RedisRaftCtx *rr = &redis_raft;

    if (argc < 2) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    RaftRedisCommandArray cmds = {0};
    RaftRedisCommand *cmd = RaftRedisCommandArrayExtend(&cmds);

    cmd->argc = argc - 1;
    cmd->argv = RedisModule_Alloc((argc - 1) * sizeof(RedisModuleString *));

    int i;
    for (i = 0; i < argc - 1; i++) {
        cmd->argv[i] = argv[i + 1];
        RedisModule_RetainString(ctx, cmd->argv[i]);
    }
    handleRedisCommand(rr, ctx, &cmds);
    RaftRedisCommandArrayFree(&cmds);

    return REDISMODULE_OK;
}

/* RAFT.ENTRY [Serialized Entry]
 *   Receive a serialized batch of Redis commands (like a Raft entry) and
 *   process them, as if received as individual RAFT commands.
 *
 *   This is used to simplify the proxying of MULTI/EXEC commands.
 * Reply:
 *   -MOVED <addr> ||
 *   Any standard Redis reply
 */
static int cmdRaftEntry(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    if (argc != 2) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    size_t data_len;
    const char *data = RedisModule_StringPtrLen(argv[1], &data_len);

    RaftRedisCommandArray cmds = {0};
    if (RaftRedisCommandArrayDeserialize(&cmds, data, data_len) != RR_OK) {
        RedisModule_ReplyWithError(ctx, "ERR invalid argument");
        return REDISMODULE_OK;
    }

    handleRedisCommandAppend(&redis_raft, ctx, &cmds);
    RaftRedisCommandArrayFree(&cmds);
    return REDISMODULE_OK;
}

/* RAFT.AE [target_node_id] [src_node_id]
 *         [leader_id]:[term]:[prev_log_idx]:[prev_log_term]:[leader_commit]:[msg_id]
 *         [n_entries] [<term>:<id>:<type> <entry>]...
 *
 *   A leader request to append entries to the Raft log (per Raft paper).
 * Reply:
 *   -NOCLUSTER ||
 *   -LOADING ||
 *   *4
 *   :<term>
 *   :<success> (0 or 1)
 *   :<current_idx>
 *   :<msg_id>
 */
static int cmdRaftAppendEntries(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    RedisRaftCtx *rr = &redis_raft;

    if (argc < 5) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    if (checkRaftState(rr, ctx) == RR_ERROR) {
        return REDISMODULE_OK;
    }

    int target_node_id;
    if (RedisModuleStringToInt(argv[1], &target_node_id) == REDISMODULE_ERR ||
        target_node_id != rr->config.id) {
        RedisModule_ReplyWithError(ctx, "invalid or incorrect target node id");
        return REDISMODULE_OK;
    }

    long long n_entries;
    if (RedisModule_StringToLongLong(argv[4], &n_entries) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx, "invalid n_entries value");
        return REDISMODULE_OK;
    }
    if (argc != 5 + 2 * n_entries) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    raft_node_id_t src_node_id;
    if (RedisModuleStringToInt(argv[2], &src_node_id) == REDISMODULE_ERR) {
        RedisModule_ReplyWithError(ctx, "invalid source node id");
        return REDISMODULE_OK;
    }

    raft_appendentries_req_t msg = {0};

    size_t tmplen;
    const char *tmpstr = RedisModule_StringPtrLen(argv[3], &tmplen);
    if (sscanf(tmpstr, "%d:%ld:%ld:%ld:%ld:%lu",
               &msg.leader_id,
               &msg.term,
               &msg.prev_log_idx,
               &msg.prev_log_term,
               &msg.leader_commit,
               &msg.msg_id) != 6) {
        RedisModule_ReplyWithError(ctx, "invalid message");
        return REDISMODULE_OK;
    }

    msg.n_entries = (int) n_entries;
    if (n_entries > 0) {
        msg.entries = RedisModule_Calloc(n_entries, sizeof(raft_entry_t));
    }

    for (int i = 0; i < n_entries; i++) {
        /* Create entry with payload */
        tmpstr = RedisModule_StringPtrLen(argv[6 + 2 * i], &tmplen);
        raft_entry_t *e = raft_entry_new(tmplen);
        memcpy(e->data, tmpstr, tmplen);

        /* Parse additional entry fields */
        tmpstr = RedisModule_StringPtrLen(argv[5 + 2 * i], &tmplen);
        if (sscanf(tmpstr, "%ld:%d:%hd", &e->term, &e->id, &e->type) != 3) {
            RedisModule_ReplyWithError(ctx, "invalid entry");
            goto out;
        }

        msg.entries[i] = e;
    }

    rr->appendreq_received++;
    rr->appendreq_with_entry_received += (msg.n_entries > 0) ? 1 : 0;

    raft_appendentries_resp_t resp = {0};
    raft_node_t *node = raft_get_node(rr->raft, src_node_id);

    if (raft_recv_appendentries(rr->raft, node, &msg, &resp) != 0) {
        RedisModule_ReplyWithError(ctx, "ERR operation failed");
        goto out;
    }

    RedisModule_ReplyWithArray(ctx, 4);
    RedisModule_ReplyWithLongLong(ctx, resp.term);
    RedisModule_ReplyWithLongLong(ctx, resp.success);
    RedisModule_ReplyWithLongLong(ctx, resp.current_idx);
    RedisModule_ReplyWithLongLong(ctx, resp.msg_id);

out:
    if (msg.n_entries > 0) {
        for (int i = 0; i < msg.n_entries; i++) {
            raft_entry_t *e = msg.entries[i];
            if (e) {
                raft_entry_release(e);
            }
        }
        RedisModule_Free(msg.entries);
    }

    return REDISMODULE_OK;
}

/* RAFT.SNAPSHOT [target-node-id] [src_node_id]
 *               [term]:[leader_id]:[msg_id]:[snapshot_index]:[snapshot_term]:[chunk_offset]:[last_chunk]
 *               [chunk_data]
 *   Store the specified snapshot chunk (e.g. Raft paper's InstallSnapshot RPC).
 *
 *  Reply:
 *    -NOCLUSTER ||
 *    -LOADING ||
 *    *5
 *    :<term>
 *    :<msg_id>
 *    :<offset>
 *    :<success>
 *    :<last_chunk>
 */
static int cmdRaftSnapshot(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    RedisRaftCtx *rr = &redis_raft;

    if (argc != 5) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    if (checkRaftState(rr, ctx) == RR_ERROR) {
        return REDISMODULE_OK;
    }

    int target_node_id;
    if (RedisModuleStringToInt(argv[1], &target_node_id) != REDISMODULE_OK ||
        target_node_id != rr->config.id) {

        RedisModule_ReplyWithError(ctx, "ERR invalid or incorrect target node id");
        return REDISMODULE_OK;
    }

    int src_node_id;
    if (RedisModuleStringToInt(argv[2], &src_node_id) == REDISMODULE_ERR) {
        RedisModule_ReplyWithError(ctx, "invalid source node id");
        return REDISMODULE_OK;
    }

    raft_snapshot_req_t req = {0};
    const char *tmpstr = RedisModule_StringPtrLen(argv[3], NULL);

    if (sscanf(tmpstr, "%lu:%d:%lu:%lu:%lu:%llu:%d",
               &req.term,
               &req.leader_id,
               &req.msg_id,
               &req.snapshot_index,
               &req.snapshot_term,
               &req.chunk.offset,
               &req.chunk.last_chunk) != 7) {
        RedisModule_ReplyWithError(ctx, "invalid message");
        return REDISMODULE_OK;
    }

    size_t len;
    void *data = (void *) RedisModule_StringPtrLen(argv[4], &len);

    req.chunk.data = data;
    req.chunk.len = len;

    raft_snapshot_resp_t resp = {0};
    raft_node_t *node = raft_get_node(rr->raft, (raft_node_id_t) src_node_id);

    rr->snapshotreq_received++;

    /* raft_recv_snapshot() might trigger loading the received RDB file.
     * While loading, Redis may process incoming messages to reply -LOADING.
     * So, we must block the leader's connection to prevent processing further
     * messages, as we haven't replied to the current command yet. */
    RedisModuleBlockedClient *c = RedisModule_BlockClient(ctx, NULL, 0, 0, 0);

    if (raft_recv_snapshot(rr->raft, node, &req, &resp) != 0) {
        RedisModule_ReplyWithError(ctx, "ERR operation failed");
        goto out;
    }

    RedisModule_ReplyWithArray(ctx, 5);
    RedisModule_ReplyWithLongLong(ctx, resp.term);
    RedisModule_ReplyWithLongLong(ctx, resp.msg_id);
    RedisModule_ReplyWithLongLong(ctx, resp.offset);
    RedisModule_ReplyWithLongLong(ctx, resp.success);
    RedisModule_ReplyWithLongLong(ctx, resp.last_chunk);

out:
    RedisModule_UnblockClient(c, NULL);
    return REDISMODULE_OK;
}

static void clusterInit(const char *cluster_id)
{
    RedisRaftCtx *rr = &redis_raft;

    /* Initialize dbid */
    memcpy(rr->snapshot_info.dbid, cluster_id, RAFT_DBID_LEN);
    rr->snapshot_info.dbid[RAFT_DBID_LEN] = '\0';

    /* This is the first node, so there are no used node ids yet */
    rr->snapshot_info.used_node_ids = NULL;

    /* If node id was not specified, make up one */
    if (!rr->config.id) {
        rr->config.id = makeRandomNodeId(rr);
    }

    rr->log = RaftLogCreate(rr->config.log_filename, rr->snapshot_info.dbid,
                            1, 0, &rr->config);
    if (!rr->log) {
        PANIC("Failed to initialize Raft log");
    }

    RaftLibraryInit(rr, true);
    initSnapshotTransferData(rr);
    AddBasicLocalShardGroup(rr);

    LOG_NOTICE("Raft Cluster initialized, node id: %d, dbid: %s",
               rr->config.id, rr->snapshot_info.dbid);
}

static void clusterJoinCompleted(RaftReq *req)
{
    RedisRaftCtx *rr = &redis_raft;

    /* Initialize Raft log.  We delay this operation as we want to create the
     * log with the proper dbid which is only received now.
     */
    rr->log = RaftLogCreate(rr->config.log_filename,
                            rr->snapshot_info.dbid,
                            rr->snapshot_info.last_applied_term,
                            rr->snapshot_info.last_applied_idx,
                            &rr->config);
    if (!rr->log) {
        PANIC("Failed to initialize Raft log");
    }

    AddBasicLocalShardGroup(rr);
    RaftLibraryInit(rr, false);
    initSnapshotTransferData(rr);

    RedisModule_ReplyWithSimpleString(req->ctx, "OK");
    RaftReqFree(req);
}

/* RAFT.CLUSTER INIT <id>
 *   Initializes a new Raft cluster.
 *   <id> is an optional 32 character string, if set, cluster will use it for the id
 * Reply:
 *   +OK [dbid]
 *
 * RAFT.CLUSTER JOIN [addr:port]
 *   Join an existing cluster.
 *   The operation is asynchronous and may take place/retry in the background.
 * Reply:
 *   +OK
 */
static int cmdRaftCluster(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    RedisRaftCtx *rr = &redis_raft;

    if (argc < 2) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    if (checkRaftUninitialized(rr, ctx) == RR_ERROR ||
        checkRaftNotLoading(rr, ctx) == RR_ERROR) {
        return REDISMODULE_OK;
    }

    size_t cmd_len;
    const char *cmd = RedisModule_StringPtrLen(argv[1], &cmd_len);

    if (!strncasecmp(cmd, "INIT", cmd_len)) {
        if (argc != 2 && argc != 3) {
            RedisModule_WrongArity(ctx);
            return REDISMODULE_OK;
        }

        char cluster_id[RAFT_DBID_LEN];

        if (argc == 2) {
            RedisModule_GetRandomHexChars(cluster_id, RAFT_DBID_LEN);
        } else {
            size_t len;
            const char *reqId = RedisModule_StringPtrLen(argv[2], &len);
            if (len != RAFT_DBID_LEN) {
                LOG_WARNING("RAFT.CLUSTER failed; argv[2] cluster id is wrong (%ld instead of %d)", len, RAFT_DBID_LEN);
                RedisModule_ReplyWithError(ctx, "ERR invalid cluster message (cluster id length)");
                return REDISMODULE_OK;
            }
            memcpy(cluster_id, reqId, RAFT_DBID_LEN);
        }

        clusterInit(cluster_id);

        char reply[RAFT_DBID_LEN + 256];
        snprintf(reply, sizeof(reply) - 1, "OK %s", rr->snapshot_info.dbid);

        RedisModule_ReplyWithSimpleString(ctx, reply);
    } else if (!strncasecmp(cmd, "JOIN", cmd_len)) {
        if (argc < 3) {
            RedisModule_WrongArity(ctx);
            return REDISMODULE_OK;
        }

        NodeAddrListElement *addrs = NULL;

        for (int i = 2; i < argc; i++) {
            NodeAddr addr;
            if (getNodeAddrFromArg(ctx, argv[i], &addr) == RR_ERROR) {
                /* Error already produced */
                return REDISMODULE_OK;
            }
            NodeAddrListAddElement(&addrs, &addr);
        }

        RaftReq *req = RaftReqInit(ctx, RR_CLUSTER_JOIN);
        JoinCluster(rr, addrs, req, clusterJoinCompleted);

        NodeAddrListFree(addrs);
        rr->state = REDIS_RAFT_JOINING;
    } else {
        RedisModule_ReplyWithError(ctx, "RAFT.CLUSTER supports INIT / JOIN only");
    }

    return REDISMODULE_OK;
}

/* RAFT.SHARDGROUP GET
 *   Returns the current cluster's local shard group configuration, in a format
 *   that is compatible with RAFT.SHARDGROUP ADD.
 * Reply:
 *   [start-slot] [end-slot] [node-id node-addr] [node-id node-addr...]
 *
 * RAFT.SHARDGROUP ADD [shardgroup id] [num_slots] [num_nodes] ([start slot] [end slot] [slot type])* ([node-uid node-addr:node-port])*
 *   Adds a new shard group configuration.
 * Reply:
 *   +OK
 *
 * RAFT.SHARDGROUP REPLACE [num shardgroups] ([shardgroup id] [num_slots] [num_nodes] ([start slot] [end slot] [slot type] [migration session key])* ([node-uid] [node-addr:node-port])*)*
 *   Replaces all the external shardgroups with the external shardgroups listed here.  ignores the shardgroup set that is the local cluster
 * Reply:
 *   +OK
 *
 * RAFT.SHARDGROUP LINK [node-addr:port]
 *   Link cluster with a new remote shardgroup.
 * Reply:
 *   +OK
 *   -ERR error description
 */
static int cmdRaftShardGroup(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    RedisRaftCtx *rr = &redis_raft;

    if (argc < 2) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    if (!rr->config.sharding) {
        RedisModule_ReplyWithError(ctx, "ERR RedisRaft sharding not enabled");
        return REDISMODULE_OK;
    }

    if (checkRaftState(rr, ctx) == RR_ERROR) {
        return REDISMODULE_OK;
    }

    if (!raft_is_leader(rr->raft)) {
        Node *leader = getLeaderNodeOrReply(rr, ctx);
        if (leader) {
            redirectCommand(rr, ctx, leader);
        }
        return REDISMODULE_OK;
    }

    size_t cmd_len;
    const char *cmd = RedisModule_StringPtrLen(argv[1], &cmd_len);

    if (!strncasecmp(cmd, "GET", cmd_len)) {
        ShardGroupGet(rr, ctx);
        return REDISMODULE_OK;
    } else if (!strncasecmp(cmd, "ADD", cmd_len)) {
        if (argc < 4) {
            RedisModule_WrongArity(ctx);
            return REDISMODULE_OK;
        }

        ShardGroupAdd(rr, ctx, argv, argc);
        return REDISMODULE_OK;
    } else if (!strncasecmp(cmd, "REPLACE", cmd_len)) {
        if (argc < 4) {
            RedisModule_WrongArity(ctx);
            return REDISMODULE_OK;
        }

        ShardGroupReplace(rr, ctx, argv, argc);
        return REDISMODULE_OK;
    } else if (!strncasecmp(cmd, "LINK", cmd_len)) {
        if (argc != 3) {
            RedisModule_WrongArity(ctx);
            return REDISMODULE_OK;
        }

        ShardGroupLink(rr, ctx, argv, argc);
        return REDISMODULE_OK;
    } else {
        RedisModule_ReplyWithError(ctx, "RAFT.SHARDGROUP supports GET/ADD/REPLACE/LINK only");
        return REDISMODULE_OK;
    }
}

/* RAFT.DEBUG COMPACT [delay]
 *   Initiate an immediate rewrite of the Raft log + snapshot.
 *   If [delay] is specified, introduce an artificial delay of [delay] seconds in
 *   the background rewrite child process.
 * Reply:
 *   +OK
 *
 * RAFT.DEBUG NODECFG <node_id> <"{+voting|-voting|+active|-active}*">
 *     Set/unset voting/active node configuration flags
 * Reply:
 *    +OK
 *
 * RAFT.DEBUG SENDSNAPSHOT <node_id>
 *     Send node snapshot
 * Reply:
 *     +OK
 *
 * RAFT.DEBUG USED_NODE_IDS
 *     return an array of used node ids
 * Reply:
 *    An array of used node ids
 *
 * RAFT.DEBUG EXEC [Redis command to execute]+
 *     Execute Redis commands locally (commands do not go through Raft interception)
 * Reply:
 *     Any standard Redis reply, depending on the commands.
 *
 * RAFT.DEBUG DISABLE_APPLY <val>
 *     Set/unset disable_apply raft configuration flag
 *
 * RAFT.DEBUG DELAY_APPLY <val>
 *     Sleep <val> microseconds before executing a command
 *
 * RAFT.DEBUG MIGRATION_DEBUG [fail_connect|fail_import|fail_unlock|none]
 *     Inject errors at specific places in migration flow to test consistency
 * Reply:
 *     +OK
 *
 * RAFT.DEBUG COMMANDSPEC <command>
 *     Retruns the flags associated with this command in the commandspec dict
 */
static int cmdRaftDebug(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    RedisRaftCtx *rr = &redis_raft;

    if (argc < 2) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    size_t cmdlen;
    const char *cmdstr = RedisModule_StringPtrLen(argv[1], &cmdlen);
    char cmd[cmdlen + 1];

    memcpy(cmd, cmdstr, cmdlen);
    cmd[cmdlen] = '\0';

    if (!strncasecmp(cmd, "compact", cmdlen)) {
        long long fail = 0;
        long long delay = 0;

        if (argc == 3) {
            if (RedisModule_StringToLongLong(argv[2], &delay) != REDISMODULE_OK) {
                RedisModule_ReplyWithError(ctx, "ERR invalid compact delay value");
                return REDISMODULE_OK;
            }
        }

        if (argc == 4) {
            if (RedisModule_StringToLongLong(argv[3], &fail) != REDISMODULE_OK) {
                RedisModule_ReplyWithError(ctx, "ERR invalid compact fail value");
                return REDISMODULE_OK;
            }
        }

        RaftReq *req = RaftReqInit(ctx, RR_DEBUG);
        req->r.debug.delay = (int) delay;
        req->r.debug.fail = (int) fail;

        rr->debug_req = req;

        if (initiateSnapshot(rr) != RR_OK) {
            LOG_VERBOSE("RAFT.DEBUG COMPACT requested but failed.");
            RedisModule_ReplyWithError(req->ctx, "ERR operation failed");
            rr->debug_req = NULL;
            RaftReqFree(req);
        }
    } else if (!strncasecmp(cmd, "nodecfg", cmdlen)) {
        if (argc != 4) {
            RedisModule_WrongArity(ctx);
            return REDISMODULE_OK;
        }

        long long node_id;
        if (RedisModule_StringToLongLong(argv[2], &node_id) != REDISMODULE_OK) {
            RedisModule_ReplyWithError(ctx, "ERR invalid node id");
            return REDISMODULE_OK;
        }

        raft_node_t *node = raft_get_node(rr->raft, (raft_node_id_t) node_id);
        if (!node) {
            RedisModule_ReplyWithError(ctx, "ERR node does not exist");
            return REDISMODULE_OK;
        }

        size_t slen;
        const char *str = RedisModule_StringPtrLen(argv[3], &slen);

        char *cfg = RedisModule_Alloc(slen + 1);
        memcpy(cfg, str, slen);
        cfg[slen] = '\0';

        char *saveptr = NULL;
        char *tok;

        tok = strtok_r(cfg, " ", &saveptr);
        while (tok != NULL) {
            if (!strcasecmp(tok, "+voting")) {
                raft_node_set_voting(node, 1);
                raft_node_set_voting_committed(node, 1);
            } else if (!strcasecmp(tok, "-voting")) {
                raft_node_set_voting(node, 0);
                raft_node_set_voting_committed(node, 0);
            } else if (!strcasecmp(tok, "+active")) {
                raft_node_set_active(node, 1);
            } else if (!strcasecmp(tok, "-active")) {
                raft_node_set_active(node, 0);
            } else {
                RedisModule_ReplyWithError(ctx, "ERR invalid nodecfg option");
                RedisModule_Free(cfg);
                return REDISMODULE_OK;
            }
            tok = strtok_r(NULL, " ", &saveptr);
        }

        RedisModule_Free(cfg);
        RedisModule_ReplyWithSimpleString(ctx, "OK");
    } else if (!strncasecmp(cmd, "sendsnapshot", cmdlen)) {
        if (argc != 3) {
            RedisModule_WrongArity(ctx);
            return REDISMODULE_OK;
        }

        long long node_id;
        if (RedisModule_StringToLongLong(argv[2], &node_id) != REDISMODULE_OK) {
            RedisModule_ReplyWithError(ctx, "ERR invalid node id");
            return REDISMODULE_OK;
        }

        raft_node_t *node = raft_get_node(rr->raft, (raft_node_id_t) node_id);
        if (!node) {
            RedisModule_ReplyWithError(ctx, "ERR node does not exist");
            return REDISMODULE_OK;
        }

        if (node_id == raft_get_nodeid(rr->raft)) {
            RedisModule_ReplyWithError(ctx, "ERR cannot send snapshot itself");
            return REDISMODULE_OK;
        }

        raft_node_set_next_idx(node, raft_get_snapshot_last_idx(rr->raft));
        RedisModule_ReplyWithSimpleString(ctx, "OK");

    } else if (!strncasecmp(cmd, "used_node_ids", cmdlen)) {
        RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

        int len = 0;
        NodeIdEntry *e;

        for (e = rr->snapshot_info.used_node_ids; e != NULL; e = e->next) {
            RedisModule_ReplyWithLongLong(ctx, e->id);
            len++;
        }

        RedisModule_ReplySetArrayLength(ctx, len);
    } else if (!strncasecmp(cmd, "exec", cmdlen) && argc > 3) {
        RedisModuleCallReply *reply;

        const char *exec_cmd = RedisModule_StringPtrLen(argv[2], NULL);

        enterRedisModuleCall();
        reply = RedisModule_Call(ctx, exec_cmd, "v", &argv[3], argc - 3);
        exitRedisModuleCall();

        if (!reply) {
            RedisModule_ReplyWithError(ctx, "Bad command or failed to execute");
        } else {
            RedisModule_ReplyWithCallReply(ctx, reply);
            RedisModule_FreeCallReply(reply);
        }
    } else if (!strncasecmp(cmd, "disable_apply", cmdlen) && argc == 3) {
        long long val;
        if (RedisModule_StringToLongLong(argv[2], &val) != REDISMODULE_OK) {
            RedisModule_ReplyWithError(ctx, "ERR invalid disable apply value");
            return REDISMODULE_OK;
        }

        if (checkRaftState(rr, ctx) != RR_OK) {
            return REDISMODULE_OK;
        }

        if (raft_config(rr->raft, 1, RAFT_CONFIG_DISABLE_APPLY, val) != 0) {
            RedisModule_ReplyWithError(ctx, "ERR failed to configure libraft");
            return REDISMODULE_OK;
        }

        RedisModule_ReplyWithSimpleString(ctx, "OK");
    } else if (!strncasecmp(cmd, "delay_apply", cmdlen) && argc == 3) {
        long long val;
        if (RedisModule_StringToLongLong(argv[2], &val) != REDISMODULE_OK) {
            RedisModule_ReplyWithError(ctx, "ERR invalid delay apply value");
            return REDISMODULE_OK;
        }

        rr->debug_delay_apply = val;
        RedisModule_ReplyWithSimpleString(ctx, "OK");
    } else if (!strncasecmp(cmd, "migration_debug", cmdlen) && argc == 3) {
        MigrationDebug val;

        size_t len;
        const char *str = RedisModule_StringPtrLen(argv[2], &len);
        if (!strncasecmp(str, "fail_connect", len)) {
            val = DEBUG_MIGRATION_EMULATE_CONNECT_FAILED;
        } else if (!strncasecmp(str, "fail_import", len)) {
            val = DEBUG_MIGRATION_EMULATE_IMPORT_FAILED;
        } else if (!strncasecmp(str, "fail_unlock", len)) {
            val = DEBUG_MIGRATION_EMULATE_UNLOCK_FAILED;
        } else if (!strncasecmp(str, "none", len)) {
            val = DEBUG_MIGRATION_NONE;
        } else {
            RedisModule_ReplyWithError(ctx, "ERR invalid migration debug value");
            return REDISMODULE_OK;
        }

        rr->migration_debug = val;

        RedisModule_ReplyWithSimpleString(ctx, "OK");
        return REDISMODULE_OK;
    } else if (!strncasecmp(cmd, "commandspec", cmdlen) && argc == 3) {
        const CommandSpec *cs = CommandSpecTableGet(redis_raft.commands_spec_table, argv[2]);
        if (cs == NULL) {
            RedisModule_ReplyWithError(ctx, "ERR unknown command");
        } else {
            RedisModule_ReplyWithLongLong(ctx, cs->flags);
        }
        return REDISMODULE_OK;
    } else {
        RedisModule_ReplyWithError(ctx, "ERR invalid debug subcommand");
    }

    return REDISMODULE_OK;
}

static int cmdRaftNodeShutdown(RedisModuleCtx *ctx,
                               RedisModuleString **argv, int argc)
{
    RedisRaftCtx *rr = &redis_raft;

    if (argc != 2) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    long long node_id;
    if (RedisModule_StringToLongLong(argv[1], &node_id) != REDISMODULE_OK ||
        node_id != rr->config.id) {
        RedisModule_ReplyWithError(ctx, "invalid node id");
        return REDISMODULE_OK;
    }

    shutdownAfterRemoval(rr);
    return REDISMODULE_OK;
}

static int cmdRaftSort(RedisModuleCtx *ctx,
                       RedisModuleString **argv, int argc)
{
    if (argc < 2) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    argv++;
    argc--;

    handleSort(ctx, argv, argc);

    return REDISMODULE_OK;
}

static int cmdRaftRandom(RedisModuleCtx *ctx,
                         RedisModuleString **argv, int argc)
{
    RedisModule_ReplyWithError(ctx, "Cannot run a command with random results in this context");

    return REDISMODULE_OK;
}

static int cmdRaftScan(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    RedisRaftCtx *rr = &redis_raft;

    if (argc < 3) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    char cursor[32] = {0};
    char slots[REDIS_RAFT_HASH_SLOTS] = {0};
    char *slot_str;

    size_t str_len;
    const char *str = RedisModule_StringPtrLen(argv[1], &str_len);
    strncpy(cursor, str, str_len);

    str = RedisModule_StringPtrLen(argv[2], &str_len);
    slot_str = RedisModule_Alloc(str_len + 1);
    strncpy(slot_str, str, str_len);
    slot_str[str_len] = '\0';

    int ret = parseHashSlots(slots, slot_str);
    RedisModule_Free(slot_str);
    if (ret != RR_OK) {
        RedisModule_ReplyWithError(ctx, "ERR couldn't parse slots");
        return REDISMODULE_OK;
    }

    RedisModuleCallReply *reply;
    if (!(reply = RedisModule_Call(ctx, "scan", "ccl", cursor, "count", rr->config.scan_size))) {
        RedisModule_ReplyWithError(ctx, "ERR scan failed");
        return REDISMODULE_OK;
    }

    if (RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_ARRAY) {
        RedisModule_ReplyWithError(ctx, "ERR scan returned unexpected reply type");
        goto exit;
    }

    if (RedisModule_CallReplyLength(reply) != 2) {
        RedisModule_ReplyWithError(ctx, "ERR scan returned unexpected array length");
        goto exit;
    }

    RedisModuleCallReply *rCursor = RedisModule_CallReplyArrayElement(reply, 0);
    if (RedisModule_CallReplyType(rCursor) != REDISMODULE_REPLY_STRING) {
        RedisModule_ReplyWithError(ctx, "ERR scan returned unexpected type for cursor");
        goto exit;
    }

    RedisModuleCallReply *list = RedisModule_CallReplyArrayElement(reply, 1);
    if (RedisModule_CallReplyType(list) != REDISMODULE_REPLY_ARRAY) {
        RedisModule_ReplyWithError(ctx, "ERR scan returned unexpected type for key list");
        goto exit;
    }

    for (size_t i = 0; i < RedisModule_CallReplyLength(list); i++) {
        if (RedisModule_CallReplyType(RedisModule_CallReplyArrayElement(list, i)) != REDISMODULE_REPLY_STRING) {
            RedisModule_ReplyWithError(ctx, "ERR scan returned unexpected type for a key");
            goto exit;
        }
    }

    RedisModule_ReplyWithArray(ctx, 2);
    RedisModule_ReplyWithCallReply(ctx, rCursor);
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_LEN);
    long count = 0;
    for (size_t i = 0; i < RedisModule_CallReplyLength(list); i++) {
        RedisModuleCallReply *key = RedisModule_CallReplyArrayElement(list, i);
        str = RedisModule_CallReplyStringPtr(key, &str_len);
        unsigned int slot = keyHashSlot(str, str_len);
        if (slots[slot]) {
            count++;
            RedisModule_ReplyWithArray(ctx, 2);
            RedisModule_ReplyWithCallReply(ctx, key);
            RedisModule_ReplyWithLongLong(ctx, slot);
        }
    }
    RedisModule_ReplySetArrayLength(ctx, count);

exit:
    RedisModule_FreeCallReply(reply);
    return REDISMODULE_OK;
}

void handleClientEvent(RedisModuleCtx *ctx, RedisModuleEvent eid,
                       uint64_t subevent, void *data)
{
    (void) ctx;

    RedisRaftCtx *rr = &redis_raft;
    RedisModuleClientInfo *ci = data;

    if (eid.id == REDISMODULE_EVENT_CLIENT_CHANGE) {
        switch (subevent) {
            case REDISMODULE_SUBEVENT_CLIENT_CHANGE_DISCONNECTED:
                ClientStateFree(rr, ci->id);
                break;
            case REDISMODULE_SUBEVENT_CLIENT_CHANGE_CONNECTED:
                ClientStateAlloc(rr, ci->id);
                break;
        }
    }
}

/* Command filter callback that intercepts normal Redis commands and prefixes them
 * with a RAFT command prefix in order to divert them to execute inside RedisRaft.
 */
static void interceptRedisCommands(RedisModuleCommandFilterCtx *filter)
{
    if (checkInRedisModuleCall()) {
        /* if we are running a command in lua that has to be sorted to be deterministic across all nodes */
        if (redis_raft.entered_eval) {
            RedisModuleString *cmd = RedisModule_CommandFilterArgGet(filter, 0);
            const CommandSpec *cs;
            if ((cs = CommandSpecTableGet(redis_raft.commands_spec_table, cmd)) != NULL) {
                if (cs->flags & CMD_SPEC_SORT_REPLY) {
                    RedisModuleString *raft_str = RedisModule_CreateString(NULL, "RAFT._SORT_REPLY", 16);
                    RedisModule_CommandFilterArgInsert(filter, 0, raft_str);
                } else if (cs->flags & CMD_SPEC_RANDOM) {
                    RedisModuleString *raft_str = RedisModule_CreateString(NULL, "RAFT._REJECT_RANDOM_COMMAND", 27);
                    RedisModule_CommandFilterArgInsert(filter, 0, raft_str);
                }
            }
        }

        return;
    }

    const CommandSpec *cs = CommandSpecTableGet(redis_raft.commands_spec_table, RedisModule_CommandFilterArgGet(filter, 0));
    if (cs && (cs->flags & CMD_SPEC_DONT_INTERCEPT))
        return;

    /* Prepend RAFT to the original command */
    RedisModuleString *raft_str = RedisModule_CreateString(NULL, "RAFT", 4);
    RedisModule_CommandFilterArgInsert(filter, 0, raft_str);
}

/* Callback from Redis event loop */
static void beforeSleep(RedisModuleCtx *ctx, RedisModuleEvent eid,
                        uint64_t subevent, void *data)
{
    if (subevent == REDISMODULE_SUBEVENT_EVENTLOOP_BEFORE_SLEEP) {
        handleBeforeSleep(&redis_raft);
    }
}

static void handleInfo(RedisModuleInfoCtx *ctx, int for_crash_report)
{
    (void) for_crash_report;

    RedisRaftCtx *rr = &redis_raft;

    RedisModule_InfoAddSection(ctx, "version");
    RedisModule_InfoAddFieldCString(ctx, "version", REDISRAFT_VERSION);
    RedisModule_InfoAddFieldCString(ctx, "git_sha1", REDISRAFT_GIT_SHA1);

    RedisModule_InfoAddSection(ctx, "general");
    RedisModule_InfoAddFieldCString(ctx, "dbid", rr->snapshot_info.dbid);
    RedisModule_InfoAddFieldLongLong(ctx, "node_id", rr->config.id);
    RedisModule_InfoAddFieldCString(ctx, "state", getStateStr(rr));
    RedisModule_InfoAddFieldCString(ctx, "role", rr->raft ? raft_get_state_str(rr->raft) : "(none)");

    char *voting = "-";
    if (rr->raft) {
        voting = raft_node_is_voting(raft_get_my_node(rr->raft)) ? "yes" : "no";
    }
    RedisModule_InfoAddFieldCString(ctx, "is_voting", voting);
    RedisModule_InfoAddFieldLongLong(ctx, "leader_id", rr->raft ? raft_get_leader_id(rr->raft) : -1);
    RedisModule_InfoAddFieldLongLong(ctx, "current_term", rr->raft ? raft_get_current_term(rr->raft) : 0);
    RedisModule_InfoAddFieldLongLong(ctx, "num_nodes", rr->raft ? raft_get_num_nodes(rr->raft) : 0);
    RedisModule_InfoAddFieldLongLong(ctx, "num_voting_nodes", rr->raft ? raft_get_num_voting_nodes(rr->raft) : 0);

    long long now = RedisModule_Milliseconds();
    int num_nodes = rr->raft ? raft_get_num_nodes(rr->raft) : 0;
    for (int i = 0; i < num_nodes; i++) {
        raft_node_t *rn = raft_get_node_from_idx(rr->raft, i);
        Node *n = raft_node_get_udata(rn);
        if (!n) {
            continue;
        }

        char name[32];
        snprintf(name, sizeof(name), "node%d", i);

        RedisModule_InfoBeginDictField(ctx, name);
        RedisModule_InfoAddFieldLongLong(ctx, "id", n->id);
        RedisModule_InfoAddFieldCString(ctx, "state", ConnGetStateStr(n->conn));
        RedisModule_InfoAddFieldCString(ctx, "voting", raft_node_is_voting(rn) ? "yes" : "no");
        RedisModule_InfoAddFieldCString(ctx, "addr", n->addr.host);
        RedisModule_InfoAddFieldULongLong(ctx, "port", n->addr.port);

        long long int last = -1;
        if (n->conn->last_connected_time) {
            last = (now - n->conn->last_connected_time) / 1000;
        }
        RedisModule_InfoAddFieldLongLong(ctx, "last_conn_secs", last);

        RedisModule_InfoAddFieldULongLong(ctx, "conn_errors", n->conn->connect_errors);
        RedisModule_InfoAddFieldULongLong(ctx, "conn_oks", n->conn->connect_oks);
        RedisModule_InfoEndDictField(ctx);
    }

    RedisModule_InfoAddSection(ctx, "log");
    RedisModule_InfoAddFieldLongLong(ctx, "log_entries", rr->raft ? raft_get_log_count(rr->raft) : 0);
    RedisModule_InfoAddFieldLongLong(ctx, "current_index", rr->raft ? raft_get_current_idx(rr->raft) : 0);
    RedisModule_InfoAddFieldLongLong(ctx, "commit_index", rr->raft ? raft_get_commit_idx(rr->raft) : 0);
    RedisModule_InfoAddFieldLongLong(ctx, "last_applied_index", rr->raft ? raft_get_last_applied_idx(rr->raft) : 0);
    RedisModule_InfoAddFieldULongLong(ctx, "file_size", rr->log ? rr->log->file_size : 0);
    RedisModule_InfoAddFieldULongLong(ctx, "cache_memory_size", rr->logcache ? rr->logcache->entries_memsize : 0);
    RedisModule_InfoAddFieldLongLong(ctx, "cache_entries", rr->logcache ? rr->logcache->len : 0);
    RedisModule_InfoAddFieldULongLong(ctx, "client_attached_entries", rr->client_attached_entries);
    RedisModule_InfoAddFieldULongLong(ctx, "fsync_count", rr->log ? rr->log->fsync_count : 0);
    RedisModule_InfoAddFieldULongLong(ctx, "fsync_max_microseconds", rr->log ? rr->log->fsync_max : 0);

    uint64_t avg = 0;
    if (rr->log && rr->log->fsync_count) {
        avg = rr->log->fsync_total / rr->log->fsync_count;
    }
    RedisModule_InfoAddFieldULongLong(ctx, "fsync_avg_microseconds", avg);

    RedisModule_InfoAddSection(ctx, "snapshot");
    RedisModule_InfoAddFieldCString(ctx, "snapshot_in_progress", rr->snapshot_in_progress ? "yes" : "no");
    RedisModule_InfoAddFieldULongLong(ctx, "snapshots_loaded", rr->snapshots_loaded);
    RedisModule_InfoAddFieldULongLong(ctx, "snapshots_created", rr->snapshots_created);

    RedisModule_InfoAddSection(ctx, "clients");
    RedisModule_InfoAddFieldULongLong(ctx, "proxy_reqs", rr->proxy_reqs);
    RedisModule_InfoAddFieldULongLong(ctx, "proxy_failed_reqs", rr->proxy_failed_reqs);
    RedisModule_InfoAddFieldULongLong(ctx, "proxy_failed_responses", rr->proxy_failed_responses);
    RedisModule_InfoAddFieldULongLong(ctx, "proxy_outstanding_reqs", rr->proxy_outstanding_reqs);

    RedisModule_InfoAddSection(ctx, "stats");
    RedisModule_InfoAddFieldULongLong(ctx, "appendreq_received", rr->appendreq_received);
    RedisModule_InfoAddFieldULongLong(ctx, "appendreq_with_entry_received", rr->appendreq_with_entry_received);
    RedisModule_InfoAddFieldULongLong(ctx, "snapshotreq_received", rr->snapshotreq_received);
    RedisModule_InfoAddFieldULongLong(ctx, "exec_throttled", rr->exec_throttled);
}

static int registerRaftCommands(RedisModuleCtx *ctx)
{
    /* Register commands.
     *
     * NOTE: Internal RedisRaft module commands must also be set with
     * their apropriate flags in commands.c, typically with a
     * CMD_SPEC_DONT_INTERCEPT flag.
     * */
    if (RedisModule_CreateCommand(ctx, "raft", cmdRaft,
                                  "write", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "raft.entry", cmdRaftEntry,
                                  "write", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "raft.cluster", cmdRaftCluster,
                                  "admin", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "raft.shardgroup", cmdRaftShardGroup,
                                  "admin", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "raft.node", cmdRaftNode,
                                  "admin", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "raft.ae", cmdRaftAppendEntries,
                                  "write", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "raft.transfer_leader",
                                  cmdRaftTransferLeader,
                                  "admin", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "raft.timeout_now", cmdRaftTimeoutNow,
                                  "admin", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "raft.requestvote", cmdRaftRequestVote,
                                  "admin", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "raft.snapshot", cmdRaftSnapshot,
                                  "admin", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "raft.debug", cmdRaftDebug,
                                  "admin", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "raft.nodeshutdown", cmdRaftNodeShutdown,
                                  "admin", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "raft._sort_reply", cmdRaftSort,
                                  "admin", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "raft._reject_random_command",
                                  cmdRaftRandom,
                                  "admin", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "raft.import", cmdRaftImport,
                                  "admin", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "raft.scan", cmdRaftScan,
                                  "admin", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    RedisRaftType = RedisModule_CreateDataType(ctx, REDIS_RAFT_DATATYPE_NAME,
                                               REDIS_RAFT_DATATYPE_ENCVER,
                                               &RedisRaftTypeMethods);
    if (RedisRaftType == NULL) {
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}

void moduleChangeCallback(RedisModuleCtx *ctx, RedisModuleEvent e, uint64_t sub, void *data)
{
    REDISMODULE_NOT_USED(e);

    /* rebuild the command spec table on any module change */
    CommandSpecTableRebuild(redis_raft.ctx, redis_raft.commands_spec_table, redis_raft.config.ignored_commands);
}

RRStatus RedisRaftCtxInit(RedisRaftCtx *rr, RedisModuleCtx *ctx)
{
    *rr = (RedisRaftCtx){
        .state = REDIS_RAFT_UNINITIALIZED,
        .ctx = RedisModule_GetDetachedThreadSafeContext(ctx),
    };

    CommandSpecTableInit(rr->ctx, &rr->commands_spec_table);

    if (ConfigInit(rr->ctx, &rr->config) != RR_OK) {
        LOG_WARNING("Failed to init configuration");
        goto error;
    }

    /* Client state for MULTI/ASKING support */
    rr->client_state = RedisModule_CreateDict(rr->ctx);
    rr->locked_keys = RedisModule_CreateDict(rr->ctx);

    /* acl -> user dictionary */
    rr->acl_dict = RedisModule_CreateDict(ctx);

    /* Cluster configuration */
    ShardingInfoInit(rr->ctx, &rr->sharding_info);

    /* Raft log exists -> go into RAFT_LOADING state:
     *
     * Redis will load RDB as a snapshot, if it exists. When done,
     * handleLoadingState() will be called, initialize Raft library and load
     * log file.
     *
     * Raft log does not exist -> go into RAFT_UNINITIALIZED state:
     *
     * Nothing will happen until users will initiate a RAFT.CLUSTER INIT
     * or RAFT.CLUSTER JOIN command.
     */
    if (RaftMetaRead(&rr->meta, rr->config.log_filename) == RR_OK) {
        rr->log = RaftLogOpen(rr->config.log_filename, &rr->config, 0);
        if (rr->log) {
            rr->state = REDIS_RAFT_LOADING;
        }
    }

    RedisModule_CreateTimer(rr->ctx, rr->config.periodic_interval, callRaftPeriodic, rr);
    RedisModule_CreateTimer(rr->ctx, rr->config.reconnect_interval, callHandleNodeStates, rr);
    threadPoolInit(&rr->thread_pool, 5);
    fsyncThreadStart(&rr->fsyncThread, handleFsyncCompleted);

    return RR_OK;

error:
    RedisRaftCtxClear(rr);
    *rr = (RedisRaftCtx){0};
    return RR_ERROR;
}

void RedisRaftCtxClear(RedisRaftCtx *rr)
{
    if (rr->raft) {
        raft_destroy(rr->raft);
        rr->raft = NULL;
    }

    if (rr->ctx) {
        RedisModule_FreeThreadSafeContext(rr->ctx);
        rr->ctx = NULL;
    }

    RaftLogFree(rr->log);
    rr->log = NULL;

    if (rr->logcache) {
        EntryCacheFree(rr->logcache);
        rr->logcache = NULL;
    }

    if (rr->transfer_req) {
        RaftReqFree(rr->transfer_req);
        rr->transfer_req = NULL;
    }

    if (rr->migrate_req) {
        RaftReqFree(rr->migrate_req);
        rr->migrate_req = NULL;
    }

    if (rr->sharding_info) {
        ShardingInfoFree(rr->ctx, rr->sharding_info);
        rr->sharding_info = NULL;
    }

    if (rr->client_state) {
        RedisModule_FreeDict(rr->ctx, rr->client_state);
        rr->client_state = NULL;
    }

    if (rr->debug_req) {
        RaftReqFree(rr->debug_req);
        rr->debug_req = NULL;
    }

    if (rr->locked_keys) {
        RedisModule_FreeDict(rr->ctx, rr->locked_keys);
        rr->locked_keys = NULL;
    }

    CommandSpecTableClear(rr->commands_spec_table);
}

void RedisRaftFreeGlobals()
{
    if (redisraft_log_ctx) {
        RedisModule_FreeThreadSafeContext(redisraft_log_ctx);
    }
    RedisRaftCtxClear(&redis_raft);
}

__attribute__((__unused__)) int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    int ret;

    ret = RedisModule_Init(ctx, "raft", 1, REDISMODULE_APIVER_1);
    if (ret != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_NOTICE,
                    "RedisRaft version %s [%s]",
                    REDISRAFT_VERSION, REDISRAFT_GIT_SHA1);

    const int MIN_SUPPORTED_REDIS_VERSION = 0x00070000;
    if (!RMAPI_FUNC_SUPPORTED(RedisModule_GetServerVersion) ||
        RedisModule_GetServerVersion() < MIN_SUPPORTED_REDIS_VERSION) {
        RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_WARNING,
                        "RedisRaft requires Redis 7.0 or above");
        return REDISMODULE_ERR;
    }

    /* Create a logging context */
    redisraft_log_ctx = RedisModule_GetDetachedThreadSafeContext(ctx);

    RedisModule_RegisterInfoFunc(ctx, handleInfo);
    RedisModule_RegisterCommandFilter(ctx, interceptRedisCommands, 0);

    if (registerRaftCommands(ctx) == RR_ERROR) {
        LOG_WARNING("Failed to register commands");
        goto error;
    }

    ret = RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_ClientChange,
                                             handleClientEvent);
    if (ret != REDISMODULE_OK) {
        LOG_WARNING("Failed to subscribe to server events.");
        goto error;
    }

    ret = RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_EventLoop,
                                             beforeSleep);
    if (ret != REDISMODULE_OK) {
        LOG_WARNING("Failed to subscribe to server events.");
        goto error;
    }

    ret = RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_Config,
                                             ConfigRedisEventCallback);
    if (ret != REDISMODULE_OK) {
        LOG_WARNING("Failed to subscribe to server events.");
        goto error;
    }

    if (RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_ModuleChange,
                                           moduleChangeCallback) != REDISMODULE_OK) {
        LOG_WARNING("Failed to subscribe to server events.");
        goto error;
    }

    RedisRaftCtx *rr = &redis_raft;

    if (RedisRaftCtxInit(rr, ctx) == RR_ERROR) {
        LOG_WARNING("Failed to init redis raft context");
        goto error;
    }

    LOG_NOTICE("Raft module loaded, state is '%s'", getStateStr(rr));
    return REDISMODULE_OK;
error:
    RedisRaftFreeGlobals();
    return REDISMODULE_ERR;
}
