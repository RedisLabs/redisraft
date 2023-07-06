/*
 * Copyright Redis Ltd. 2020 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "entrycache.h"
#include "log.h"
#include "redisraft.h"

#include <stdlib.h>
#include <string.h>
#include <strings.h>

#define RAFTLIB_TRACE(fmt, ...) TRACE_MODULE(RAFTLIB, fmt, ##__VA_ARGS__)

const char *RaftReqTypeStr[] = {
    [0] = "<undef>",
    [RR_GENERIC] = "RR_GENERIC",
    [RR_CLUSTER_JOIN] = "RR_CLUSTER_JOIN",
    [RR_CFGCHANGE_ADDNODE] = "RR_CFGCHANGE_ADDNODE",
    [RR_CFGCHANGE_REMOVENODE] = "RR_CFGCHANGE_REMOVENODE",
    [RR_REDISCOMMAND] = "RR_REDISCOMMAND",
    [RR_DEBUG] = "RR_DEBUG",
    [RR_SHARDGROUP_ADD] = "RR_SHARDGROUP_ADD",
    [RR_SHARDGROUPS_REPLACE] = "RR_SHARDGROUPS_REPLACE",
    [RR_SHARDGROUP_LINK] = "RR_SHARDGROUP_LINK",
    [RR_TRANSFER_LEADER] = "RR_TRANSFER_LEADER",
    [RR_IMPORT_KEYS] = "RR_IMPORT_KEYS",
    [RR_MIGRATE_KEYS] = "RR_MIGRATE_KEYS",
    [RR_DELETE_UNLOCK_KEYS] = "RR_DELETE_UNLOCK_KEYS",
    [RR_END_SESSION] = "RR_END_SESSION",
    [RR_CLIENT_UNBLOCK] = "RR_CLIENT_UNBLOCK",
};

/* Forward declarations */
static void configureFromSnapshot(RedisRaftCtx *rr);
static void applyShardGroupChange(RedisRaftCtx *rr, raft_entry_t *entry, RaftReq *req);
static void replaceShardGroups(RedisRaftCtx *rr, raft_entry_t *entry, RaftReq *req);

/* ------------------------------------ Common helpers ------------------------------------ */

void shutdownAfterRemoval(RedisRaftCtx *rr)
{
    LOG_NOTICE("*** NODE REMOVED, SHUTTING DOWN.");

    MetadataArchiveFile(&rr->meta);
    LogArchiveFiles(&rr->log);

    if (rr->config.rdb_filename) {
        archiveSnapshot(rr);
    }

    shutdownServer(rr);
}

RaftReq *entryDetachRaftReq(RedisRaftCtx *rr, raft_entry_t *entry)
{
    RaftReq *req = entry->user_data;

    if (!req) {
        return NULL;
    }

    entry->user_data = NULL;
    entry->free_func = NULL;
    rr->client_attached_entries--;

    return req;
}

/* Set up a Raft log entry with an attached RaftReq. We use this when a user command provided
 * in a RaftReq should keep the client blocked until the log entry is committed and applied.
 */
void entryFreeAttachedRaftReq(raft_entry_t *ety)
{
    RaftReq *req = entryDetachRaftReq(&redis_raft, ety);

    if (req) {
        RedisModule_ReplyWithError(req->ctx, "TIMEOUT not committed yet");
        RaftReqFree(req);
    }

    RedisModule_Free(ety);
}

/* Attach a RaftReq to a Raft log entry. The common case for this is when a user request
 * needs to block until it gets committed, and only then a reply should be produced.
 *
 * To do that, we link the RaftReq to the Raft log entry and keep the client blocked.
 * When the entry will later reach the apply flow, the linkage to the RaftReq will
 * make it possible to generate the reply to the user.
 */
void entryAttachRaftReq(RedisRaftCtx *rr, raft_entry_t *entry, RaftReq *req)
{
    entry->user_data = req;
    entry->free_func = entryFreeAttachedRaftReq;
    rr->client_attached_entries++;
}

/* ----------------------------- Log Execution ------------------------------ */

static bool isSharding(RedisRaftCtx *rr)
{
    return rr->sharding_info->is_sharding;
}

typedef enum KeysStatus {
    ALL_EXIST,
    SOME_EXIST,
    NONE_EXIST,
    LOCKED_EXIST,
} KeysStatus;

static KeysStatus validateKeyExistence(RedisRaftCtx *rr, RaftRedisCommandArray *cmds)
{
    int total_keys = 0;
    int found = 0;
    unsigned int locked = 0;

    for (int i = 0; i < cmds->len; i++) {
        RaftRedisCommand *cmd = cmds->commands[i];

        /* Iterate command keys */
        int num_keys = 0;
        int *keyindex = RedisModule_GetCommandKeys(rr->ctx, cmd->argv, cmd->argc, &num_keys);
        total_keys += num_keys;

        for (int j = 0; j < num_keys; j++) {
            if (RedisModule_KeyExists(rr->ctx, cmd->argv[keyindex[j]])) {
                found++;

                /* test if locked */
                int nokey;
                RedisModule_DictGet(rr->locked_keys, cmd->argv[keyindex[j]], &nokey);
                if (!nokey) {
                    locked++;
                }
            }
        }
        RedisModule_Free(keyindex);
    }

    if (locked > 0) {
        return LOCKED_EXIST;
    }

    if (found != total_keys) {
        return (found == 0) ? NONE_EXIST : SOME_EXIST;
    }

    return ALL_EXIST;
}

/* figure out the "owner shardgroup"
 *
 * the owner shardgroup is either
 *
 * 1) a shardgroup that owns the slot as a stable slot
 * 2) a shardgroup that owns the slot as a migrating slot.  by definition is this shardgroup is local, then it
 *    can't also be importing (a single RedisRaft cluster cannot be both importing and migrating the same slot
 * 3) a shardgroup marked as local (i.e. corresponding to this cluster) that owns the slot as an importing slot and
 *    has a RaftRedisCommandArray marked as asking
 */
static ShardGroup *getSlotShardGroup(RedisRaftCtx *rr, unsigned int slot, bool asking)
{
    ShardGroup *sg = rr->sharding_info->stable_slots_map[slot];
    if (sg != NULL) {
        return sg;
    }

    sg = rr->sharding_info->migrating_slots_map[slot];
    if (sg && sg->local) {
        return sg;
    }

    if (asking) {
        ShardGroup *isg = rr->sharding_info->importing_slots_map[slot];
        if (isg && isg->local) {
            return isg;
        }
    }

    return sg;
}

static RRStatus validateRaftRedisCommandArray(RedisRaftCtx *rr, RedisModuleCtx *reply_ctx,
                                              RaftRedisCommandArray *cmds, unsigned int slot)
{
    RedisModule_Assert(slot <= REDIS_RAFT_HASH_MAX_SLOT);

    /* Make sure hash slot is mapped and handled locally. */
    ShardGroup *sg = getSlotShardGroup(rr, slot, cmds->asking);
    if (!sg) {
        if (reply_ctx) {
            RedisModule_ReplyWithError(reply_ctx, "CLUSTERDOWN Hash slot is not served");
        }
        return RR_ERROR;
    }

    SlotRangeType slot_type = SLOTRANGE_TYPE_UNDEF;
    for (size_t i = 0; i < sg->slot_ranges_num; i++) {
        if (sg->slot_ranges[i].start_slot <= slot && slot <= sg->slot_ranges[i].end_slot) {
            slot_type = sg->slot_ranges[i].type;
            break;
        }
    }

    if (slot_type == SLOTRANGE_TYPE_UNDEF) {
        if (reply_ctx) {
            RedisModule_ReplyWithError(reply_ctx, "ERR internal error, couldn't associate a shardgroup slot to this request");
        }
        return RR_ERROR;
    }

    if (!sg->local) {
        if (reply_ctx) {
            sg->next_redir = (sg->next_redir + 1) % sg->nodes_num;
            replyRedirect(reply_ctx, slot, &sg->nodes[sg->next_redir].addr);
        }

        return RR_ERROR;
    }

    /* if our keys belong to a local migrating/importing slot, all keys must exist */
    if (slot_type == SLOTRANGE_TYPE_MIGRATING) {
        switch (validateKeyExistence(rr, cmds)) {
            case SOME_EXIST:
            case LOCKED_EXIST:
                if (reply_ctx) {
                    RedisModule_ReplyWithError(reply_ctx, "TRYAGAIN");
                }
                return RR_ERROR;
            case NONE_EXIST:
                if (reply_ctx) {
                    ShardGroup *isg;

                    isg = rr->sharding_info->importing_slots_map[slot];
                    if (isg) {
                        replyAsk(reply_ctx, slot, &isg->nodes[0].addr);
                    } else {
                        RedisModule_ReplyWithError(reply_ctx, "ERR no importing shard group to ask");
                    }
                }
                return RR_ERROR;
            case ALL_EXIST:
                return RR_OK;
        }
    } else if (slot_type == SLOTRANGE_TYPE_IMPORTING) {
        switch (validateKeyExistence(rr, cmds)) {
            case SOME_EXIST:
            case NONE_EXIST:
            case LOCKED_EXIST:
                if (reply_ctx) {
                    RedisModule_ReplyWithError(reply_ctx, "TRYAGAIN");
                }
                return RR_ERROR;
            case ALL_EXIST:
                return RR_OK;
        }
    }

    return RR_OK;
}

/* When sharding is enabled, handle sharding aspects before processing
 * the request:
 *
 * 1. Compute hash slot of all associated keys and validate there's no cross-slot
 *    violation.
 * 2. Update the request's hash_slot for future reference.
 * 3. If the hash slot is associated with a foreign ShardGroup, perform a redirect.
 * 4. If the hash slot is not mapped, produce a CLUSTERDOWN error.
 */
static RRStatus handleSharding(RedisRaftCtx *rr, RedisModuleCtx *ctx, RaftRedisCommandArray *cmds)
{
    int slot;

    if (!isSharding(rr)) {
        return RR_OK;
    }

    if (HashSlotCompute(rr, cmds, &slot) != RR_OK) {
        if (ctx) {
            replyCrossSlot(ctx);
        }
        return RR_ERROR;
    }

    /* If commands have no keys, continue */
    if (slot == -1) {
        return RR_OK;
    }

    return validateRaftRedisCommandArray(rr, ctx, cmds, slot);
}

/* returns the client session object for this CommandArray if applicable
 * starts/creates it if necessary
 */
static void *getClientSession(RedisRaftCtx *rr, RaftRedisCommandArray *cmds, bool local)
{
    unsigned long long id = cmds->client_id;
    int nokey;

    ClientSession *client_session = RedisModule_DictGetC(rr->client_session_dict, &id, sizeof(id), &nokey);

    if (nokey) {
        RaftRedisCommand *c = cmds->commands[0];
        size_t cmd_len;
        const char *cmd = RedisModule_StringPtrLen(c->argv[0], &cmd_len);

        if (cmd_len == 5 && strncasecmp("watch", cmd, cmd_len) == 0) {
            client_session = RedisModule_Alloc(sizeof(ClientSession));
            client_session->client_id = id;
            client_session->local = local;
            RedisModule_DictSetC(rr->client_session_dict, &id, sizeof(id), client_session);
        }
    }

    return client_session;
}

RedisModuleUser *RaftGetACLUser(RedisModuleCtx *ctx, RedisRaftCtx *rr, RaftRedisCommandArray *cmds)
{
    int nokey;
    RedisModuleUser *user = RedisModule_DictGet(rr->acl_dict, cmds->acl, &nokey);
    if (nokey) {
        char user_name[64];
        /* Note: This assumes we don't delete "acl users" */
        uint64_t count = RedisModule_DictSize(rr->acl_dict);
        count++;
        snprintf(user_name, 64, "redis_raft%lu", (unsigned long) count);
        user = RedisModule_CreateModuleUser(user_name);
        RedisModule_Assert(user != NULL);

        const char *acl_str = RedisModule_StringPtrLen(cmds->acl, NULL);
        int ret = RedisModule_SetModuleUserACLString(ctx, user, acl_str, NULL);
        RedisModule_Assert(ret == REDISMODULE_OK);

        RedisModule_DictSet(rr->acl_dict, cmds->acl, user);
    }

    return user;
}

void handleUnblock(RedisModuleCtx *ctx, RedisModuleCallReply *reply, void *private_data)
{
    UNUSED(ctx);

    BlockedCommand *bc = private_data;
    if (bc->req) {
        RedisModule_ReplyWithCallReply(bc->req->ctx, reply);
        RaftReqFree(bc->req);
    }

    RedisModule_FreeCallReply(reply);
    deleteBlockedCommand(bc->idx);
    freeBlockedCommand(bc);
}

/* Execute all commands in a specified RaftRedisCommandArray.
 *
 * If reply_ctx is non-NULL, replies are delivered to it.
 * Otherwise, no replies are delivered.
 *
 * Now handles blocking commands (ex: brpop) as well.
 * If a blocking command is issued, we execute normally, but now use the return value of RM_Call()
 * to indicate if we are blocking (i.e. should the req not be freed).  If RM_Call returns a promise, we
 * return the promise to the caller, otherwise we return NULL.
 */
RedisModuleCallReply *RaftExecuteCommandArray(RedisRaftCtx *rr,
                                              RaftReq *req,
                                              RaftRedisCommandArray *cmds)
{
    RedisModuleCallReply *reply = NULL;
    RedisModuleUser *user = NULL;
    RedisModuleCtx *ctx = req ? req->ctx : rr->ctx;

    if (cmds->acl) {
        user = RaftGetACLUser(rr->ctx, rr, cmds);
    }

    if (rr->config.log_delay_apply) {
        usleep(rr->config.log_delay_apply);
    }

    /* When we're in cluster mode, go through handleSharding. This will perform
     * hash slot validation and return an error / redirection if necessary. */
    if (handleSharding(rr, req ? req->ctx : NULL, cmds) != RR_OK) {
        return NULL; /* sharding error, so even if blocking command, don't */
    }

    ClientSession *client_session = getClientSession(rr, cmds, req != NULL);
    (void) client_session; /* unused for now */

    if (cmds->cmd_flags & CMD_SPEC_BLOCKING) {
        replaceBlockingTimeout(cmds);
    }

    for (int i = 0; i < cmds->len; i++) {
        RaftRedisCommand *c = cmds->commands[i];

        size_t cmdlen;
        const char *cmd = RedisModule_StringPtrLen(c->argv[0], &cmdlen);

        /* We need to handle MULTI as a special case:
        * 1. Skip the command (no need to execute MULTI in a Module context).
        * 2. If we're returning a response, group it as an array (multibulk)
        * 3. When executing the MULTI/EXEC we don't really need to wrap it
        *    because Redis wraps all module commands in MULTI/EXEC
        *    (although no harm is done).
        */
        if (i == 0 && cmdlen == 5 && !strncasecmp(cmd, "MULTI", 5)) {
            if (req) {
                RedisModule_ReplyWithArray(req->ctx, cmds->len - 1);
            }

            continue;
        }

        int old_entered_eval = rr->entered_eval;

        if ((cmdlen == 4 && !strncasecmp(cmd, "eval", 4)) ||
            (cmdlen == 7 && !strncasecmp(cmd, "eval_ro", 7)) ||
            (cmdlen == 7 && !strncasecmp(cmd, "evalsha", 7)) ||
            (cmdlen == 10 && !strncasecmp(cmd, "evalsha_ro", 10)) ||
            (cmdlen == 5 && !strncasecmp(cmd, "fcall", 5)) ||
            (cmdlen == 8 && !strncasecmp(cmd, "fcall_ro", 8))) {
            rr->entered_eval = 1;
        }

        /* Explanation:
         * When we have an ACL, we will have a user set on the context, so need "C"
         */
        char *resp_call_fmt;
        if (cmds->cmd_flags & CMD_SPEC_MULTI) {
            /* don't block inside a MULTI */
            resp_call_fmt = cmds->acl ? "CE0v" : "E0v";
        } else {
            resp_call_fmt = cmds->acl ? "KCE0v" : "KE0v";
        }

        enterRedisModuleCall();
        RedisModule_SetContextUser(ctx, user);
        reply = RedisModule_Call(ctx, cmd, resp_call_fmt, &c->argv[1], c->argc - 1);
        RedisModule_SetContextUser(ctx, NULL);
        exitRedisModuleCall();
        rr->entered_eval = old_entered_eval;

        if (RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_PROMISE) {
            /* reply to client if we didn't block */
            if (req) {
                RedisModule_ReplyWithCallReply(req->ctx, reply);
            }

            /* only free non reply on non blocked commands */
            RedisModule_FreeCallReply(reply);

            /* we return non blocked commands as NULL */
            reply = NULL;
        } else {
            /* if we're blocking, there should only have been one command */
            RedisModule_Assert(cmds->len == 1); /* should only block when there's a single command */
        }
    }

    /* if blocking (this won't be NULL), return it to the caller, to setup callback / saving state */
    return reply;
}

static void lockKeys(RedisRaftCtx *rr, raft_entry_t *entry, RaftReq *req)
{
    RedisModule_Assert(entry->type == RAFT_LOGTYPE_LOCK_KEYS);

    /* FIXME: can optimize this for leader by getting it out of the req, keeping code simple for now */
    size_t num_keys;
    RedisModuleString **keys = RaftRedisLockKeysDeserialize(entry->data, entry->data_len, &num_keys);

    /* sanity check keys all belong to same slot */
    int slot = -1;
    for (size_t i = 0; i < num_keys; i++) {
        RedisModuleString *key = keys[i];

        unsigned int thisslot = keyHashSlotRedisString(key);
        if (slot == -1) {
            slot = (int) thisslot;
        } else {
            if (slot != (int) thisslot) {
                if (req) {
                    replyCrossSlot(req->ctx);
                }
                goto error;
            }
        }
    }

    if (slot == -1) { /* should be impossible, as keys should be listed up front */
        if (req) {
            RedisModule_ReplyWithSimpleString(req->ctx, "ERR lockKeys called without any keys to lock");
        }
        goto error;
    }

    ShardingInfo *si = rr->sharding_info;
    if (!si->migrating_slots_map[slot]) {
        if (req) {
            RedisModule_ReplyWithError(req->ctx, "ERR keys are not migratable");
        }
        goto error;
    }

    if (!si->migrating_slots_map[slot]->local) {
        if (req) {
            RedisModule_ReplyWithError(req->ctx, "ERR This RedisRaft cluster doesn't own these keys");
        }
        goto error;
    }

    if (!si->importing_slots_map[slot]) {
        if (req) {
            RedisModule_ReplyWithError(req->ctx, "ERR no RedisRaft cluster to import keys into");
        }
        goto error;
    }

    if (si->importing_slots_map[slot]->local) {
        if (req) {
            RedisModule_ReplyWithError(req->ctx, "ERR trying to import keys into self RedisCluster");
        }
        goto error;
    }

    for (size_t i = 0; i < num_keys; i++) {
        if (RedisModule_KeyExists(rr->ctx, keys[i])) {
            size_t str_len;
            const char *str = RedisModule_StringPtrLen(keys[i], &str_len);
            MIGRATION_TRACE("Locking key: %.*s", (int) str_len, str);

            RedisModule_DictSet(rr->locked_keys, keys[i], NULL);
        }
    }

    if (req) {
        memcpy(req->r.migrate_keys.shard_group_id, si->importing_slots_map[slot]->id, RAFT_DBID_LEN);
        MigrateKeys(rr, req);
    }
    goto exit;

error:
    if (req) {
        RaftReqFree(req);
    }

exit:
    for (size_t i = 0; i < num_keys; i++) {
        RedisModule_FreeString(rr->ctx, keys[i]);
    }
    RedisModule_Free(keys);
}

static void unlockDeleteKeys(RedisRaftCtx *rr, raft_entry_t *entry, RaftReq *req)
{
    RedisModule_Assert(entry->type == RAFT_LOGTYPE_DELETE_UNLOCK_KEYS);

    size_t num_keys;
    RedisModuleString **keys;

    keys = RaftRedisLockKeysDeserialize(entry->data, entry->data_len, &num_keys);

    enterRedisModuleCall();
    RedisModuleCallReply *reply = RedisModule_Call(redis_raft.ctx, "del", "v", keys, num_keys);
    exitRedisModuleCall();
    RedisModule_Assert(reply != NULL);
    RedisModule_FreeCallReply(reply);

    for (size_t i = 0; i < num_keys; i++) {
        size_t str_len;
        const char *str = RedisModule_StringPtrLen(keys[i], &str_len);

        MIGRATION_TRACE("Unlocking key: %.*s", (int) str_len, str);

        RedisModule_DictDel(rr->locked_keys, keys[i], NULL);
        RedisModule_FreeString(rr->ctx, keys[i]);
    }
    RedisModule_Free(keys);

    if (req) {
        RedisModule_ReplyWithSimpleString(req->ctx, "OK");
        RaftReqFree(req);
    }
}

static void freeClientSession(void *client_session)
{
    RedisModule_Free(client_session);
}

static void endClientSession(RedisRaftCtx *rr, unsigned long long id)
{
    void *client_session = NULL;
    RedisModule_DictDelC(rr->client_session_dict, &id, sizeof(id), &client_session);
    if (client_session) {
        freeClientSession(client_session);
    }
}

static void handleEndClientSession(RedisRaftCtx *rr, raft_entry_t *entry, RaftReq *req)
{
    RedisModule_Assert(entry->type == RAFT_LOGTYPE_END_SESSION);

    unsigned long long id = entry->session;
    endClientSession(rr, id);

    if (req) {
        RedisModule_ReplyWithSimpleString(req->ctx, "OK");
        RaftReqFree(req);
    }
}

void clearClientSessions(RedisRaftCtx *rr)
{
    ClientSession *client_session;
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(rr->client_session_dict, "^", NULL, 0);
    while (RedisModule_DictNextC(iter, NULL, (void **) &client_session) != NULL) {
        if (client_session->local) {
            RedisModule_DeauthenticateAndCloseClient(rr->ctx, client_session->client_id);
        }
        freeClientSession(client_session);
    }
    RedisModule_DictIteratorStop(iter);
    RedisModule_FreeDict(rr->ctx, rr->client_session_dict);
    rr->client_session_dict = RedisModule_CreateDict(rr->ctx);
}

static void timeoutBlockedCommand(RedisRaftCtx *rr, raft_entry_t *entry, RaftReq *req)
{
    raft_index_t idx;
    bool error;

    int ret = RaftRedisDeserializeTimeout(entry->data, entry->data_len, &idx, &error);
    RedisModule_Assert(ret == RR_OK);

    BlockedCommand *bc = getBlockedCommand(idx);
    if (!bc) {
        /* unblock handler called before timeout was applied */
        if (req) {
            RedisModule_ReplyWithLongLong(req->ctx, 0);
            RaftReqFree(req);
        }
        return;
    }

    /* In future might have to handle abort failing, but for plain redis commands this is a panic condition */
    ret = RedisModule_CallReplyPromiseAbort(bc->reply, NULL);

    if (ret == REDISMODULE_OK) {
        if (bc->req) {
            /* responding to blocked command */
            if (error) {
                RedisModule_ReplyWithError(bc->req->ctx, "UNBLOCKED client unblocked via CLIENT UNBLOCK");
            } else {
                if ((strncasecmp(bc->command, "bzpopmin", 8) == 0) ||
                    (strncasecmp(bc->command, "bzpopmax", 8) == 0) ||
                    (strncasecmp(bc->command, "bzmpop", 6) == 0)) {
                    RedisModule_ReplyWithNullArray(bc->req->ctx);
                } else {
                    RedisModule_ReplyWithNull(bc->req->ctx);
                }
            }

            RaftReqFree(bc->req);
        }

        deleteBlockedCommand(bc->idx);
        freeBlockedCommand(bc);
    }

    if (req) {
        /* respond to initiator of the timeout, i.e. CLIENT UNBLOCK */
        if (ret == REDISMODULE_OK) {
            RedisModule_ReplyWithLongLong(req->ctx, 1);
        } else {
            RedisModule_ReplyWithLongLong(req->ctx, 0);
        }

        RaftReqFree(req);
    }
}

/*
 * Execution of Raft log on the local instance.
 *
 * There are two variants:
 * 1) Execution of a raft entry received from another node.
 * 2) Execution of a locally initiated command.
 */
static void executeLogEntry(RedisRaftCtx *rr, raft_entry_t *entry, raft_index_t entry_idx, RaftReq *req)
{
    RedisModule_Assert(entry->type == RAFT_LOGTYPE_NORMAL);

    RaftRedisCommandArray tmp = {0};
    RaftRedisCommandArray *cmds;

    /* setup for req/non req nodes needs to mirror teardown below */
    if (req) {
        cmds = &req->r.redis.cmds;
    } else {
        if (RaftRedisCommandArrayDeserialize(&tmp,
                                             entry->data,
                                             entry->data_len) != RR_OK) {
            PANIC("Invalid Raft entry");
        }
        tmp.client_id = entry->session;
        cmds = &tmp;
    }

    RedisModuleCallReply *reply = RaftExecuteCommandArray(rr, req, cmds);

    if (reply == NULL) {
        /* setup for req/non req nodes needs to mirror teardown below */
        if (req) { /*  node instance where client issued command */
            RaftReqFree(req);
        } else {
            RaftRedisCommandArrayFree(cmds);
        }
    } else {
        size_t cmdstr_len;
        const char *cmdstr = RedisModule_StringPtrLen(cmds->commands[0]->argv[0], &cmdstr_len);
        BlockedCommand *bc = allocBlockedCommand(cmdstr, entry_idx, entry->session, entry->data, entry->data_len, req, reply);
        addBlockedCommand(bc);
        RedisModule_CallReplyPromiseSetUnblockHandler(reply, handleUnblock, bc);
        if (req) {
            /* nothing really happens here for now, but for symmetry, keeping it in place */
            ;
        } else {
            RaftRedisCommandArrayFree(cmds);
        }
    }

    /* Update snapshot info in Redis dataset. This must be done now so it's
     * always consistent with what we applied and we never end up applying
     * an entry onto a snapshot where it was applied already.
     */
    rr->snapshot_info.last_applied_term = entry->term;
    rr->snapshot_info.last_applied_idx = entry_idx;
}

static void raftSendNodeShutdown(raft_node_t *raft_node)
{
    if (!raft_node) {
        return;
    }

    Node *node = raft_node_get_udata(raft_node);
    if (!node) {
        return;
    }

    if (!ConnIsConnected(node->conn)) {
        NODE_TRACE(node, "not connected, state=%s", ConnGetStateStr(node->conn));
        return;
    }

    if (redisAsyncCommand(ConnGetRedisCtx(node->conn), NULL, NULL,
                          "RAFT.NODESHUTDOWN %d",
                          (int) raft_node_get_id(raft_node)) != REDIS_OK) {
        NODE_TRACE(node, "failed to send raft.nodeshutdown");
    }
}

/* ------------------------------------ RequestVote ------------------------------------ */

static void handleRequestVoteResponse(redisAsyncContext *c, void *r, void *privdata)
{
    Node *node = privdata;
    RedisRaftCtx *rr = node->rr;

    redisReply *reply = r;

    NodeDismissPendingResponse(node);
    if (!reply) {
        NODE_LOG_DEBUG(node, "RAFT.REQUESTVOTE failed: connection dropped.");
        ConnMarkDisconnected(node->conn);
        return;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        NODE_LOG_DEBUG(node, "RAFT.REQUESTVOTE error: %s", reply->str);
        return;
    }

    if (reply->type != REDIS_REPLY_ARRAY || reply->elements != 4 ||
        reply->element[0]->type != REDIS_REPLY_INTEGER ||
        reply->element[1]->type != REDIS_REPLY_INTEGER ||
        reply->element[2]->type != REDIS_REPLY_INTEGER ||
        reply->element[3]->type != REDIS_REPLY_INTEGER) {
        NODE_LOG_WARNING(node, "invalid RAFT.REQUESTVOTE reply");
        return;
    }

    raft_requestvote_resp_t response = {
        .prevote = reply->element[0]->integer,
        .request_term = reply->element[1]->integer,
        .term = reply->element[2]->integer,
        .vote_granted = reply->element[3]->integer,
    };

    raft_node_t *raft_node = raft_get_node(rr->raft, node->id);
    if (!raft_node) {
        NODE_LOG_DEBUG(node, "RAFT.REQUESTVOTE stale reply.");
        return;
    }

    int ret;
    if ((ret = raft_recv_requestvote_response(
             rr->raft,
             raft_node,
             &response)) != 0) {
        LOG_DEBUG("raft_recv_requestvote_response failed, error %d", ret);
    }
}

static int raftSendRequestVote(raft_server_t *raft, void *user_data,
                               raft_node_t *raft_node, raft_requestvote_req_t *msg)
{
    Node *node = (Node *) raft_node_get_udata(raft_node);

    if (!ConnIsConnected(node->conn)) {
        NODE_TRACE(node, "not connected, state=%s", ConnGetStateStr(node->conn));
        return 0;
    }

    /* RAFT.REQUESTVOTE <target_node_id> <src_node_id> <prevote>:<term>:<candidate_id>:<last_log_idx>:<last_log_term> */
    if (redisAsyncCommand(ConnGetRedisCtx(node->conn), handleRequestVoteResponse,
                          node, "RAFT.REQUESTVOTE %d %d %d:%ld:%d:%ld:%ld",
                          raft_node_get_id(raft_node),
                          raft_get_nodeid(raft),
                          msg->prevote,
                          msg->term,
                          msg->candidate_id,
                          msg->last_log_idx,
                          msg->last_log_term) != REDIS_OK) {
        NODE_TRACE(node, "failed requestvote");
    } else {
        NodeAddPendingResponse(node, false);
    }

    return 0;
}

/* ------------------------------------ AppendEntries ------------------------------------ */

static void handleAppendEntriesResponse(redisAsyncContext *c, void *r, void *privdata)
{
    Node *node = privdata;
    RedisRaftCtx *rr = node->rr;

    NodeDismissPendingResponse(node);

    redisReply *reply = r;
    if (!reply) {
        NODE_TRACE(node, "RAFT.AE failed: connection dropped.");
        ConnMarkDisconnected(node->conn);
        return;
    }

    if (reply->type == REDIS_REPLY_ERROR) {
        NODE_TRACE(node, "RAFT.AE error: %s", reply->str);
        return;
    }

    if (reply->type != REDIS_REPLY_ARRAY || reply->elements != 4 ||
        reply->element[0]->type != REDIS_REPLY_INTEGER ||
        reply->element[1]->type != REDIS_REPLY_INTEGER ||
        reply->element[2]->type != REDIS_REPLY_INTEGER ||
        reply->element[3]->type != REDIS_REPLY_INTEGER) {
        NODE_LOG_WARNING(node, "invalid RAFT.AE reply");
        return;
    }

    raft_appendentries_resp_t response = {
        .term = reply->element[0]->integer,
        .success = reply->element[1]->integer,
        .current_idx = reply->element[2]->integer,
        .msg_id = reply->element[3]->integer,
    };

    raft_node_t *raft_node = raft_get_node(rr->raft, node->id);

    int ret = raft_recv_appendentries_response(rr->raft, raft_node, &response);
    if (ret != 0) {
        NODE_TRACE(node, "raft_recv_appendentries_response failed, error %d", ret);
    }
}

static int raftSendAppendEntries(raft_server_t *raft, void *user_data,
                                 raft_node_t *raft_node, raft_appendentries_req_t *msg)
{
    Node *node = (Node *) raft_node_get_udata(raft_node);

    int argc = 5 + msg->n_entries * 2;
    char **argv = NULL;
    size_t *argvlen = NULL;

    if (!ConnIsConnected(node->conn)) {
        NODE_TRACE(node, "not connected, state=%s", ConnGetStateStr(node->conn));
        return 0;
    }

    argv = RedisModule_Alloc(sizeof(argv[0]) * argc);
    argvlen = RedisModule_Alloc(sizeof(argvlen[0]) * argc);

    char target_node_str[12];
    char source_node_str[12];
    char msg_str[100];
    char nentries_str[12];

    argv[0] = "RAFT.AE";
    argvlen[0] = strlen(argv[0]);

    argv[1] = target_node_str;
    argvlen[1] = snprintf(target_node_str, sizeof(target_node_str) - 1, "%d", raft_node_get_id(raft_node));

    argv[2] = source_node_str;
    argvlen[2] = snprintf(source_node_str, sizeof(source_node_str) - 1, "%d", raft_get_nodeid(raft));

    argv[3] = msg_str;
    argvlen[3] = snprintf(msg_str, sizeof(msg_str) - 1, "%d:%ld:%ld:%ld:%ld:%lu",
                          msg->leader_id,
                          msg->term,
                          msg->prev_log_idx,
                          msg->prev_log_term,
                          msg->leader_commit,
                          msg->msg_id);

    argv[4] = nentries_str;
    argvlen[4] = snprintf(nentries_str, sizeof(nentries_str) - 1, "%ld", msg->n_entries);

    int i;
    for (i = 0; i < msg->n_entries; i++) {
        raft_entry_t *e = msg->entries[i];
        argv[5 + i * 2] = RedisModule_Alloc(64);
        argvlen[5 + i * 2] = snprintf(argv[5 + i * 2], 63, "%ld:%d:%llu:%d", e->term, e->id, e->session, e->type);
        argvlen[6 + i * 2] = e->data_len;
        argv[6 + i * 2] = e->data;
    }

    if (redisAsyncCommandArgv(ConnGetRedisCtx(node->conn), handleAppendEntriesResponse,
                              node, argc, (const char **) argv, argvlen) != REDIS_OK) {
        NODE_TRACE(node, "failed appendentries");
    } else {
        NodeAddPendingResponse(node, false);
    }

    for (i = 0; i < msg->n_entries; i++) {
        RedisModule_Free(argv[5 + i * 2]);
    }

    RedisModule_Free(argv);
    RedisModule_Free(argvlen);

    return 0;
}

/* ------------------------------------ Timeout Follower --------------------------------- */

static void handleTimeoutNowResponse(redisAsyncContext *c, void *r, void *privdata)
{
    Node *node = privdata;

    NodeDismissPendingResponse(node);

    redisReply *reply = r;
    if (!reply) {
        NODE_TRACE(node, "RAFT.TIMEOUT_NOW failed: connection dropped.");
        ConnMarkDisconnected(node->conn);
        return;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        NODE_TRACE(node, "RAFT.TIMEOUT_NOW error: %s", reply->str);
        return;
    }

    if (reply->type != REDIS_REPLY_STATUS || strcmp("OK", reply->str) != 0) {
        NODE_LOG_WARNING(node, "invalid RAFT.TIMEOUT_NOW reply");
        return;
    }
}

static int raftSendTimeoutNow(raft_server_t *raft, void *udata, raft_node_t *raft_node)
{
    Node *node = raft_node_get_udata(raft_node);

    if (!ConnIsConnected(node->conn)) {
        NODE_TRACE(node, "not connected, state=%s", ConnGetStateStr(node->conn));
        return 0;
    }

    if (redisAsyncCommand(ConnGetRedisCtx(node->conn), handleTimeoutNowResponse,
                          node, "RAFT.TIMEOUT_NOW") != REDIS_OK) {
        NODE_TRACE(node, "failed timeout now");
    } else {
        NodeAddPendingResponse(node, false);
    }

    return 0;
}

/* ------------------------------------ Log Callbacks ------------------------------------ */

static int raftPersistMetadata(raft_server_t *raft, void *user_data,
                               raft_term_t term, raft_node_id_t vote)
{
    RedisRaftCtx *rr = user_data;

    int ret = MetadataWrite(&rr->meta, term, vote);
    RedisModule_Assert(ret == RR_OK);

    return 0;
}

static int raftApplyLog(raft_server_t *raft, void *user_data, raft_entry_t *entry, raft_index_t entry_idx)
{
    RedisRaftCtx *rr = user_data;
    RaftReq *req = entryDetachRaftReq(rr, entry);

    switch (entry->type) {
        case RAFT_LOGTYPE_ADD_NONVOTING_NODE: {
            RaftCfgChange *cfg = (RaftCfgChange *) entry->data;
            if (!req) {
                break;
            }

            RedisModule_ReplyWithArray(req->ctx, 2);
            RedisModule_ReplyWithLongLong(req->ctx, cfg->id);
            RedisModule_ReplyWithSimpleString(req->ctx, rr->snapshot_info.dbid);
            RaftReqFree(req);
            break;
        }
        case RAFT_LOGTYPE_REMOVE_NODE: {
            RaftCfgChange *cfg = (RaftCfgChange *) entry->data;

            if (req) {
                RedisModule_ReplyWithSimpleString(req->ctx, "OK");
                RaftReqFree(req);
            }

            if (cfg->id == raft_get_nodeid(raft)) {
                shutdownAfterRemoval(rr);
            }

            if (raft_is_leader(raft)) {
                raftSendNodeShutdown(raft_get_node(raft, cfg->id));
            }
            break;
        }
        case RAFT_LOGTYPE_NORMAL:
            executeLogEntry(rr, entry, entry_idx, req);
            break;
        case RAFT_LOGTYPE_ADD_SHARDGROUP:
        case RAFT_LOGTYPE_UPDATE_SHARDGROUP:
            applyShardGroupChange(rr, entry, req);
            break;
        case RAFT_LOGTYPE_REPLACE_SHARDGROUPS:
            replaceShardGroups(rr, entry, req);
            break;
        case RAFT_LOGTYPE_IMPORT_KEYS:
            importKeys(rr, entry, req);
            break;
        case RAFT_LOGTYPE_LOCK_KEYS:
            lockKeys(rr, entry, req);
            break;
        case RAFT_LOGTYPE_DELETE_UNLOCK_KEYS:
            unlockDeleteKeys(rr, entry, req);
            break;
        case RAFT_LOGTYPE_END_SESSION:
            handleEndClientSession(rr, entry, req);
            break;
        case RAFT_LOGTYPE_NO_OP:
            clearClientSessions(rr);
            clearAllBlockCommands();
            break;
        case RAFT_LOGTYPE_TIMEOUT_BLOCKED:
            timeoutBlockedCommand(rr, entry, req);
            break;
        default:
            break;
    }

    rr->snapshot_info.last_applied_term = entry->term;
    rr->snapshot_info.last_applied_idx = entry_idx;

    return 0;
}

/* ------------------------------------ Utility Callbacks ------------------------------------ */

static void raftLog(raft_server_t *raft, void *user_data, const char *buf)
{
    (void) raft;
    (void) user_data;

    RAFTLIB_TRACE("<raftlib> %s", buf);
}

static raft_node_id_t raftLogGetNodeId(raft_server_t *raft, void *user_data,
                                       raft_entry_t *entry,
                                       raft_index_t entry_idx)
{
    RaftCfgChange *req = (RaftCfgChange *) entry->data;
    return req->id;
}

static int raftNodeHasSufficientLogs(raft_server_t *raft, void *user_data, raft_node_t *raft_node)
{
    RedisModule_Assert(raft_node_is_active(raft_node));

    Node *node = raft_node_get_udata(raft_node);
    RedisModule_Assert(node != NULL);

    LOG_NOTICE("node:%d has sufficient logs, adding as voting node.", node->id);

    raft_entry_req_t *entry = raft_entry_new(sizeof(RaftCfgChange));
    entry->type = RAFT_LOGTYPE_ADD_NODE;

    RaftCfgChange *cfgchange = (RaftCfgChange *) entry->data;
    cfgchange->id = node->id;
    cfgchange->addr = node->addr;

    int e = raft_recv_entry(raft, entry, NULL);
    raft_entry_release(entry);

    return e;
}

static char *raftMembershipInfoString(raft_server_t *raft)
{
    size_t buflen = 1024;
    char *buf = RedisModule_Calloc(1, buflen);
    int i;

    buf = catsnprintf(buf, &buflen, "term:%ld index:%ld nodes:",
                      raft_get_current_term(raft),
                      raft_get_current_idx(raft));
    for (i = 0; i < raft_get_num_nodes(raft); i++) {
        raft_node_t *rn = raft_get_node_from_idx(raft, i);
        Node *n = raft_node_get_udata(rn);
        char addr[512];

        if (n) {
            snprintf(addr, sizeof(addr) - 1, "%s:%u", n->addr.host, n->addr.port);
        } else {
            addr[0] = '-';
            addr[1] = '\0';
        }

        buf = catsnprintf(buf, &buflen, " id=%d,voting=%d,active=%d,addr=%s",
                          raft_node_get_id(rn),
                          raft_node_is_voting(rn),
                          raft_node_is_active(rn),
                          addr);
    }

    return buf;
}

void raftNotifyMembershipEvent(raft_server_t *raft, void *user_data,
                               raft_node_t *raft_node, raft_entry_t *entry,
                               raft_membership_e type)
{
    RedisRaftCtx *rr = (RedisRaftCtx *) user_data;
    RaftCfgChange *cfgchange;
    raft_node_id_t my_id = raft_get_nodeid(raft);
    Node *node;

    switch (type) {
        case RAFT_MEMBERSHIP_ADD:
            /* When raft_add_node() is called explicitly, we get no entry so we
             * have nothing to do.
             */
            if (!entry) {
                addUsedNodeId(rr, my_id);
                break;
            }

            /* Ignore our own node, as we don't maintain a Node structure for it */
            RedisModule_Assert(entry->type == RAFT_LOGTYPE_ADD_NODE ||
                               entry->type == RAFT_LOGTYPE_ADD_NONVOTING_NODE);

            cfgchange = (RaftCfgChange *) entry->data;
            if (cfgchange->id == my_id) {
                break;
            }

            /* Allocate a new node */
            node = NodeCreate(rr, cfgchange->id, &cfgchange->addr);
            RedisModule_Assert(node != NULL);

            addUsedNodeId(rr, cfgchange->id);

            raft_node_set_udata(raft_node, node);
            break;

        case RAFT_MEMBERSHIP_REMOVE:
            node = raft_node_get_udata(raft_node);
            if (node != NULL) {
                ConnAsyncTerminate(node->conn);
                raft_node_set_udata(raft_node, NULL);
            }
            break;

        default:
            RedisModule_Assert(0);
    }

    char *s = raftMembershipInfoString(raft);
    LOG_NOTICE("Cluster Membership: %s", s);
    RedisModule_Free(s);
}

static void handleTransferLeaderComplete(raft_server_t *raft, raft_leader_transfer_e result);

/* just keep libraft callbacks together
 * so this just calls the redisraft RaftReq completion function, which is kept together with its functions
 */
static void raftNotifyTransferEvent(raft_server_t *raft, void *user_data, raft_leader_transfer_e result)
{
    handleTransferLeaderComplete(raft, result);
}

static void killPubsubClients()
{
    RedisModuleCallReply *r;
    RedisRaftCtx *rr = &redis_raft;

    r = RedisModule_Call(rr->ctx, "CLIENT", "cccE", "KILL", "TYPE", "PUBSUB");

    RedisModule_Assert(r);
    RedisModule_Assert(RedisModule_CallReplyType(r) == REDISMODULE_REPLY_INTEGER);

    long long n = RedisModule_CallReplyInteger(r);
    if (n) {
        LOG_NOTICE("Closed %lld pubsub client connections", n);
    }

    RedisModule_FreeCallReply(r);
}

static void raftNotifyStateEvent(raft_server_t *raft, void *user_data, raft_state_e state)
{
    switch (state) {
        case RAFT_STATE_FOLLOWER:
            LOG_NOTICE("State change: Node is now a follower, term %ld",
                       raft_get_current_term(raft));
            break;
        case RAFT_STATE_PRECANDIDATE:
            LOG_NOTICE("State change: Election starting, node is now a pre-candidate, term %ld",
                       raft_get_current_term(raft));
            break;
        case RAFT_STATE_CANDIDATE:
            LOG_NOTICE("State change: Node is now a candidate, term %ld",
                       raft_get_current_term(raft));
            break;
        case RAFT_STATE_LEADER:
            LOG_NOTICE("State change: Node is now a leader, term %ld",
                       raft_get_current_term(raft));
            break;
        default:
            break;
    }

    char *s = raftMembershipInfoString(raft);
    LOG_NOTICE("Cluster Membership: %s", s);
    RedisModule_Free(s);

    /* Kill subscribers if this is node is not the leader. On retry, they'll be
     * redirected to the leader. */
    if (state != RAFT_STATE_LEADER) {
        killPubsubClients();
    }
}

/* Apply some backpressure for the node. Helps in two cases, first we don't
 * want to create many appendentries messages for the node if we don't get
 * replies. Otherwise, it might cause out of memory. Second, we want to send
 * entries in batches for performance reasons. We are effectively batching
 * entries until we get replies from the previous ones. */
static int raftBackpressure(raft_server_t *raft, void *user_data, raft_node_t *raft_node)
{
    RedisRaftCtx *rr = &redis_raft;
    Node *node = raft_node_get_udata(raft_node);
    if (node->pending_raft_response_num >= rr->config.append_req_max_count) {
        /* Don't send append req to this node */
        return 1;
    }

    return 0;
}

static raft_time_t raftTimestamp(raft_server_t *raft, void *user_data)
{
    (void) raft;
    (void) user_data;

    return (raft_time_t) RedisModule_MonotonicMicroseconds();
}

static raft_index_t raftGetEntriesToSend(raft_server_t *raft, void *user_data,
                                         raft_node_t *node, raft_index_t idx,
                                         raft_index_t entries_n,
                                         raft_entry_t **entries)
{
    (void) node;
    (void) user_data;

    raft_index_t i;
    long long serialized_size = 0;
    RedisRaftCtx *rr = &redis_raft;

    for (i = 0; i < entries_n; i++) {
        raft_entry_t *e = raft_get_entry_from_idx(raft, idx + i);
        if (!e) {
            break;
        }
        serialized_size += e->data_len;
        /* A single entry can be larger than the limit, so, we always allow the
         * first entry. */
        if (i != 0 && serialized_size > rr->config.append_req_max_size) {
            break;
        }
        entries[i] = e;
    }

    return i;
}

raft_cbs_t redis_raft_callbacks = {
    .send_requestvote = raftSendRequestVote,
    .send_appendentries = raftSendAppendEntries,
    .persist_metadata = raftPersistMetadata,
    .log = raftLog,
    .get_node_id = raftLogGetNodeId,
    .applylog = raftApplyLog,
    .node_has_sufficient_logs = raftNodeHasSufficientLogs,
    .send_snapshot = raftSendSnapshot,
    .load_snapshot = raftLoadSnapshot,
    .clear_snapshot = raftClearSnapshot,
    .get_snapshot_chunk = raftGetSnapshotChunk,
    .store_snapshot_chunk = raftStoreSnapshotChunk,
    .notify_membership_event = raftNotifyMembershipEvent,
    .notify_state_event = raftNotifyStateEvent,
    .send_timeoutnow = raftSendTimeoutNow,
    .notify_transfer_event = raftNotifyTransferEvent,
    .backpressure = raftBackpressure,
    .timestamp = raftTimestamp,
    .get_entries_to_send = raftGetEntriesToSend,
};

static RRStatus loadRaftLog(RedisRaftCtx *rr)
{
    int ret;

    if (LogLoadEntries(&rr->log) != RR_OK) {
        LOG_WARNING("Failed to read Raft log");
        return RR_ERROR;
    }

    if (strcmp(rr->meta.dbid, LogDbid(&rr->log)) != 0) {
        PANIC("Log and metadata have different dbids: [metadata=%s/log=%s]",
              rr->meta.dbid, LogDbid(&rr->log));
    }

    /* Make sure the log we're going to apply matches the RDB we've loaded */
    if (rr->snapshot_info.loaded) {
        if (strcmp(rr->snapshot_info.dbid, rr->meta.dbid) != 0) {
            PANIC("Metadata and snapshot have different dbids: [metadata=%s/snapshot=%s]",
                  rr->meta.dbid, rr->snapshot_info.dbid);
        }
        if (rr->snapshot_info.last_applied_term < LogPrevLogTerm(&rr->log)) {
            PANIC("Log term (%lu) does not match snapshot term (%lu), aborting.",
                  LogPrevLogTerm(&rr->log), rr->snapshot_info.last_applied_term);
        }
        if (rr->snapshot_info.last_applied_idx + 1 < LogPrevLogIndex(&rr->log)) {
            PANIC("Log initial index (%lu) does not match snapshot last index (%lu), aborting.",
                  LogPrevLogIndex(&rr->log), rr->snapshot_info.last_applied_idx);
        }
    } else {
        /* If there is no snapshot, the log should also not refer to it */
        if (LogPrevLogIndex(&rr->log) != 0) {
            PANIC("Log refers to snapshot (term=%lu/index=%lu which was not loaded, aborting.",
                  LogPrevLogTerm(&rr->log), LogPrevLogIndex(&rr->log));
        }
    }

    /* Reset the log if snapshot is more advanced */
    if (LogCurrentIdx(&rr->log) < rr->snapshot_info.last_applied_idx) {
        LOG_WARNING("Snapshot (%ld) is more advanced than the log (%ld)",
                    rr->snapshot_info.last_applied_idx,
                    LogCurrentIdx(&rr->log));

        LogImpl.reset(rr, rr->snapshot_info.last_applied_idx + 1,
                      rr->snapshot_info.last_applied_term);
    }

    memcpy(rr->snapshot_info.dbid, rr->meta.dbid, RAFT_DBID_LEN);
    rr->snapshot_info.dbid[RAFT_DBID_LEN] = '\0';

    ret = raft_restore_log(rr->raft);
    RedisModule_Assert(ret == 0);

    LOG_NOTICE("Raft state after loading log: log_count=%lu, first_idx=%lu, "
               "current_idx=%lu, last_applied_idx=%lu",
               raft_get_log_count(rr->raft),
               raft_get_first_entry_idx(rr->raft),
               raft_get_current_idx(rr->raft),
               raft_get_last_applied_idx(rr->raft));

    ret = raft_restore_metadata(rr->raft, rr->meta.term, rr->meta.vote);
    RedisModule_Assert(ret == 0);

    LOG_DEBUG("Raft term=%lu, vote=%d", rr->meta.term, rr->meta.vote);

    return RR_OK;
}

/* Check if Redis is loading an RDB file */
static bool checkRedisLoading(RedisRaftCtx *rr)
{
    int err;
    RedisModuleServerInfoData *in;

    in = RedisModule_GetServerInfo(rr->ctx, "persistence");
    int val = (int) RedisModule_ServerInfoGetFieldSigned(in, "loading", &err);
    RedisModule_Assert(err == REDISMODULE_OK);
    RedisModule_FreeServerInfo(rr->ctx, in);

    return val;
}

static void handleLoadingState(RedisRaftCtx *rr)
{
    if (!checkRedisLoading(rr)) {
        /* If Redis loaded a snapshot (RDB), log some information and configure the
         * raft library as necessary.
         */
        LOG_NOTICE("Loading: Redis loading complete, snapshot %s",
                   rr->snapshot_info.loaded ? "LOADED" : "NOT LOADED");

        /* If id is configured, confirm the log matches. If not, we set it from
         * the metadata file.
         */
        if (!rr->config.id) {
            rr->config.id = rr->meta.node_id;
        }

        if (rr->config.id != LogNodeId(&rr->log)) {
            PANIC("Configured node id [%d] does not match Raft log node id [%d]",
                  rr->config.id, LogNodeId(&rr->log));
        }

        if (rr->config.id != rr->meta.node_id) {
            PANIC("Metadata node id [%d] does not match configured id [%d]",
                  rr->meta.node_id, rr->config.id);
        }

        if (!rr->sharding_info->shard_groups_num) {
            AddBasicLocalShardGroup(rr);
        }

        RaftLibraryInit(rr, false);

        if (rr->snapshot_info.loaded) {
            createOutgoingSnapshotMmap(rr);
            configureFromSnapshot(rr);
        }

        if (loadRaftLog(rr) != RR_OK) {
            PANIC("Failed to read Raft log");
        }
        rr->state = REDIS_RAFT_UP;
    }
}

void callRaftPeriodic(RedisModuleCtx *ctx, void *arg)
{
    RedisRaftCtx *rr = arg;
    int ret;

    RedisModule_CreateTimer(rr->ctx, rr->config.periodic_interval,
                            callRaftPeriodic, rr);

    /* If we're in LOADING state, we need to wait for Redis to finish loading before
     * we can apply the log.
     */
    if (rr->state == REDIS_RAFT_LOADING) {
        handleLoadingState(rr);
    }

    /* Proceed only if we're initialized */
    if (rr->state != REDIS_RAFT_UP) {
        return;
    }

    /* If we're creating a persistent snapshot, check if we're done */
    if (rr->snapshot_in_progress) {
        SnapshotResult sr;

        ret = pollSnapshotStatus(rr, &sr);
        if (ret == -1) {
            LOG_WARNING("Snapshot operation failed, cancelling.");
            cancelSnapshot(rr, &sr);
        } else if (ret) {
            LOG_DEBUG("Snapshot operation completed successfully.");
            finalizeSnapshot(rr, &sr);
        } /* else we're still in progress */
    }

    ret = raft_periodic(rr->raft);
    if (ret == RAFT_ERR_SHUTDOWN) {
        shutdownAfterRemoval(rr);
    }

    RedisModule_Assert(ret == 0);

    /* Compact cache */
    if (rr->config.log_max_cache_size) {
        /* Compact applied entries only. Otherwise, deleting entries from the
         * cache will result in replying -TIMEOUT to the user in the entry
         * destroy callback. */
        EntryCacheCompact(rr->logcache, rr->config.log_max_cache_size,
                          raft_get_last_applied_idx(rr->raft));
    }

    /* Initiate snapshot if log size exceeds raft-log-file-max
     *
     * Compaction happens in two steps:
     * 1- We call LogCompactionBegin() and start writing entries to a new file.
     * 2- Once we commit all the entries of the first file, we initiate the
     *    snapshot.
     */
    if (!rr->snapshot_in_progress) {
        bool start;
        /* Step-1: Start compaction if we are over the file size limit or if
         * there is a debug req. */
        uint64_t limit = rr->config.log_max_file_size;
        start = (limit && LogFileSize(&rr->log) > limit);

        if (start && !LogCompactionStarted(&rr->log) &&
            raft_get_num_snapshottable_logs(rr->raft) > 0) {

            /* Move to second log file. We'll trigger rdb save once we've
             * committed all the entries of the first log file. */
            int rc = LogCompactionBegin(&rr->log);
            RedisModule_Assert(rc == RR_OK);

            LOG_NOTICE("Raft log file size is %lu, "
                       "initiating compaction for index: %ld",
                       LogFileSize(&rr->log), LogCompactionIdx(&rr->log));
        }

        /* Step-2: If we've committed all the entries of the first log page, we
         * can start the snapshot. */
        start = (rr->debug_req || !rr->config.snapshot_disable);
        if (start && LogCompactionStarted(&rr->log) &&
            raft_get_commit_idx(rr->raft) >= LogCompactionIdx(&rr->log)) {

            LOG_NOTICE("Log file is ready for compaction. "
                       "log index:%ld, commit index: %ld.",
                       LogCompactionIdx(&rr->log),
                       raft_get_commit_idx(rr->raft));

            initiateSnapshot(rr);
        }
    }

    /* Call cluster */
    if (rr->config.sharding) {
        ShardingPeriodicCall(rr);
    }
}

/* A callback that invokes HandleNodeStates(), to handle node connection
 * management (reconnects, etc.).
 */
void callHandleNodeStates(RedisModuleCtx *ctx, void *arg)
{
    (void) ctx;
    RedisRaftCtx *rr = arg;

    RedisModule_CreateTimer(rr->ctx, rr->config.reconnect_interval,
                            callHandleNodeStates, rr);
    HandleIdleConnections(rr);
    HandleNodeStates(rr);
}

raft_node_id_t makeRandomNodeId(RedisRaftCtx *rr)
{
    unsigned int tmp;
    raft_node_id_t id;

    /* Generate a random id and validate:
     * 1. It's not zero (reserved value)
     * 2. Avoid negative numbers for better convenience
     * 3. Skip existing IDs, if library is already initialized
     */

    do {
        RedisModule_GetRandomBytes((unsigned char *) &tmp, sizeof(tmp));
        id = (raft_node_id_t) (tmp & ~(1u << 31));
    } while (!id || (rr->raft && raft_get_node(rr->raft, id) != NULL) || hasNodeIdBeenUsed(rr, id));

    return id;
}

bool hasNodeIdBeenUsed(RedisRaftCtx *rr, raft_node_id_t node_id)
{
    for (NodeIdEntry *e = rr->snapshot_info.used_node_ids; e != NULL; e = e->next) {
        if (e->id == node_id) {
            return true;
        }
    }
    return false;
}

void addUsedNodeId(RedisRaftCtx *rr, raft_node_id_t node_id)
{
    if (hasNodeIdBeenUsed(rr, node_id)) {
        return;
    }

    NodeIdEntry *entry = RedisModule_Alloc(sizeof(NodeIdEntry));
    entry->id = node_id;
    entry->next = rr->snapshot_info.used_node_ids;
    rr->snapshot_info.used_node_ids = entry;
}

void RaftLibraryInit(RedisRaftCtx *rr, bool cluster_init)
{
    raft_node_t *node;

    raft_set_heap_functions(RedisModule_Alloc,
                            RedisModule_Calloc,
                            RedisModule_Realloc,
                            RedisModule_Free);

    rr->raft = raft_new_with_log(&LogImpl, rr);
    if (!rr->raft) {
        PANIC("Failed to initialize Raft library");
    }

    int eltimeo = rr->config.election_timeout;
    int reqtimeo = rr->config.request_timeout;
    int log = redisraft_trace & TRACE_RAFTLIB ? 1 : 0;
    int noapply = rr->config.log_disable_apply;

    if (raft_config(rr->raft, 1, RAFT_CONFIG_ELECTION_TIMEOUT, eltimeo) != 0 ||
        raft_config(rr->raft, 1, RAFT_CONFIG_REQUEST_TIMEOUT, reqtimeo) != 0 ||
        raft_config(rr->raft, 1, RAFT_CONFIG_DISABLE_APPLY, noapply) != 0 ||
        raft_config(rr->raft, 1, RAFT_CONFIG_AUTO_FLUSH, 0) != 0 ||
        raft_config(rr->raft, 1, RAFT_CONFIG_NONBLOCKING_APPLY, 1) != 0 ||
        raft_config(rr->raft, 1, RAFT_CONFIG_LOG_ENABLED, log) != 0) {
        PANIC("Failed to configure libraft");
    }

    raft_set_callbacks(rr->raft, &redis_raft_callbacks, rr);

    node = raft_add_non_voting_node(rr->raft, NULL, rr->config.id, 1);
    if (!node) {
        PANIC("Failed to create local Raft node [id %d]", rr->config.id);
    }

    if (cluster_init) {
        if (raft_set_current_term(rr->raft, 1) != 0 ||
            raft_become_leader(rr->raft) != 0) {
            PANIC("Failed to init raft library");
        }

        RaftCfgChange cfg = {
            .id = rr->config.id,
            .addr = rr->config.addr,
        };

        raft_entry_t *ety = raft_entry_new(sizeof(cfg));

        ety->id = rand();
        ety->type = RAFT_LOGTYPE_ADD_NODE;
        memcpy(ety->data, &cfg, sizeof(cfg));

        if (raft_recv_entry(rr->raft, ety, NULL) != 0) {
            PANIC("Failed to init raft library");
        }
        raft_entry_release(ety);
    }

    rr->state = REDIS_RAFT_UP;
}

static void configureFromSnapshot(RedisRaftCtx *rr)
{
    SnapshotCfgEntry *c;

    LOG_NOTICE("Loading: Snapshot: applied term=%lu index=%lu",
               rr->snapshot_info.last_applied_term,
               rr->snapshot_info.last_applied_idx);

    for (c = rr->snapshot_info.cfg; c != NULL; c = c->next) {
        LOG_NOTICE("Loading: Snapshot config: node id=%u [%s:%u], voting=%u",
                   c->id, c->addr.host, c->addr.port, c->voting);
    }

    /* Load configuration loaded from the snapshot into Raft library.
     */
    configRaftFromSnapshotInfo(rr);

    int ret = raft_restore_snapshot(rr->raft,
                                    rr->snapshot_info.last_applied_term,
                                    rr->snapshot_info.last_applied_idx);
    RedisModule_Assert(ret == 0);
}

/* ------------------------------------ RaftReq ------------------------------------ */

/* Free a RaftReq structure.
 *
 * If it is associated with a blocked client, it will be unblocked and
 * the thread safe context released as well.
 */
void RaftReqFree(RaftReq *req)
{
    TRACE("RaftReqFree: req=%p, req->ctx=%p, req->client=%p",
          req, req->ctx, req->client);

    if (req->timeout_timer) {
        RedisModule_StopTimer(req->ctx, req->timeout_timer, NULL);
        req->timeout_timer = 0;
    }

    if (req->client_id) {
        BlockedReqResetById(&redis_raft, req->client_id);
    }

    if (req->type == RR_REDISCOMMAND) {
        if (req->r.redis.cmds.size) {
            RaftRedisCommandArrayFree(&req->r.redis.cmds);
        }
    } else if (req->type == RR_IMPORT_KEYS) {
        if (req->r.import_keys.key_names) {
            for (size_t i = 0; i < req->r.import_keys.num_keys; i++) {
                RedisModule_FreeString(req->ctx, req->r.import_keys.key_names[i]);
            }
            RedisModule_Free(req->r.import_keys.key_names);
            req->r.import_keys.key_names = NULL;
        }
        if (req->r.import_keys.key_serialized) {
            for (size_t i = 0; i < req->r.import_keys.num_keys; i++) {
                RedisModule_FreeString(req->ctx, req->r.import_keys.key_serialized[i]);
            }
            RedisModule_Free(req->r.import_keys.key_serialized);
            req->r.import_keys.key_serialized = NULL;
        }
    } else if (req->type == RR_MIGRATE_KEYS) {
        if (req->r.migrate_keys.keys) {
            for (size_t i = 0; i < req->r.migrate_keys.num_keys; i++) {
                RedisModule_FreeString(req->ctx, req->r.migrate_keys.keys[i]);
            }
            RedisModule_Free(req->r.migrate_keys.keys);
            req->r.migrate_keys.keys = NULL;
        }
        if (req->r.migrate_keys.keys_serialized) {
            for (size_t i = 0; i < req->r.migrate_keys.num_keys; i++) {
                if (req->r.migrate_keys.keys_serialized[i] != NULL) {
                    RedisModule_FreeString(req->ctx, req->r.migrate_keys.keys_serialized[i]);
                }
            }
            RedisModule_Free(req->r.migrate_keys.keys_serialized);
            req->r.migrate_keys.keys_serialized = NULL;
        }
        redis_raft.migrate_req = NULL;
    }

    if (req->ctx) {
        RedisModule_FreeThreadSafeContext(req->ctx);
        RedisModule_UnblockClient(req->client, NULL);
    }

    RedisModule_Free(req);
}

void blockedTimedOut(RedisModuleCtx *ctx, void *data)
{
    RedisRaftCtx *rr = &redis_raft;
    RaftReq *req = data;

    /* don't need to attach the req to this entry, as the req is part of the BlockedClient object */
    raft_entry_t *entry = RaftRedisSerializeTimeout(req->raft_idx, false);

    int e = raft_recv_entry(rr->raft, entry, NULL);
    if (e != 0) {
        /* if we timed out, but we couldn't apply the timeout entry, it means we lost leadership.
         * Therefore, when we apply a new NO_OP, it will automatically be timed out on all nodes.
         * Therefore, don't have to do anything here
         */
    }

    raft_entry_release(entry);
}

static RaftReq *RaftReqInitCore(RedisModuleCtx *ctx, enum RaftReqType type)
{
    RaftReq *req = RedisModule_Calloc(1, sizeof(RaftReq));
    if (ctx != NULL) {
        req->client = RedisModule_BlockClient(ctx, NULL, NULL, NULL, 0);
        req->ctx = RedisModule_GetThreadSafeContext(req->client);
    }
    req->type = type;

    return req;
}

RaftReq *RaftReqInit(RedisModuleCtx *ctx, enum RaftReqType type)
{
    RaftReq *req = RaftReqInitCore(ctx, type);

    if (type == RR_MIGRATE_KEYS) {
        redis_raft.migrate_req = req;
    }

    TRACE("RaftReqInit: req=%p, type=%s, client=%p, ctx=%p",
          req, RaftReqTypeStr[req->type], req->client, req->ctx);

    return req;
}

RaftReq *RaftReqInitBlocking(RedisModuleCtx *ctx, enum RaftReqType type, long long timeout)
{
    RedisRaftCtx *rr = &redis_raft;

    RaftReq *req = RaftReqInitCore(ctx, type);

    if (timeout > 0) {
        req->timeout_timer = RedisModule_CreateTimer(req->ctx, timeout, blockedTimedOut, req);
    }

    req->client_id = RedisModule_GetClientId(ctx);
    ClientStateSetBlockedReq(rr, req->client_id, req);

    TRACE("RaftReqInit: req=%p, type=%s, client=%p, ctx=%p",
          req, RaftReqTypeStr[req->type], req->client, req->ctx);

    return req;
}

/* ------------------------------------ RaftReq Implementation ------------------------------------ */

/*
 * Implementation of specific request types.
 */

void handleTransferLeaderComplete(raft_server_t *raft, raft_leader_transfer_e result)
{
    char buf[64];
    RedisRaftCtx *rr = &redis_raft;

    if (!rr->transfer_req) {
        LOG_WARNING("leader transfer update: but no req to correlate it to!");
        return;
    }

    RedisModuleCtx *ctx = rr->transfer_req->ctx;

    switch (result) {
        case RAFT_LEADER_TRANSFER_EXPECTED_LEADER:
            RedisModule_ReplyWithSimpleString(ctx, "OK");
            break;
        case RAFT_LEADER_TRANSFER_UNEXPECTED_LEADER:
            RedisModule_ReplyWithError(ctx, "ERR different node elected leader");
            break;
        case RAFT_LEADER_TRANSFER_TIMEOUT:
            RedisModule_ReplyWithError(ctx, "ERR transfer timed out");
            break;
        default:
            snprintf(buf, sizeof(buf), "ERR unknown case: %d", result);
            RedisModule_ReplyWithError(ctx, buf);
            break;
    }

    RaftReqFree(rr->transfer_req);
    rr->transfer_req = NULL;
}

/* Apply a SHARDGROUP Add and Update log entries by deserializing the payload and
 * updating the in-memory shardgroup configuration.
 *
 * If the entry holds a user_data pointing to a RaftReq, this implies we're
 * applying an operation performed by a local client (vs. one received from
 * persisted log, or through AppendEntries). In that case, we also need to
 * generate the reply as the client is blocked waiting for it.
 */
void applyShardGroupChange(RedisRaftCtx *rr, raft_entry_t *entry, RaftReq *req)
{
    RRStatus ret;

    ShardGroup *sg;

    if ((sg = ShardGroupDeserialize(entry->data, entry->data_len)) == NULL) {
        LOG_WARNING("Failed to deserialize ADD_SHARDGROUP payload: [%.*s]",
                    (int) entry->data_len, entry->data);
        return;
    }

    switch (entry->type) {
        case RAFT_LOGTYPE_ADD_SHARDGROUP:
            if ((ret = ShardingInfoAddShardGroup(rr, sg)) != RR_OK)
                LOG_WARNING("Failed to add a shardgroup");
            break;
        case RAFT_LOGTYPE_UPDATE_SHARDGROUP:
            if ((ret = ShardingInfoUpdateShardGroup(rr, sg)) != RR_OK)
                LOG_WARNING("Failed to update shardgroup");
            break;
        default:
            PANIC("Unknown entry type %d", entry->type);
            break;
    }

    /* If we have an attached client, handle the reply */
    if (req) {
        if (ret == RR_OK) {
            RedisModule_ReplyWithSimpleString(req->ctx, "OK");
        } else {
            RedisModule_ReplyWithError(req->ctx, "ERR Invalid ShardGroup Update");
        }
        RaftReqFree(req);
    }
}

void replaceShardGroups(RedisRaftCtx *rr, raft_entry_t *entry, RaftReq *req)
{
    // 1. reset sharding info
    ShardingInfo *si = rr->sharding_info;

    if (si->shard_group_map != NULL) {
        ShardGroup *data;
        RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(si->shard_group_map, "^", NULL, 0);

        while (RedisModule_DictNextC(iter, NULL, (void **) &data) != NULL) {
            ShardGroupFree(data);
        }
        RedisModule_DictIteratorStop(iter);
        RedisModule_FreeDict(rr->ctx, si->shard_group_map);
        si->shard_group_map = NULL;
    }

    si->shard_group_map = RedisModule_CreateDict(rr->ctx);
    si->shard_groups_num = 0;

    for (int i = 0; i <= REDIS_RAFT_HASH_MAX_SLOT; i++) {
        si->stable_slots_map[i] = NULL;
        si->importing_slots_map[i] = NULL;
        si->migrating_slots_map[i] = NULL;
    }

    /* 2. iterate over payloads
     * payload structure
     * "# shard groups:payload1 len:payload1:....:payload n len:payload n:"
     */
    char *payload = entry->data;

    char *nl = strchr(payload, '\n');
    char *endptr;
    int num_payloads = (int) strtoul(payload, &endptr, 10);
    RedisModule_Assert(endptr == nl);
    payload = nl + 1;

    for (int i = 0; i < num_payloads; i++) {
        nl = strchr(payload, '\n');
        size_t payload_len = strtoul(payload, &endptr, 10);
        RedisModule_Assert(endptr == nl);
        payload = nl + 1;

        ShardGroup *sg;
        if ((sg = ShardGroupDeserialize(payload, payload_len)) == NULL) {
            LOG_WARNING("Failed to deserialize shardgroup payload: [%.*s]", (int) payload_len, payload);
            return;
        }

        if (!strncmp(sg->id, rr->meta.dbid, RAFT_DBID_LEN)) {
            sg->local = true;
        }

        RRStatus res = ShardingInfoAddShardGroup(rr, sg);
        RedisModule_Assert(res == RR_OK);
        payload += payload_len + 1;
    }

    /* If we have an attached client, handle the reply */
    if (req) {
        RedisModule_ReplyWithSimpleString(req->ctx, "OK");
        RaftReqFree(req);
    }
}

/* Callback for fsync thread. This will be triggered by fsync thread but will
 * be called by Redis thread. */
void handleFsyncCompleted(void *result)
{
    FsyncThreadResult *rs = result;
    RedisRaftCtx *rr = &redis_raft;

    rr->log.fsync_count++;
    rr->log.fsync_index = rs->fsync_index;
    rr->log.fsync_total += rs->time;
    rr->log.fsync_max = MAX(rs->time, rr->log.fsync_max);

    RedisModule_Free(rs);
}

static void noopCallback(void *result)
{
    (void) result;
}

/* Redis thread callback, called just before Redis main thread goes to sleep,
 * e.g epoll_wait(). At this point, we've read all new network messages for this
 * event loop iteration. We can trigger new appendentries messages for the new
 * entries and check for committing/applying new entries. */
void handleBeforeSleep(RedisRaftCtx *rr)
{
    if (rr->state != REDIS_RAFT_UP) {
        return;
    }

    raft_index_t flushed = rr->log.fsync_index;
    raft_index_t next = raft_get_index_to_sync(rr->raft);
    if (next > 0) {
        LogFlush(&rr->log);

        if (rr->config.log_fsync) {
            /* Trigger async fsync() for the current index */
            fsyncThreadAddTask(&rr->fsyncThread, LogCurrentFd(&rr->log), next);
        } else {
            /* Skipping fsync(), we can just update the sync'd index. */
            flushed = next;
        }
    }

    int e = raft_flush(rr->raft, flushed);
    if (e == RAFT_ERR_SHUTDOWN) {
        shutdownAfterRemoval(rr);
    }
    RedisModule_Assert(e == 0);

    if (raft_pending_operations(rr->raft)) {
        /* If there are pending operations, we need to call raft_flush() again.
         * We'll do it in the next iteration as we want to process messages
         * from the network first. Here, we just wake up the event loop. In the
         * next iteration, beforeSleep() callback will be called again. */
        RedisModule_EventLoopAddOneShot(noopCallback, NULL);
    }
}
