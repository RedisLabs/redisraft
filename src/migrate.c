#include <string.h>

#include "redisraft.h"

static int validSlotMagic(RedisRaftCtx *rr, unsigned int slot, int magic)
{
    /* validSlot will have already validated that slot is importing */
    ShardGroup *sg = rr->sharding_info->importing_slots_map[slot];

    for (unsigned int i = 0; i < sg->slot_ranges_num; i++) {
        ShardGroupSlotRange *sr = &sg->slot_ranges[i];
        if (sr->start_slot <= slot && sr->end_slot >= slot && sr->magic == magic) {
            return 1;
        }
    }

    return 0;
}

static int validSlotTerm(RedisRaftCtx *rr, int slot, raft_term_t term)
{
    ShardingInfo *si = rr->sharding_info;

    if (si->max_importing_term[slot] <= term) {
        si->max_importing_term[slot] = term;
        return 1;
    }

    return 0;
}

static int validSlot(RedisRaftCtx *rr, int slot)
{
    ShardGroup *sg = rr->sharding_info->importing_slots_map[slot];

    if (sg && sg->local) {
        return 1;
    }

    return 0;
}

void importKeys(RedisRaftCtx *rr, raft_entry_t *entry)
{
    RedisModule_Assert(entry->type == RAFT_LOGTYPE_IMPORT_KEYS);

    RaftReq *req = entry->user_data;

    ImportKeys import_keys = {0};
    RedisModule_Assert(RaftRedisDeserializeImport(&import_keys, entry->data, entry->data_len) == RR_OK);
    RedisModule_Assert(import_keys.num_keys > 0);

    // FIXME: validate no cross slot migration at append time
    int slot = keyHashSlotRedisString(import_keys.key_names[0]);

    if (!validSlot(rr, slot)) {
        if (req) {
            RedisModule_ReplyWithError(req->ctx, "ERR not an importing slot");
        }
        goto exit;
    }

    if (!validSlotMagic(rr, slot, import_keys.magic)) {
        LOG_DEBUG("ignoring import keys as incorrect migration session key");
        if (req) {
            RedisModule_ReplyWithError(req->ctx, "ERR invalid magic");
        }
        goto exit;
    }

    if (!validSlotTerm(rr, slot, import_keys.term)) {
        // this is expected situation (suspended old leader)
        LOG_DEBUG("ignoring import keys as old term");
        if (req) {
            RedisModule_ReplyWithError(req->ctx, "ERR invalid term");
        }
        goto exit;
    }

    /* 2  'static' strings need for restore command */
    RedisModuleString *zero = RedisModule_CreateString(rr->ctx, "0", 1); /* ttl of zero */
    RedisModuleString *replace = RedisModule_CreateString(rr->ctx, "REPLACE", 1); /* overwrite on import */

    for (size_t i = 0; i < import_keys.num_keys; i++) {
        RedisModuleString * temp[4];
        temp[0] = import_keys.key_names[i];
        temp[1] = zero;
        temp[2] = import_keys.key_serialized[i];
        temp[3] = replace;

        enterRedisModuleCall();
        RedisModuleCallReply *reply;
        RedisModule_Assert((reply = RedisModule_Call(rr->ctx, "restore", "v", temp, 3)) != NULL);
        exitRedisModuleCall();
        RedisModule_Assert(RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_ERROR);
        RedisModule_FreeCallReply(reply);
    }

    RedisModule_FreeString(rr->ctx, zero);
    RedisModule_FreeString(rr->ctx, replace);

    if (req) {
        RedisModule_ReplyWithSimpleString(req->ctx, "OK");
    }

exit:
    if (req) {
        RaftReqFree(req);
    }
    FreeImportKeys(&import_keys);
}

/* RAFT.IMPORT <term> <session key> key0_name key0_serialization ... keyN_name keyN_serialization
 * Imports the keys and their values while validating via term and session key that import is valid
 * Reply:
 *   +OK upon success
 */
int cmdRaftImport(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    RedisRaftCtx *rr = &redis_raft;

    /* argc must be at least 5, for 3 static args (cmd/magic/term) + 2 for 1 key
     * and must be odd, due to 2 args needed for each key
     */
    if (argc < 5 || (argc % 2) == 0) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    long long term;
    if (RedisModule_StringToLongLong(argv[1], &term) == REDISMODULE_ERR) {
        RedisModule_ReplyWithError(ctx, "ERR failed to parse raft term");
        return REDISMODULE_OK;
    }

    long long magic;
    if (RedisModule_StringToLongLong(argv[2], &magic) == REDISMODULE_ERR) {
        RedisModule_ReplyWithError(ctx, "ERR failed to parse import magic");
        return REDISMODULE_OK;
    }

    RaftReq *req = RaftReqInit(ctx, RR_IMPORT_KEYS);
    req->r.import_keys.term = (raft_term_t) term;
    req->r.import_keys.magic = magic;

    int num_keys = (argc -3) / 2;
    req->r.import_keys.num_keys = num_keys;
    req->r.import_keys.key_names = RedisModule_Calloc(num_keys, sizeof(RedisModuleString *));
    req->r.import_keys.key_serialized = RedisModule_Calloc(num_keys, sizeof(RedisModuleString *));

    for (int i = 0; i < num_keys; i++) {
        req->r.import_keys.key_names[i] = RedisModule_HoldString(rr->ctx, argv[3 + (i*2)]);
        req->r.import_keys.key_serialized[i] = RedisModule_HoldString(rr->ctx, argv[3 + (i*2) + 1]);
    }

    raft_entry_t *entry = RaftRedisSerializeImport(&req->r.import_keys);
    entry->id = rand();
    entry->type = RAFT_LOGTYPE_IMPORT_KEYS;
    entryAttachRaftReq(rr, entry, req);

    raft_entry_resp_t response;
    int e = raft_recv_entry(rr->raft, entry, &response);
    if (e != 0) {
        replyRaftError(req->ctx, e);
        entryDetachRaftReq(rr, entry);
        raft_entry_release(entry);
        goto fail;
    }

    raft_entry_release(entry);

    return REDISMODULE_OK;

fail:
    RaftReqFree(req);
    return REDISMODULE_OK;
}

static void raftAppendRaftUnlockDeleteEntry(RedisRaftCtx *rr, RaftReq *req)
{
    raft_entry_resp_t response;

    raft_entry_t *entry = RaftRedisLockKeysSerialize(req->r.migrate_keys.keys, req->r.migrate_keys.num_keys);
    entry->id = rand();
    entry->type = RAFT_LOGTYPE_DELETE_UNLOCK_KEYS;
    entryAttachRaftReq(rr, entry, req);

    int e = raft_recv_entry(rr->raft, entry, &response);
    if (e != 0) {
        RedisModule_ReplyWithError(req->ctx, "ERR Unable to unlock/delete migrated keys, try again");
        replyRaftError(req->ctx, e);
        entryDetachRaftReq(rr, entry);
        raft_entry_release(entry);
        goto error;
    }

    raft_entry_release(entry);

    /* Unless applied by raft_apply_all() (and freed by it), the request
     * is pending so we don't free it or unblock the client.
     */
    return;

error:
    RaftReqFree(req);
}

static void transferKeysResponse(redisAsyncContext *c, void *r, void *privdata)
{
    Connection *conn = privdata;
    JoinLinkState *state = ConnGetPrivateData(conn);
    RedisRaftCtx *rr = ConnGetRedisRaftCtx(conn);
    RaftReq *req = state->req;

    redisReply *reply = r;

    if (!reply) {
        ConnMarkDisconnected(conn);
        RedisModule_ReplyWithError(req->ctx, "ERR: connection dropped impporting keys into remote cluster, try again");
        RaftReqFree(req);
    } else if (reply->type == REDIS_REPLY_ERROR) {
        ConnAsyncTerminate(conn);
        replyWithFormatErrorString(req->ctx, "RAFT.IMPORT failed: %.*s", (int) reply->len, reply->str);
        RaftReqFree(req);
    } else if (reply->type != REDIS_REPLY_STATUS || reply->len != 2 || strncmp(reply->str, "OK", 2)) {
        ConnAsyncTerminate(conn);
        /* FIXME: above should be changed to string eventually? */
        replyWithFormatErrorString(req->ctx, "ERR: received unexpected response from remote cluster, type = %d (wanted %d), len = %ld, response = %.*s", reply->type, REDIS_REPLY_STATUS, reply->len, (int) reply->len, reply->str);
        RaftReqFree(req);
    } else {
        /* SUCCESS */
        ConnAsyncTerminate(conn);
        raftAppendRaftUnlockDeleteEntry(rr, req);
    }

    redisAsyncDisconnect(c);
}

static void transferKeys(Connection *conn)
{
    RedisRaftCtx *rr = ConnGetRedisRaftCtx(conn);
    JoinLinkState *state = ConnGetPrivateData(conn);
    RaftReq *req = state->req;

    /* Connection is not good?  Terminate and continue */
    if (!ConnIsConnected(conn)) {
        return;
    }

    ShardGroup * sg = getShardGroupById(rr, req->r.migrate_keys.shardGroupId);
    if (sg == NULL) {
        //FIXME: error
    }

    // raft.import term magic <key1_name> <key1_serialized> ... <keyn_name> <keyn_serialized>
    int argc = 3 + (req->r.migrate_keys.num_serialized_keys * 2);
    char **argv = RedisModule_Calloc(argc, sizeof(char *));
    size_t *argv_len = RedisModule_Calloc(argc, sizeof(size_t));

    argv[0] = RedisModule_Strdup("RAFT.IMPORT");
    argv_len[0] = strlen("RAFT.IMPORT");
    argv[1] = RedisModule_Alloc(32);
    int n = snprintf(argv[1], 32, "%ld", req->r.migrate_keys.migrate_term);
    argv_len[1] = n;
    argv[2] = RedisModule_Alloc(32);
    // FIXME needs to be taken from sg (in raft.import pr))
    n = snprintf(argv[2], 32, "%llu", (long long unsigned) 0);
    argv_len[2] = n;

    for (size_t i = 0; i < req->r.migrate_keys.num_keys; i++) {
        if (req->r.migrate_keys.keys_serialized[i] == NULL) {
            continue;
        }

        size_t key_len;
        const char * key = RedisModule_StringPtrLen(req->r.migrate_keys.keys[i], &key_len);
        argv[3 + (i*2)] = RedisModule_Strdup(key);
        argv_len[3+ (i*2)] = key_len;

        size_t str_len;
        const char * str = RedisModule_StringPtrLen(req->r.migrate_keys.keys_serialized[i], &str_len);
        argv[3 + (i*2) + 1] = RedisModule_Alloc(str_len);
        memcpy(argv[3 + (i*2) + 1], str, str_len);
        argv_len[3 + (i*2) + 1] = str_len;
    }

    if (redisAsyncCommandArgv(ConnGetRedisCtx(conn), transferKeysResponse, conn, argc, (const char **) argv, argv_len) != REDIS_OK) {
        RedisModule_ReplyWithError(req->ctx, "ERR failed to submit RAFT.IMPORT command, try again");
        redisAsyncDisconnect(ConnGetRedisCtx(conn));
        ConnMarkDisconnected(conn);
        RaftReqFree(req);
    }

    for(int i = 0; i < argc; i++) {
        RedisModule_Free(argv[i]);
    }
    RedisModule_Free(argv);
    RedisModule_Free(argv_len);
}

void MigrateKeys(RedisRaftCtx *rr, RaftReq *req)
{
    JoinLinkState *state = RedisModule_Calloc(1, sizeof(*state));
    state->type = "migrate";
    state->connect_callback = transferKeys;
    state->start = time(NULL);
    ShardGroup * sg = getShardGroupById(rr, req->r.migrate_keys.shardGroupId);
    if (sg == NULL) {
        RedisModule_ReplyWithError(req->ctx, "ERR couldn't resolve shardgroup id");
        goto exit;
    }
    req->r.migrate_keys.migrate_term = raft_get_current_term(rr->raft);

    for (size_t i = 0; i < req->r.migrate_keys.num_keys; i++) {
        RedisModuleString *key = req->r.migrate_keys.keys[i];

        if (RedisModule_KeyExists(req->ctx, key)) {
            req->r.migrate_keys.num_serialized_keys++;

            enterRedisModuleCall();
            RedisModuleCallReply *reply = RedisModule_Call(rr->ctx, "DUMP", "s", key);
            exitRedisModuleCall();

            if (reply && RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_STRING) {
                req->r.migrate_keys.keys_serialized[i] = RedisModule_CreateStringFromCallReply(reply);
            } else {
                if (reply) {
                    LOG_WARNING("unexpected response type = %d", RedisModule_CallReplyType(reply));
                    RedisModule_FreeCallReply(reply);
                } else {
                    LOG_WARNING("didn't get a reply!");
                }
                RedisModule_ReplyWithError(req->ctx, "ERR see logs");
                goto exit;
            }
        }
    }

    /* nothing to migrate, return quickly */
    if (req->r.migrate_keys.num_serialized_keys == 0) {
        RedisModule_ReplyWithCString(req->ctx, "OK");
        goto exit;
    }

    for (unsigned int i = 0; i < sg->nodes_num; i++) {
        LOG_WARNING("MigrateKeys: adding %s:%d", sg->nodes[i].addr.host, sg->nodes[i].addr.port);
        NodeAddrListAddElement(&state->addr, &sg->nodes[i].addr);
    }
    state->req = req;

    char *username = req->r.migrate_keys.auth_username;
    char *password = req->r.migrate_keys.auth_password;
    state->conn = ConnCreate(rr, state, joinLinkIdleCallback, joinLinkFreeCallback, username, password);
    return;

exit:
    RaftReqFree(req);
}
