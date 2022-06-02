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
    int slot = KeyHashSlotRedisString(import_keys.key_names[0]);

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
