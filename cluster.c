/*
 * This file is part of RedisRaft.
 *
 * Copyright (c) 2020 Redis Labs
 *
 * RedisRaft is dual licensed under the GNU Affero General Public License version 3
 * (AGPLv3) or the Redis Source Available License (RSAL).
 */

#include <string.h>

#include "redisraft.h"
#include "crc16.h"

/* -----------------------------------------------------------------------------
 * Hashing code - copied directly from Redis.
 * -------------------------------------------------------------------------- */

/* We have 16384 hash slots. The hash slot of a given key is obtained
 * as the least significant 14 bits of the crc16 of the key.
 *
 * However if the key contains the {...} pattern, only the part between
 * { and } is hashed. This may be useful in the future to force certain
 * keys to be in the same node (assuming no resharding is in progress). */
unsigned int keyHashSlot(const char *key, int keylen) {
    int s, e; /* start-end indexes of { and } */

    for (s = 0; s < keylen; s++)
        if (key[s] == '{') break;

    /* No '{' ? Hash the whole key. This is the base case. */
    if (s == keylen) return crc16_ccitt(key,keylen) & 0x3FFF;

    /* '{' found? Check if we have the corresponding '}'. */
    for (e = s+1; e < keylen; e++)
        if (key[e] == '}') break;

    /* No '}' or nothing between {} ? Hash the whole key. */
    if (e == keylen || e == s+1) return crc16_ccitt(key,keylen) & 0x3FFF;

    /* If we are here there is both a { and a } on its right. Hash
     * what is in the middle between { and }. */
    return crc16_ccitt(key+s+1,e-s-1) & 0x3FFF;
}

/* -----------------------------------------------------------------------------
 * ShardingInfo Handling
 * -------------------------------------------------------------------------- */

/* Add a new ShardGroup to the active ShardingInfo. The start and end slots are
 * validated against hash slot confilicts.
 */

static RRStatus addShardGroup(RedisRaftCtx *rr, int start_slot, int end_slot, int num_nodes,
        ShardGroupNode *nodes)
{
    int i;
    ShardingInfo *si = rr->sharding_info;

    /* Verify all specified slots are available */
    if (!REDIS_RAFT_VALID_HASH_SLOT_RANGE(start_slot, end_slot))
        return RR_ERROR;
    for (i = start_slot; i <= end_slot; i++) {
        if (si->hash_slots_map[i] != 0)
            return RR_ERROR;
    }

    si->shard_groups_num++;
    si->shard_groups = RedisModule_Realloc(si->shard_groups, sizeof(ShardGroup) * si->shard_groups_num);

    ShardGroup *sg = &si->shard_groups[si->shard_groups_num-1];
    sg->start_slot = start_slot;
    sg->end_slot = end_slot;
    sg->nodes_num = num_nodes;
    sg->nodes = RedisModule_Alloc(sizeof(ShardGroupNode) * num_nodes);
    memcpy(sg->nodes, nodes, sizeof(ShardGroupNode) * num_nodes);

    for (i = start_slot; i <= end_slot; i++) {
        si->hash_slots_map[i] = si->shard_groups_num;
    }

    return RR_OK;
}

/* Parse a ShardGroup specification as passed directly to RAFT.SHARDGROUP ADD.
 * Shard group syntax is as follows:
 *
 *  [start slot] [end slot] [node-uid node-addr:node-port] [node-uid node-addr:node-port...]
 *
 * !!! This is a temporary hack implemented just for demo/testing purposes. We should
 * !!! replace this by a mechanism similar to RAFT.CLUSTER JOIN, where the user provides
 * !!! host address and port to connect to and we periodically poll remote shardgroups
 * !!! for their configuration.
 */

void addShardGroupFromArgs(RedisRaftCtx *rr, RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    long long start_slot, end_slot;
    int i;

    if (RedisModule_StringToLongLong(argv[0], &start_slot) != REDISMODULE_OK ||
            RedisModule_StringToLongLong(argv[1], &end_slot) != REDISMODULE_OK ||
            !REDIS_RAFT_VALID_HASH_SLOT_RANGE(start_slot, end_slot)) {
        RedisModule_ReplyWithError(ctx, "ERR invalid slot range");
        return;
    }

    int num_nodes = (argc - 2) / 2;
    if ((argc - 2) != num_nodes * 2) {
        RedisModule_WrongArity(ctx);
        return;
    }

    ShardGroupNode sg_nodes[num_nodes];
    for (i = 0; i < num_nodes; i++) {
        size_t len;
        const char *str = RedisModule_StringPtrLen(argv[2+(i*2)], &len);

        if (len != 40) {
            RedisModule_ReplyWithError(ctx, "ERR invalid node id length");
            return;
        }

        memcpy(sg_nodes[i].node_id, str, len);
        sg_nodes[i].node_id[len] = '\0';

        str = RedisModule_StringPtrLen(argv[3+(i*2)], &len);
        if (!NodeAddrParse(str, len, &sg_nodes[i].addr)) {
            RedisModule_ReplyWithError(ctx, "ERR invalid node address/port");
            return;
        }
    }

    if (addShardGroup(rr, start_slot, end_slot, num_nodes, sg_nodes) != RR_OK) {
        RedisModule_ReplyWithError(ctx, "ERR invalid shardgroup configuration");
        return;
    }

    RedisModule_ReplyWithSimpleString(ctx, "OK");
}

/* Initialize ShardingInfo and add our local RedisRaft cluster as the first
 * ShardGroup.
 */

RRStatus ShardingInfoInit(RedisRaftCtx *rr)
{
    rr->sharding_info = RedisModule_Calloc(1, sizeof(ShardingInfo));
    return addShardGroup(rr, rr->config->cluster_start_hslot,
            rr->config->cluster_end_hslot, 0, NULL);
}

/* Issue a COMMAND GETKEYS command to fetch the list of keys addressed
 * by the specified command.
 *
 * TODO: This is a temporary inefficient work around until RM_GetCommandKeys
 * is accepted as an API function.
 *
 * We may want to keep it for a while after the Module API is available for
 * compatibility with older versions of Redis, but emit a warning if we have
 * to fall back to use it.
 */

RedisModuleCallReply *getCommandKeys(RedisRaftCtx *rr, RaftRedisCommand *cmd)
{
    RedisModuleString *getkeys = RedisModule_CreateString(rr->ctx, "GETKEYS", 7);
    RedisModuleString *argv[cmd->argc + 1];

    argv[0] = getkeys;
    memcpy(&argv[1], &cmd->argv[0], cmd->argc * sizeof(RedisModuleString *));

    RedisModule_ThreadSafeContextLock(rr->ctx);
    RedisModuleCallReply *reply = RedisModule_Call(
            rr->ctx, "COMMAND", "v", argv, cmd->argc + 1);
    RedisModule_ThreadSafeContextUnlock(rr->ctx);

    RedisModule_FreeString(rr->ctx, getkeys);

    if (RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_ARRAY) {
        RedisModule_FreeCallReply(reply);
        reply = NULL;
    }

    return reply;
}

/* Compute the hash slot for a RaftRedisCommandArray list of commands and update
 * the entry.
 */

RRStatus computeHashSlot(RedisRaftCtx *rr, RaftReq *req)
{
    int i, j;
    int slot = -1;

    RaftRedisCommandArray *cmds = &req->r.redis.cmds;
    for (i = 0; i < cmds->len; i++) {
        RaftRedisCommand *cmd = cmds->commands[i];

        /* Iterate command keys */
        RedisModuleCallReply *reply = getCommandKeys(rr, cmd);
        for (j = 0; j < RedisModule_CallReplyLength(reply); j++) {
            size_t key_len;
            const char *key = RedisModule_CallReplyStringPtr(
                    RedisModule_CallReplyArrayElement(reply, j), &key_len);
            int thisslot = keyHashSlot(key, key_len);

            if (slot == -1) {
                /* First key */
                slot = thisslot;
            } else {
                if (slot != thisslot) {
                    RedisModule_FreeCallReply(reply);
                    return RR_ERROR;
                }
            }
        }
        RedisModule_FreeCallReply(reply);
    }

    req->r.redis.hash_slot = slot;

    return RR_OK;
}

static int addClusterSlotNodeReply(RedisRaftCtx *rr, RedisModuleCtx *ctx, raft_node_t *raft_node)
{
    Node *node = raft_node_get_udata(raft_node);
    NodeAddr *addr;
    char node_id[42];

    /* Stale nodes should not exist but we prefer to be defensive.
     * Our own node doesn't have a connection so we don't expect a Node object.
     */
    if (node) {
        addr = &node->addr;
    } else if (raft_get_my_node(rr->raft) == raft_node) {
        addr = &rr->config->addr;
    } else {
        return 0;
    }

    /* Create a three-element reply:
     * 1) Address
     * 2) Port
     * 3) Node ID
     */

    RedisModule_ReplyWithArray(ctx, 3);
    RedisModule_ReplyWithCString(ctx, addr->host);
    RedisModule_ReplyWithLongLong(ctx, addr->port);

    snprintf(node_id, sizeof(node_id) - 1, "%.32s%08x", rr->log->dbid, raft_node_get_id(raft_node));
    RedisModule_ReplyWithCString(ctx, node_id);

    return 1;
}

static int addClusterSlotShardGroupNodeReply(RedisRaftCtx *rr, RedisModuleCtx *ctx, ShardGroupNode *sgn)
{
    /* Create a three-element reply:
     * 1) Address
     * 2) Port
     * 3) Node ID
     */

    RedisModule_ReplyWithArray(ctx, 3);
    RedisModule_ReplyWithCString(ctx, sgn->addr.host);
    RedisModule_ReplyWithLongLong(ctx, sgn->addr.port);
    RedisModule_ReplyWithCString(ctx, sgn->node_id);

    return 1;
}

static void addClusterSlotsReply(RedisRaftCtx *rr, RaftReq *req)
{
    int i, j;
    int alen;

    /* Make sure we have a leader, or return a -CLUSTERDOWN message */
    raft_node_t *leader_node = raft_get_current_leader_node(rr->raft);
    if (!leader_node) {
        RedisModule_ReplyWithError(req->ctx,
                "CLUSTERDOWN No raft leader");
        return;
    }

    ShardingInfo *si = rr->sharding_info;
    RedisModule_ReplyWithArray(req->ctx, si->shard_groups_num);

    for (i = 0; i < si->shard_groups_num; i++) {
        ShardGroup *sg = &si->shard_groups[i];

        /* Dump Raft nodes now. Leader (master) first, followed by others */
        RedisModule_ReplyWithArray(req->ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

        RedisModule_ReplyWithLongLong(req->ctx, sg->start_slot);    /* Start slot */
        RedisModule_ReplyWithLongLong(req->ctx, sg->end_slot);      /* End slot */
        alen = 2;

        if (i == 0) {
            /* Local cluster's ShardGroup: we list the leader node first,
             * followed by all cluster nodes we know. This information does not
             * come from the ShardGroup.
             */

            alen += addClusterSlotNodeReply(rr, req->ctx, leader_node);
            for (j = 0; j < raft_get_num_nodes(rr->raft); j++) {
                raft_node_t *raft_node = raft_get_node_from_idx(rr->raft, j);
                if (raft_node_get_id(raft_node) == raft_get_current_leader(rr->raft) ||
                        !raft_node_is_active(raft_node)) {
                    continue;
                }

                alen += addClusterSlotNodeReply(rr, req->ctx, raft_node);
            }
            RedisModule_ReplySetArrayLength(req->ctx, alen);
        } else {
            /* Remote cluster: we simply dump what the ShardGroup configuration
             * tells us.
             */

            for (j = 0; j < sg->nodes_num; j++) {
                alen += addClusterSlotShardGroupNodeReply(rr, req->ctx, &sg->nodes[j]);
            }

            RedisModule_ReplySetArrayLength(req->ctx, alen);
        }
    }
}

void handleClusterCommand(RedisRaftCtx *rr, RaftReq *req)
{
    RaftRedisCommand *cmd = req->r.redis.cmds.commands[0];

    if (cmd->argc < 2) {
        RedisModule_WrongArity(req->ctx);
        goto exit;
    }

    size_t cmd_len;
    const char *cmd_str = RedisModule_StringPtrLen(cmd->argv[1], &cmd_len);

    if (cmd_len == 5 && !strncasecmp(cmd_str, "SLOTS", 5) && cmd->argc == 2) {
        addClusterSlotsReply(rr, req);
        goto exit;
    } else {
        RedisModule_ReplyWithError(req->ctx,
            "ERR Unknown subcommand ot wrong number of arguments.");
        goto exit;
    }

exit:
    RaftReqFree(req);
}
