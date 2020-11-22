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
 * ShardGroup Handling
 * -------------------------------------------------------------------------- */

/* ShardGroup serialization and deserialization is used in Raft log entries
 * of type RAFT_LOGTYPE_ADD_SHARDGROUP.
 *
 * The format is as follows:
 *      <start-slot>:<end-slot>:<number-of-nodes>\n
 *      <node-uid>:<node host>:<node port>\n
 *      ...
 */

/* Serialize a ShardGroup. Returns a newly allocated null terminated buffer
 * that contains the serialized form.
 */
char *ShardGroupSerialize(ShardGroup *sg)
{
    size_t buf_size = SHARDGROUP_MAXLEN + (SHARDGROUPNODE_MAXLEN * sg->nodes_num) + 1;
    char *buf = RedisModule_Calloc(1, buf_size);
    char *p = buf;

    p = catsnprintf(p, &buf_size, "%u:%u:%u:%u\n", sg->id, sg->start_slot, sg->end_slot, sg->nodes_num);
    for (int i = 0; i < sg->nodes_num; i++) {
        NodeAddr *addr = &sg->nodes[i].addr;
        p = catsnprintf(p, &buf_size, "%s:%s:%d\n", sg->nodes[i].node_id, addr->host, addr->port);
    }

    return buf;
}

/* Deserialize a ShardGroup from the specified buffer. The target ShardGroup is assumed
 * to be uninitialized, and the nodes array will be allocated on demand.
 */
RRStatus ShardGroupDeserialize(const char *buf, size_t buf_len, ShardGroup *sg)
{
    /* Make a mutable, null terminated copy */
    char str[buf_len + 1];
    memcpy(str, buf, buf_len);
    str[buf_len] = '\0';
    char *s = str;

    /* Find and null terminate header */
    char *nl = strchr(str, '\n');
    if (!nl) goto error;
    *nl = '\0';

    memset(sg, 0, sizeof(*sg));

    if (sscanf(s, "%u:%u:%u:%u", &sg->id, &sg->start_slot, &sg->end_slot, &sg->nodes_num) != 4)
        goto error;
    s = nl + 1;

    sg->nodes = RedisModule_Alloc(sizeof(ShardGroupNode) * sg->nodes_num);
    for (int i = 0; i < sg->nodes_num; i++) {
        ShardGroupNode *n = &sg->nodes[i];

        nl = strchr(s, '\n');
        if (!nl) goto error;
        *nl = '\0';

        /* Validate node id */
        char *p = strchr(s, ':');
        if (!p || p - s > RAFT_SHARDGROUP_NODEID_LEN)
            goto error;

        /* Copy node id */
        int len = p - s;
        memcpy(n->node_id, s, len);
        n->node_id[len] = '\0';

        /* Parse node address */
        s = p + 1;
        if (!NodeAddrParse(s, strlen(s), &n->addr))
            goto error;

        s = nl + 1;
    }

    return RR_OK;

error:
    ShardGroupFree(sg);
    return RR_ERROR;
}

/* Initialize a (previously allocated) sharegroup structure.
 * Basically just zero-initializing everything, but a place holder
 * for the future.
 */
void ShardGroupInit(ShardGroup *sg) {
    memset(sg, 0, sizeof(ShardGroup));
}

/* Free internal allocations of a ShardGroup.
 */
void ShardGroupFree(ShardGroup *sg)
{
    if (sg->conn) {
        ConnAsyncTerminate(sg->conn);
        sg->conn = NULL;
    }

    if (sg->nodes) {
        RedisModule_Free(sg->nodes);
        sg->nodes = NULL;
    }
}

/* -----------------------------------------------------------------------------
 * ShardGroup synchronization
 * -------------------------------------------------------------------------- */

/* Compare two shardgroup entities and return an integer less than, equal
 * or greater than zero following common convention.
 *
 * FIXME: We currently only compare node configuration! This is because all
 *        shardgroup configuration changes are expected to only involve nodes
 *        anyway.
 */
int compareShardGroups(ShardGroup *a, ShardGroup *b)
{
    if (a->nodes_num != b->nodes_num) {
        return a->nodes_num - b->nodes_num;
    }

    for (int i = 0; i < a->nodes_num; i++) {
        int ret = strcmp(a->nodes[i].node_id, b->nodes[i].node_id);
        if (ret != 0) {
            return ret;
        }

        ret = strcmp(a->nodes[i].addr.host, b->nodes[i].addr.host);
        if (ret != 0) {
            return ret;
        }

        if (a->nodes[i].addr.port != b->nodes[i].addr.port) {
            return a->nodes[i].addr.port - b->nodes[i].addr.port;
        }
    }

    return 0;
}

/* Parse the reply of a RAFT.SHARDGROUP GET command, expressed
 * as a hiredis redisReply struct, and returns a ShardGroup object.
 *
 * This implements the same logic as ShardGroupParse() but operates
 * on a hiredis reply and not a RedisModuleString argv.
 */
RRStatus parseShardGroupReply(redisReply *reply, ShardGroup *sg)
{
    if (reply->type != REDIS_REPLY_ARRAY || reply->elements < 3) {
        return RR_ERROR;
    }

    /* Start and end slots */
    if (reply->element[0]->type != REDIS_REPLY_INTEGER ||
        reply->element[1]->type != REDIS_REPLY_INTEGER) {
        return RR_ERROR;
    }

    /* Validate node arguments count is correct */
    int num_nodes = (reply->elements - 2) / 2;
    if ((reply->elements - 2) != num_nodes * 2) {
        return RR_ERROR;
    }
    int elemidx = 2; /* Next element to consume */

    sg->start_slot = reply->element[0]->integer;
    sg->end_slot = reply->element[1]->integer;
    sg->nodes_num = num_nodes;
    sg->nodes = RedisModule_Alloc(sizeof(ShardGroupNode) * num_nodes);

    /* Parse nodes */
    for (int i = 0; i < num_nodes; i++) {
        redisReply *elem = reply->element[elemidx++];

        if (elem->type != REDIS_REPLY_STRING ||
            elem->len != RAFT_SHARDGROUP_NODEID_LEN) {
            goto error;
        }

        memcpy(sg->nodes[i].node_id, elem->str, elem->len);
        sg->nodes[i].node_id[elem->len] = '\0';

        /* Advance to node address and port */
        elem = reply->element[elemidx++];
        if (elem->type != REDIS_REPLY_STRING ||
            !NodeAddrParse(elem->str, elem->len, &sg->nodes[i].addr)) {
            goto error;
        }
    }

    return RR_OK;

error:
    RedisModule_Free(sg->nodes);
    sg->nodes = NULL;

    return RR_ERROR;
}

/* Create and append a shardgroup update log entry to the Raft log.
 *
 * We handle both RAFT_LOGTYPE_UPDATE_SHARDGROUP and RAFT_LOGTYPE_ADD_SHARDGROUP.
 *
 * The caller may specify a user_data value, so in case the operation has
 * a bound RaftReq and a client waiting for acknowledgements it will be handled.
 */
RRStatus ShardGroupAppendLogEntry(RedisRaftCtx *rr, ShardGroup *sg, int type, void *user_data)
{
    /* Make sure we're still a leader, could have changed... */
    if (!raft_is_leader(rr->raft)) {
        return RR_ERROR;
    }

    /* Serialize */
    char *payload = ShardGroupSerialize(sg);
    if (!payload) {
        return RR_ERROR;
    }

    raft_entry_t *entry = raft_entry_new(strlen(payload));
    entry->type = type;
    entry->id = rand();
    entry->user_data = user_data;
    memcpy(entry->data, payload, strlen(payload));
    RedisModule_Free(payload);

    /* Submit */
    msg_entry_response_t response;
    int e = raft_recv_entry(rr->raft, entry, &response);
    raft_entry_release(entry);

    if (e != 0) {
        LOG_ERROR("Failed to append shardgroup entry, error %d", e);
        return RR_ERROR;
    }

    return RR_OK;
}

/* A hiredis callback that handles the Redis reply after sending a
 * RAFT.SHARDGROUP GET command.
 *
 * FIXME: Some error handling paths may not be accurate and may require
 *        some cleanup here.
 */

static void handleShardGroupResponse(redisAsyncContext *c, void *r, void *privdata)
{
    UNUSED(c);

    redisReply *reply = r;
    Connection *conn = (Connection *) privdata;
    ShardGroup *sg = ConnGetPrivateData(conn);

    if (!reply) {
        LOG_ERROR("RAFT.SHARDGROUP GET failed: connection dropped.");
    } else if (reply->type == REDIS_REPLY_ERROR) {
        /* -MOVED? */
        if (strlen(reply->str) > 6 && !strncmp(reply->str, "MOVED ", 6)) {
            if (!parseMovedReply(reply->str, &sg->conn_addr)) {
                LOG_ERROR("RAFT.SHARDGROUP GET failed: invalid MOVED response: %s", reply->str);
            } else {
                LOG_VERBOSE("RAFT.SHARDGROUP GET redirected to leader: %s:%d",
                            sg->conn_addr.host, sg->conn_addr.port);
                sg->use_conn_addr = true;
            }
        } else {
            LOG_ERROR("RAFT.SHARDGROUP GET failed: %s", reply->str);
        }
    } else {
        ShardGroup recv_sg;
        ShardGroupInit(&recv_sg);

        if (parseShardGroupReply(reply, &recv_sg) == RR_ERROR) {
            LOG_ERROR("RAFT.SHARDGROUP GET invalid reply.");
        } else {
            LOG_INFO("Received shardgroup reply for %u!", sg->id);
            sg->use_conn_addr = true;
            sg->last_updated = RedisModule_Milliseconds();
            sg->update_in_progress = false;

            /* Issue update */
            recv_sg.id = sg->id;    /* Copy ID to allow correlation */
            if (compareShardGroups(sg, &recv_sg) != 0) {
                ShardGroupAppendLogEntry(ConnGetRedisRaftCtx(conn), &recv_sg,
                                         RAFT_LOGTYPE_UPDATE_SHARDGROUP, NULL);
            }
            ShardGroupFree(&recv_sg);

            return;
        }
    }

    /* Mark connection as disconnected and prepare to connect to another
     * node.
     */
    ConnMarkDisconnected(conn);
}

/* Issue a RAFT.SHARDGROUP GET command on an active connection and register
 * a callback to process the reply.
 */
static void sendShardGroupRequest(Connection *conn)
{
    /* Failed to connect? Advance node_idx to attempt another node. */
    if (!ConnIsConnected(conn)) {
        return;
    }

    /* Request configuration */
    redisAsyncContext *rc = ConnGetRedisCtx(conn);
    if (redisAsyncCommand(rc, handleShardGroupResponse, conn,
                "RAFT.SHARDGROUP %s", "GET") != REDIS_OK) {

        redisAsyncDisconnect(rc);
        ConnMarkDisconnected(conn);
        return;
    }

    /* We'll be back with handleShardGroupResponse */
}

/* Initiate a connection using an existing Connection object already
 * associated with a shardgroup.
 *
 * This is called periodically as an idle callback to address the
 * initial connection and future re-connects.
 */
static void establishShardGroupConn(Connection *conn)
{
    ShardGroup *sg = ConnGetPrivateData(conn);
    RedisRaftCtx *rr = ConnGetRedisRaftCtx(conn);
    NodeAddr *addr;

    /* Only establish the connection if we're a leader, and if it's already
     * time to update.
     */
    if (!raft_is_leader(rr->raft) ||
        RedisModule_Milliseconds() - sg->last_updated < rr->config->shardgroup_update_interval) {
       return;
    }

    if (sg->use_conn_addr) {
        addr = &sg->conn_addr;
    } else {
        if (sg->node_conn_idx >= sg->nodes_num) {
            sg->node_conn_idx = 0;
        }
        addr = &sg->nodes[sg->node_conn_idx++].addr;
    }

    LOG_DEBUG("Initiating shardgroup(%u) connection to %s:%u", sg->id, addr->host, addr->port);
    sg->update_in_progress = true;
    ConnConnect(conn, addr, sendShardGroupRequest);

    /* Disable use_conn_addr, as by default we'll try the next address on a
     * reconnect. It will be reset to true if the connection was successful, or
     * if conn_addr was populated by a -MOVED reply.
     */
    sg->use_conn_addr = false;
}

/* Called periodically by the main loop when cluster mode is enabled.
 *
 * Currently we use this to iterate all shardgroups and trigger an
 * update for shardgroups that have not been updated recently.
 */
void ClusterPeriodicCall(RedisRaftCtx *rr)
{
    /* See if we have any shardgroups that need a refresh.
     */

    if (!raft_is_leader(rr->raft)) {
        return;
    }

    long long mstime = RedisModule_Milliseconds();

    ShardingInfo *si = rr->sharding_info;
    for (int i = 0; i < si->shard_groups_num; i++) {
        ShardGroup *sg = si->shard_groups[i];
        if (!sg->nodes_num || !sg->conn || mstime - sg->last_updated < rr->config->shardgroup_update_interval ||
            !ConnIsConnected(sg->conn) || sg->update_in_progress) {
            continue;
        }

        sendShardGroupRequest(sg->conn);
    }
}

/* -----------------------------------------------------------------------------
 * ShardingInfo Handling
 * -------------------------------------------------------------------------- */

/* Save ShardingInfo to RDB during snapshotting. This gets invoked by rdbSaveSnapshotInfo
 * which uses a pseudo key to get triggered.
 *
 * We skip writing the first shardgroup that represents our local cluster.
 */

void ShardingInfoRDBSave(RedisModuleIO *rdb)
{
    RedisRaftCtx *rr = &redis_raft;
    ShardingInfo *si = rr->sharding_info;

    /* If no ShardingInfo, write a zero count and abort. */
    if (!si) {
        RedisModule_SaveUnsigned(rdb, 0);
        return;
    }

    /* When saving, skip shardgroup #1 which is the local cluster */
    RedisModule_SaveUnsigned(rdb, si->shard_groups_num - 1);
    for (int i = 1; i < si->shard_groups_num; i++) {
        ShardGroup *sg = si->shard_groups[i];

        RedisModule_SaveUnsigned(rdb, sg->id);
        RedisModule_SaveUnsigned(rdb, sg->start_slot);
        RedisModule_SaveUnsigned(rdb, sg->end_slot);
        RedisModule_SaveUnsigned(rdb, sg->nodes_num);
        for (int j = 0; j < sg->nodes_num; j++) {
            ShardGroupNode *n = &sg->nodes[j];
            RedisModule_SaveStringBuffer(rdb, n->node_id, strlen(n->node_id));
            RedisModule_SaveStringBuffer(rdb, n->addr.host, strlen(n->addr.host));
            RedisModule_SaveUnsigned(rdb, n->addr.port);
        }
    }
}

/* Load ShardingInfo from RDB. This gets invoked by rdbLoadSnapshotInfo which uses a
 * pseudo key to get triggered.
 *
 * NOTE: Some attention to sequence of events is required here. When a snapshot is
 * loaded, the RDB loading is guaranteed to take place when everything is already
 * well initialized.
 *
 * However, we need to also consider the initial loading of RDB, which can take
 * place after the module has been loaded but before RedisRaft has initialized
 * completely. This logic has already been implemented correctly for SnapshotInfo
 * and we need to consider consolidating everything and possibly move to more
 * modern Module API capabilities that can let us avoid piggybacking on keys.
 */

void ShardingInfoRDBLoad(RedisModuleIO *rdb)
{
    RedisRaftCtx *rr = &redis_raft;
    ShardingInfo *si = rr->sharding_info;

    /* Always read the shards_group_num, because it's always written (but may
     * be zero).
     */
    unsigned int rdb_shard_groups_num = RedisModule_LoadUnsigned(rdb);
    if (!rdb_shard_groups_num) {
        /* No shardgroups. This could mean no sharding, or simply no shardgrups
         * to read. If we have ShardingInfo we'll reset it.
         */
        if (si)
            ShardingInfoReset(rr);

        return;
    }

    /* If we have something to load, we need to reset ShardingInfo first.
     * We also need to be sure we're in cluster mode, i.e. that si was
     * initialized.
     */
    RedisModule_Assert(si != NULL);
    ShardingInfoReset(rr);

    /* Load individual shard groups */
    for (int i = 0; i < rdb_shard_groups_num; i++) {
        ShardGroup sg;
        ShardGroupInit(&sg);

        sg.id = RedisModule_LoadUnsigned(rdb);
        sg.start_slot = RedisModule_LoadUnsigned(rdb);
        sg.end_slot = RedisModule_LoadUnsigned(rdb);
        sg.nodes_num = RedisModule_LoadUnsigned(rdb);

        /* Load nodes */
        sg.nodes = RedisModule_Calloc(sg.nodes_num, sizeof(ShardGroupNode));
        for (int j = 0; j < sg.nodes_num; j++) {
            ShardGroupNode *n = &sg.nodes[j];
            size_t len;
            char *buf = RedisModule_LoadStringBuffer(rdb, &len);

            RedisModule_Assert(len < sizeof(n->node_id));
            memcpy(n->node_id, buf, len);
            n->node_id[len] = '\0';
            RedisModule_Free(buf);

            buf = RedisModule_LoadStringBuffer(rdb, &len);
            RedisModule_Assert(len < sizeof(n->addr.host));
            memcpy(n->addr.host, buf, len);
            n->addr.host[len] = '\0';
            RedisModule_Free(buf);

            n->addr.port = RedisModule_LoadUnsigned(rdb);
        }

        /* This also handles all validation so serious violations, although
         * should never exist, will be caught.
         */
        RRStatus ret = ShardingInfoAddShardGroup(rr, &sg);
        RedisModule_Assert(ret == RR_OK);

        ShardGroupFree(&sg);
    }
}

/* Validate a new shardgroup and make sure there are no conflicts with
 * current ShardingInfo configuration.
 *
 * Currently we check:
 * 1. Slot range is valid.
 * 2. All specified slots are currently unassigned.
 */

RRStatus ShardingInfoValidateShardGroup(RedisRaftCtx *rr, ShardGroup *new_sg)
{
    ShardingInfo *si = rr->sharding_info;

    /* Verify all specified slots are available */
    if (!HashSlotRangeValid(new_sg->start_slot, new_sg->end_slot)) {
        LOG_ERROR("Invalid shardgroup: bad slots range %u-%u",
                new_sg->start_slot, new_sg->end_slot);
        return RR_ERROR;
    }

    for (int i = new_sg->start_slot; i <= new_sg->end_slot; i++) {
        if (si->hash_slots_map[i] != 0) {
            LOG_ERROR("Invalid shardgroup: hash slot already mapped: %u", i);
            return RR_ERROR;
        }
    }

    return RR_OK;
}

/* Update an existing ShardGroup in the active ShardingInfo.
 *
 * FIXME: We currently only handle updating nodes but don't support remapping
 *        hash slots.
 */

RRStatus ShardingInfoUpdateShardGroup(RedisRaftCtx *rr, ShardGroup *new_sg)
{
    ShardingInfo *si = rr->sharding_info;

    if (new_sg->id < 1 || new_sg->id > si->shard_groups_num)
        return RR_ERROR;

    ShardGroup *sg = si->shard_groups[new_sg->id - 1];

    sg->nodes_num = new_sg->nodes_num;
    sg->nodes = RedisModule_Realloc(sg->nodes, sizeof(ShardGroupNode) * sg->nodes_num);
    memcpy(sg->nodes, new_sg->nodes, sizeof(ShardGroupNode) * sg->nodes_num);

    return RR_OK;
}

/* Add a new ShardGroup to the active ShardingInfo. Validation is done according to
 * ShardingInfoValidateShardGroup() above.
 */

RRStatus ShardingInfoAddShardGroup(RedisRaftCtx *rr, ShardGroup *new_sg)
{
    ShardingInfo *si = rr->sharding_info;

    /* Validate first */
    if (ShardingInfoValidateShardGroup(rr, new_sg) != RR_OK)
        return RR_ERROR;

    si->shard_groups_num++;
    si->shard_groups = RedisModule_Realloc(si->shard_groups, sizeof(ShardGroup *) * si->shard_groups_num);

    ShardGroup *sg = si->shard_groups[si->shard_groups_num-1] = RedisModule_Alloc(sizeof(ShardGroup));
    sg->id = si->shard_groups_num;
    sg->start_slot = new_sg->start_slot;
    sg->end_slot = new_sg->end_slot;
    sg->nodes_num = new_sg->nodes_num;
    sg->next_redir = 0;
    sg->use_conn_addr = false;
    sg->node_conn_idx = 0;
    sg->conn = NULL;
    sg->nodes = RedisModule_Alloc(sizeof(ShardGroupNode) * new_sg->nodes_num);
    memcpy(sg->nodes, new_sg->nodes, sizeof(ShardGroupNode) * new_sg->nodes_num);

    /* Do slot mapping */
    for (int i = new_sg->start_slot; i <= new_sg->end_slot; i++) {
        si->hash_slots_map[i] = si->shard_groups_num;
    }

    /* Create a connection object for syncing. We assume that if nodes_num is zero
     * this is the shardgroup entry for our local cluster so it can be skipped.
     * */
    if (sg->nodes_num > 0) {
        sg->conn = ConnCreate(rr, sg, establishShardGroupConn, NULL);
    }

    return RR_OK;
}

/* Parse a ShardGroup specification as passed directly to RAFT.SHARDGROUP ADD.
 * Shard group syntax is as follows:
 *
 *  [start slot] [end slot] [node-uid node-addr:node-port] [node-uid node-addr:node-port...]
 *
 * If parsing errors are encountered, an error reply is generated on the supplied RedisModuleCtx,
 * and RR_ERROR is returned.
 */

RRStatus ShardGroupParse(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, ShardGroup *sg)
{
    long long start_slot, end_slot;

    ShardGroupInit(sg);

    /* Slot range */
    if (RedisModule_StringToLongLong(argv[0], &start_slot) != REDISMODULE_OK ||
            RedisModule_StringToLongLong(argv[1], &end_slot) != REDISMODULE_OK ||
            !HashSlotRangeValid(start_slot, end_slot)) {
        RedisModule_ReplyWithError(ctx, "ERR invalid slot range");
        goto error;
    }

    /* Validate node arguments count is correct */
    int num_nodes = (argc - 2) / 2;
    if ((argc - 2) != num_nodes * 2) {
        RedisModule_WrongArity(ctx);
        goto error;
    }
    int argidx = 2; /* Next arg to consume */

    /* Parse nodes */
    sg->start_slot = start_slot;
    sg->end_slot = end_slot;
    sg->nodes_num = num_nodes;
    sg->nodes = RedisModule_Alloc(sizeof(ShardGroupNode) * num_nodes);
    for (int i = 0; i < num_nodes; i++) {
        size_t len;
        const char *str = RedisModule_StringPtrLen(argv[argidx++], &len);

        if (len != RAFT_SHARDGROUP_NODEID_LEN) {
            RedisModule_ReplyWithError(ctx, "ERR invalid node id length");
            goto error;
        }

        memcpy(sg->nodes[i].node_id, str, len);
        sg->nodes[i].node_id[len] = '\0';

        str = RedisModule_StringPtrLen(argv[argidx++], &len);
        if (!NodeAddrParse(str, len, &sg->nodes[i].addr)) {
            RedisModule_ReplyWithError(ctx, "ERR invalid node address/port");
            goto error;
        }
    }

    return RR_OK;

error:
    ShardGroupFree(sg);
    return RR_ERROR;
}

/* Initialize ShardingInfo and add our local RedisRaft cluster as the first
 * ShardGroup.
 */

void ShardingInfoInit(RedisRaftCtx *rr)
{
    rr->sharding_info = RedisModule_Calloc(1, sizeof(ShardingInfo));

    ShardingInfoReset(rr);
}

/* Free and reset the ShardingInfo structure.
 *
 * This is called after ShardingInfo has already been allocated, and typically
 * right before loading serialized ShardGroups from a snapshot.
 */
void ShardingInfoReset(RedisRaftCtx *rr)
{
    ShardingInfo *si = rr->sharding_info;

    for (int i = 0; i < si->shard_groups_num; i++) {
        ShardGroupFree(si->shard_groups[i]);
        RedisModule_Free(si->shard_groups[i]);
        si->shard_groups[i] = NULL;
    }

    if (si->shard_groups)
        RedisModule_Free(si->shard_groups);
    si->shard_groups = NULL;
    si->shard_groups_num = 0;

    /* Reset array */
    for (int i = 0; i < REDIS_RAFT_HASH_SLOTS; i++)
        si->hash_slots_map[i] = 0;

    /* Add our local mapping */
    ShardGroup sg = {
        .start_slot = rr->config->cluster_start_hslot,
        .end_slot = rr->config->cluster_end_hslot,
        .nodes_num = 0,
        .nodes = NULL
    };

    RRStatus ret = ShardingInfoAddShardGroup(rr, &sg);
    RedisModule_Assert(ret == RR_OK);
}

/* Issue a COMMAND GETKEYS command to fetch the list of keys addressed
 * by the specified command.
 *
 * THIS IS DEPRECATED! Starting with Redis 6.0.9 a Module API call is available
 * to fetch this information, so this is left here just for a short while to
 * maintain backwards compatibility.
 *
 * Using this technique is significantly slower.
 */

RedisModuleCallReply *execCommandGetKeys(RedisRaftCtx *rr, RaftRedisCommand *cmd)
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
 *
 * FIXME: This is a LEGACY VERSION based on 'COMMAND GETKEYS', which is here only
 * to allow running on Redis versions older than 6.0.9.
 */

static RRStatus legacy_computeHashSlot(RedisRaftCtx *rr, RaftReq *req)
{
    int slot = -1;

    RaftRedisCommandArray *cmds = &req->r.redis.cmds;
    for (int i = 0; i < cmds->len; i++) {
        RaftRedisCommand *cmd = cmds->commands[i];

        /* Iterate command keys */
        RedisModuleCallReply *reply = execCommandGetKeys(rr, cmd);
        for (int j = 0; j < RedisModule_CallReplyLength(reply); j++) {
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

/* Compute the hash slot for a RaftRedisCommandArray list of commands and update
 * the entry.
 */

RRStatus computeHashSlot(RedisRaftCtx *rr, RaftReq *req)
{
    int slot = -1;

    if (RedisModule_GetCommandKeys == NULL)
        return legacy_computeHashSlot(rr, req);

    RaftRedisCommandArray *cmds = &req->r.redis.cmds;
    for (int i = 0; i < cmds->len; i++) {
        RaftRedisCommand *cmd = cmds->commands[i];

        /* Iterate command keys */
        int num_keys = 0;
        int *keyindex = RedisModule_GetCommandKeys(rr->ctx, cmd->argv, cmd->argc, &num_keys);
        for (int j = 0; j < num_keys; j++) {
            size_t key_len;
            const char *key = RedisModule_StringPtrLen(cmd->argv[keyindex[j]], &key_len);
            int thisslot = keyHashSlot(key, key_len);

            if (slot == -1) {
                /* First key */
                slot = thisslot;
            } else {
                if (slot != thisslot) {
                    RedisModule_Free(keyindex);
                    return RR_ERROR;
                }
            }
        }

        RedisModule_Free(keyindex);
    }

    req->r.redis.hash_slot = slot;


    return RR_OK;
}

/* Produces a CLUSTER SLOTS compatible reply entry for the specified local cluster node.
 */
static int addClusterSlotNodeReply(RedisRaftCtx *rr, RedisModuleCtx *ctx, raft_node_t *raft_node)
{
    Node *node = raft_node_get_udata(raft_node);
    NodeAddr *addr;
    char node_id[RAFT_SHARDGROUP_NODEID_LEN+1];

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

    snprintf(node_id, sizeof(node_id), "%.32s%08x", rr->log->dbid, raft_node_get_id(raft_node));
    RedisModule_ReplyWithCString(ctx, node_id);

    return 1;
}

/* Produce a CLUSTER SLOTS compatible reply entry for the specified shardgroup node.
 */
static int addClusterSlotShardGroupNodeReply(RedisRaftCtx *rr, RedisModuleCtx *ctx, ShardGroupNode *sgn)
{
    UNUSED(rr);

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

/* Produce a CLUSTER SLOTS compatible reply, including:
 *
 * 1. Local cluster's slot range and nodes.
 * 2. All configured shardgroups with their slot ranges and nodes.
 */

static void addClusterSlotsReply(RedisRaftCtx *rr, RaftReq *req)
{
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

    for (int i = 0; i < si->shard_groups_num; i++) {
        ShardGroup *sg = si->shard_groups[i];

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
            for (int j = 0; j < raft_get_num_nodes(rr->raft); j++) {
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

            for (int j = 0; j < sg->nodes_num; j++) {
                alen += addClusterSlotShardGroupNodeReply(rr, req->ctx, &sg->nodes[j]);
            }

            RedisModule_ReplySetArrayLength(req->ctx, alen);
        }
    }
}

/* Process CLUSTER commands, as intercepted earlier by the Raft module.
 *
 * Currently only supporting CLUSTER SLOTS.
 */
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
            "ERR Unknown subcommand or wrong number of arguments.");
        goto exit;
    }

exit:
    RaftReqFree(req);
}

/* -----------------------------------------------------------------------------
 * ShardGroup Link Implementation
 * -------------------------------------------------------------------------- */

/* The state we track when performing a RAFT.SHARDGROUP LINK operation.
 * It is short lived until the operation is complete and a response is
 * returned to the user.
 */
typedef struct ShardGroupLinkState {
    NodeAddrListElement *addr;          /* Address list to try */
    NodeAddrListElement *addr_iter;     /* Current iterator in list */
    Connection *conn;                   /* Connection we use */
    RaftReq *req;                       /* Original RaftReq, so we can return a reply */
} ShardGroupLinkState;

/* Free a ShardGroupLinkState structure.
 */
static void linkFree(void *privdata)
{
    ShardGroupLinkState *state = privdata;

    NodeAddrListFree(state->addr);
    if (state->req) {
        /* Normally a reply is returned and this should be NULL. If it is not,
         * we need to reply something before freeing as the client is still blocking.
         */
        RedisModule_ReplyWithError(state->req->ctx, "operation failed, please consult the logs.");
        RaftReqFree(state->req);
        state->req = NULL;
    }

    RedisModule_Free(state);
}

/* Handle the received RAFT.SHARDGROUP GET reply from the remote cluster.
 *
 * Basically, just parse, validate and submit as RAFT_LOGTYPE_SHARDGROUP_ADD
 * entry to the Raft log.
 */

static void linkHandleResponse(redisAsyncContext *c, void *r, void *privdata)
{
    UNUSED(c);

    redisReply *reply = r;
    Connection *conn = (Connection *) privdata;
    RedisRaftCtx *rr = ConnGetRedisRaftCtx(conn);
    ShardGroupLinkState *state = ConnGetPrivateData(conn);

    if (!reply) {
        LOG_ERROR("RAFT.SHARDGROUP GET failed: connection dropped.");
    } else if (reply->type == REDIS_REPLY_ERROR) {
        /* -MOVED? */
        if (strlen(reply->str) > 6 && !strncmp(reply->str, "MOVED ", 6)) {
            NodeAddr addr;
            if (!parseMovedReply(reply->str, &addr)) {
                LOG_ERROR("RAFT.SHARDGROUP GET failed: invalid MOVED response: %s", reply->str);
            } else {
                LOG_VERBOSE("RAFT.SHARDGROUP GET redirected to leader: %s:%d",
                            addr.host, addr.port);
                NodeAddrListAddElement(&state->addr, &addr);
            }
        } else {
            LOG_ERROR("RAFT.SHARDGROUP GET failed: %s", reply->str);
        }
    } else {
        ShardGroup recv_sg;
        ShardGroupInit(&recv_sg);

        if (parseShardGroupReply(reply, &recv_sg) == RR_ERROR) {
            LOG_ERROR("RAFT.SHARDGROUP GET invalid reply.");
        } else {
            /* Validate */
            if (ShardingInfoValidateShardGroup(rr, &recv_sg) != RR_OK) {
                LOG_ERROR("Received shardgroup failed validation!");
            } else {
                LOG_VERBOSE("Shardgroup link: %s:%u: received configuration, propagating to Raft log.",
                            state->addr_iter->addr.host, state->addr_iter->addr.port);
                if (ShardGroupAppendLogEntry(ConnGetRedisRaftCtx(conn), &recv_sg,
                                         RAFT_LOGTYPE_ADD_SHARDGROUP, state->req) == RR_OK) {
                    state->req = NULL;

                    ConnAsyncTerminate(conn);
                    ShardGroupFree(&recv_sg);
                    return;
                }
            }
            ShardGroupFree(&recv_sg);
        }
    }

    /* Mark connection as disconnected and prepare to connect to another
     * node.
     */
    ConnMarkDisconnected(conn);
}

/* Issue a RAFT.SHARDGROUP GET command on an active connection and register
 * a callback to process the reply.
 */
static void linkSendRequest(Connection *conn)
{
    /* Failed to connect? Advance node_idx to attempt another node. */
    if (!ConnIsConnected(conn)) {
        return;
    }

    ShardGroupLinkState *state = ConnGetPrivateData(conn);
    LOG_VERBOSE("Shardgroup link %s:%u: connected, requesting configuration",
                state->addr_iter->addr.host,
                state->addr_iter->addr.port);

    /* Request configuration */
    redisAsyncContext *rc = ConnGetRedisCtx(conn);
    if (redisAsyncCommand(rc, linkHandleResponse, conn,
                          "RAFT.SHARDGROUP %s", "GET") != REDIS_OK) {

        redisAsyncDisconnect(rc);
        ConnMarkDisconnected(conn);
        return;
    }

    /* We'll be back with handleShardGroupResponse */
}

/* Establish a connection with the remote cluster to deliver a RAFT.SHARDGROUP GET
 * request.
 *
 * We use addr and addr_iter to handle iteration through multiple addresses.
 * Normally, we should have only a single address which is specified by the suer.
 * However, if a -MOVED reply is received we append it here and initiate a retry
 * (which may, although not likely, happen several times).
 */

static void linkConnect(Connection *conn)
{
    ShardGroupLinkState *state = ConnGetPrivateData(conn);

    /* First iteration? */
    if (!state->addr_iter) {
        state->addr_iter = state->addr;
    } else {
        /* Try next address */
        state->addr_iter = state->addr_iter->next;
    }

    /* If all attempts were exhausted, abort. */
    if (!state->addr_iter) {
        /* Nothing else to try? */
        RedisModule_ReplyWithError(state->req->ctx, "failed to link, please check the logs.");

        /* Release client */
        RaftReqFree(state->req);
        state->req = NULL;

        ConnAsyncTerminate(conn);
        return;
    }

    LOG_VERBOSE("Shardgroup link: connecting to %s:%u",
                state->addr_iter->addr.host, state->addr_iter->addr.port);

    /* Establish connection. We silently ignore errors here as we'll
     * just get iterated again in the future.
     */
    ConnConnect(state->conn, &state->addr_iter->addr, linkSendRequest);
}

/* Handle a RAFT.SHARDGROUP LINK request.
 */
void handleShardGroupLink(RedisRaftCtx *rr, RaftReq *req)
{
    /* Must be done on a leader */
    if (checkRaftState(rr, req) == RR_ERROR ||
        checkLeader(rr, req, NULL) == RR_ERROR) {
        goto exit;
    }

    LOG_INFO("Attempting to link shardgroup %s:%u",
             req->r.shardgroup_link.addr.host,
             req->r.shardgroup_link.addr.port);

    ShardGroupLinkState *state = RedisModule_Calloc(1, sizeof(ShardGroupLinkState));
    NodeAddrListAddElement(&state->addr, &req->r.shardgroup_link.addr);
    state->req = req;
    state->conn = ConnCreate(rr, state, linkConnect, linkFree);

    return;

exit:
    RaftReqFree(req);

}
