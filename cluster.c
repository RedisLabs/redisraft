/*
 * This file is part of RedisRaft.
 *
 * Copyright (c) 2020-2021 Redis Ltd.
 *
 * RedisRaft is licensed under the Redis Source Available License (RSAL).
 */

#include <string.h>
#include <strings.h>
#include <stdlib.h>

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
 *      <id>:<start-slot>:<end-slot>:<number-of-nodes>\n
 *      <node-uid>:<node host>:<node port>\n
 *      ...
 */

/* Serialize a ShardGroup. Returns a newly allocated null terminated buffer
 * that contains the serialized form.
 */
char *ShardGroupSerialize(ShardGroup *sg)
{
    size_t buf_size = SHARDGROUP_MAXLEN +
            (SHARDGROUPNODE_MAXLEN * sg->nodes_num) + 1 +
            (SLOT_RANGE_MAXLEN * sg->slot_ranges_num) + 1;
    char *buf = RedisModule_Calloc(1, buf_size);
    char *p = buf;

    /* shard group id */
    p = catsnprintf(p, &buf_size, "%s\n", sg->id);

    /* number of slot ranges and nodes */
    p = catsnprintf(p, &buf_size, "%u\n%u\n", sg->slot_ranges_num, sg->nodes_num);

    for (int i = 0; i < sg->slot_ranges_num; i++) {
        /* individual slot ranges */
        ShardGroupSlotRange *sr = &sg->slot_ranges[i];
        p = catsnprintf(p, &buf_size, "%u\n%u\n%u\n", sr->start_slot, sr->end_slot, sr->type);
    }

    for (int i = 0; i < sg->nodes_num; i++) {
        /* individual nodes */
        NodeAddr *addr = &sg->nodes[i].addr;
        p = catsnprintf(p, &buf_size, "%s\n%s:%d\n", sg->nodes[i].node_id, addr->host, addr->port);
    }

    return buf;
}

/* Deserialize a ShardGroup from the specified buffer. The target ShardGroup is assumed
 * to be uninitialized, and the nodes array will be allocated on demand.
 */
RRStatus ShardGroupDeserialize(const char *buf, size_t buf_len, ShardGroup *sg)
{
    /* Make a mutable, null terminated copy */
    const char *s = buf;
    const char *end = buf + buf_len;
    char *nl;
    char *endptr;
    memset(sg, 0, sizeof(*sg));

    /* Find shard group id */
    nl = memchr(s, '\n', end - s);
    if (!nl) {
        goto error;
    }

    /* Validate shard group id */
    if (nl - s > RAFT_DBID_LEN) {
        goto error;
    }

    memcpy(sg->id, s, nl - s);
    s = nl + 1;

    /* Find slot_ranges_num*/
    nl = memchr(s, '\n', end - s);
    if (!nl) {
        goto error;
    }

    sg->slot_ranges_num = strtoul(s, &endptr, 10);
    RedisModule_Assert(endptr == nl);
    s = nl + 1;

    /* Find nodes_num */
    nl = memchr(s, '\n', end - s);
    if (!nl) {
        goto error;
    }

    sg->nodes_num = strtoul(s, &endptr, 10);
    RedisModule_Assert(endptr == nl);
    s = nl+1;

    /* Read in slot ranges */
    sg->slot_ranges = RedisModule_Calloc(sg->slot_ranges_num, sizeof(ShardGroupSlotRange));
    for (int i = 0; i < sg->slot_ranges_num; i++) {
        ShardGroupSlotRange *r = &sg->slot_ranges[i];

        /* parse individual slot range */
        nl = memchr(s, '\n', end - s);
        if (!nl) {
            goto error;
        }

        r->start_slot = strtoul(s, &endptr, 10);
        RedisModule_Assert(endptr == nl);
        s = nl + 1;

        nl = memchr(s, '\n', end - s);
        if (!nl) {
            goto error;
        }

        r->end_slot = strtoul(s, &endptr, 10);
        RedisModule_Assert(endptr == nl);
        s = nl + 1;

        nl = memchr(s, '\n', end - s);
        if (!nl) {
            goto error;
        }

        r->type = strtoul(s, &endptr, 10);
        RedisModule_Assert(endptr == nl);
        s = nl + 1;
    }

    /* read in shard group nodes */
    sg->nodes = RedisModule_Calloc(sg->nodes_num, sizeof(ShardGroupNode));
    for (int i = 0; i < sg->nodes_num; i++) {
        /* parse individual node */
        ShardGroupNode *n = &sg->nodes[i];

        nl = memchr(s, '\n', end - s);
        if (!nl || nl - s > RAFT_SHARDGROUP_NODEID_LEN) {
            goto error;
        }

        /* Copy node id */
        size_t len = nl - s;
        memcpy(n->node_id, s, len);
        n->node_id[len] = '\0';

        /* Parse node address */
        s = nl + 1;
        nl = memchr(s, '\n', end - s);
        if (!nl || !NodeAddrParse(s, nl-s, &n->addr)) {
            goto error;
        }

        s = nl + 1;
    }

    return RR_OK;

error:
    ShardGroupTerm(sg);
    return RR_ERROR;
}

/* Initialize a (previously allocated) shardgroup structure.
 * Basically just zero-initializing everything, but a place holder
 * for the future.
 */
void ShardGroupInit(ShardGroup *sg) {
    memset(sg, 0, sizeof(ShardGroup));
}

/* Free internal allocations of a ShardGroup.
 */
void ShardGroupTerm(ShardGroup *sg)
{
    if (sg->conn) {
        ConnAsyncTerminate(sg->conn);
        sg->conn = NULL;
    }

    if (sg->slot_ranges) {
        RedisModule_Free(sg->slot_ranges);
        sg->slot_ranges = NULL;
    }

    if (sg->nodes) {
        RedisModule_Free(sg->nodes);
        sg->nodes = NULL;
    }
}

ShardGroup *ShardGroupCreate() {
    ShardGroup *sg = RedisModule_Calloc(1, sizeof(*sg));
    ShardGroupInit(sg);
    return sg;
}

void ShardGroupFree(ShardGroup *sg) {
    ShardGroupTerm(sg);
    RedisModule_Free(sg);
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
    if (reply->type != REDIS_REPLY_ARRAY) {
        return RR_ERROR;
    }

    /* check if it has 2 elements and both are arrays */
    if (reply->elements != 3 ||
        reply->element[0]->type != REDIS_REPLY_STRING || /* shardgroup_id */
        reply->element[1]->type != REDIS_REPLY_ARRAY || /* slots array */
        reply->element[2]->type != REDIS_REPLY_ARRAY) { /* nodes array */
        return RR_ERROR;
    }

    strncpy(sg->id, reply->element[0]->str, RAFT_DBID_LEN);
    sg->id[RAFT_DBID_LEN] = '\0';
    sg->slot_ranges_num = reply->element[1]->elements;
    sg->nodes_num = reply->element[2]->elements;

    sg->slot_ranges = RedisModule_Calloc(sg->slot_ranges_num, sizeof(ShardGroupSlotRange));
    for (int i = 0; i < sg->slot_ranges_num; i++) {
        if (reply->element[1]->element[i]->type != REDIS_REPLY_ARRAY ||
            reply->element[1]->element[i]->elements != 3 ||
            reply->element[1]->element[i]->element[0]->type != REDIS_REPLY_INTEGER || /* start slot */
            reply->element[1]->element[i]->element[1]->type != REDIS_REPLY_INTEGER || /* end slot */
            reply->element[1]->element[i]->element[2]->type != REDIS_REPLY_INTEGER) { /* slot types */
            goto error;
        }

        sg->slot_ranges[i].start_slot = reply->element[1]->element[i]->element[0]->integer;
        sg->slot_ranges[i].end_slot = reply->element[1]->element[i]->element[1]->integer;
        sg->slot_ranges[i].type = reply->element[1]->element[i]->element[2]->integer;

        if (!HashSlotRangeValid(sg->slot_ranges[i].start_slot, sg->slot_ranges[i].end_slot) ||
            !SlotRangeTypeValid(sg->slot_ranges[i].type)) {
            goto error;
        }
    }

    sg->nodes = RedisModule_Calloc(sg->nodes_num, sizeof(ShardGroupNode));

    /* Parse nodes */
    for (int i = 0; i < sg->nodes_num; i++) {
        if (reply->element[2]->element[i]->type != REDIS_REPLY_ARRAY ||
            reply->element[2]->element[i]->elements != 2) { /* 1) nodeid and 2) addr (host:port) */
            goto error;
        }

        redisReply *elem = reply->element[2]->element[i]->element[0];

        if (elem->type != REDIS_REPLY_STRING ||
            elem->len != RAFT_SHARDGROUP_NODEID_LEN) {
            goto error;
        }

        memcpy(sg->nodes[i].node_id, elem->str, elem->len);
        sg->nodes[i].node_id[elem->len] = '\0';

        /* Advance to node address and port */
        elem = reply->element[2]->element[i]->element[1];
        if (elem->type != REDIS_REPLY_STRING ||
            !NodeAddrParse(elem->str, elem->len, &sg->nodes[i].addr)) {
            goto error;
        }
    }

    return RR_OK;

error:
    if (sg->slot_ranges) {
        RedisModule_Free(sg->slot_ranges);
        sg->slot_ranges = NULL;
    }
    if (sg->nodes) {
        RedisModule_Free(sg->nodes);
        sg->nodes = NULL;
    }

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

RRStatus ShardGroupsAppendLogEntry(RedisRaftCtx *rr, int num_sg, ShardGroup **sg, int type, void *user_data)
{
    /* Serialize */
    /* We reuse the existing shard group serialization
     * Format:
     * <num shard groups>:strlen(sg #1 payload):sg #1 payload|...:strlen(sg #n payload):sg #n payload|
     */
    size_t buflen = 4096;
    char *buf = RedisModule_Calloc(1, buflen);
    buf = catsnprintf(buf, &buflen, "%d\n", num_sg);
    for (int i = 0; i < num_sg; i++) {
        char *payload = ShardGroupSerialize(sg[i]);
        buf = catsnprintf(buf, &buflen, "%lu\n%s\n", strlen(payload), payload);
        RedisModule_Free(payload);
    }
    size_t payload_len = strlen(buf);
    raft_entry_t *entry = raft_entry_new(payload_len);
    entry->type = type;
    entry->id = rand();
    entry->user_data = user_data;
    memcpy(entry->data, buf, payload_len);
    RedisModule_Free(buf);

    /* Submit */
    msg_entry_response_t response;
    int e = raft_recv_entry(rr->raft, entry, &response);
    raft_entry_release(entry);

    if (e != 0) {
        LOG_ERROR("Failed to append shardgroups entry, error %d", e);
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
            LOG_DEBUG("Received shardgroup %s reply.", recv_sg.id);
            sg->use_conn_addr = true;
            sg->last_updated = RedisModule_Milliseconds();
            sg->update_in_progress = false;

            /* Issue update */
            memcpy(recv_sg.id, sg->id, RAFT_DBID_LEN); /* Copy ID to allow correlation */
            recv_sg.id[RAFT_DBID_LEN] = '\0';
            if (compareShardGroups(sg, &recv_sg) != 0) {
                ShardGroupAppendLogEntry(ConnGetRedisRaftCtx(conn), &recv_sg,
                                         RAFT_LOGTYPE_UPDATE_SHARDGROUP, NULL);
            }
            ShardGroupTerm(&recv_sg);

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
        sg->conn_addr = *addr;
    }

    LOG_DEBUG("Initiating shardgroup(%s) connection to %s:%u", sg->id, addr->host, addr->port);
    sg->update_in_progress = true;
    ConnConnect(conn, addr, sendShardGroupRequest);

    /* Disable use_conn_addr, as by default we'll try the next address on a
     * reconnect. It will be reset to true if the connection was successful, or
     * if conn_addr was populated by a -MOVED reply.
     */
    sg->use_conn_addr = false;
}

/* Called periodically by the main loop when sharding is enabled.
 *
 * Currently we use this to iterate all shardgroups and trigger an
 * update for shardgroups that have not been updated recently.
 */
void ShardingPeriodicCall(RedisRaftCtx *rr)
{
    /* See if we have any shardgroups that need a refresh.
     */

    if (!raft_is_leader(rr->raft)) {
        return;
    }

    long long mstime = RedisModule_Milliseconds();

    ShardingInfo *si = rr->sharding_info;
    if (si->shard_group_map != NULL) {
        size_t key_len;
        ShardGroup *sg;

        RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(si->shard_group_map, "^", NULL, 0);
        while (RedisModule_DictNextC(iter, &key_len, (void **) &sg) != NULL) {
            if (!sg->nodes_num || !sg->conn || mstime - sg->last_updated < rr->config->shardgroup_update_interval ||
                !ConnIsConnected(sg->conn) || sg->update_in_progress) {
                continue;
            }

            sendShardGroupRequest(sg->conn);
        }

        RedisModule_DictIteratorStop(iter);
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
    RedisModule_SaveUnsigned(rdb, si->shard_groups_num);
    if (si->shard_group_map != NULL) {
        RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(si->shard_group_map, "^", NULL, 0);

        size_t key_len;
        ShardGroup *sg;

        while (RedisModule_DictNextC(iter, &key_len, (void **) &sg) != NULL) {
            RedisModule_SaveStringBuffer(rdb, sg->id, strlen(sg->id));
            RedisModule_SaveUnsigned(rdb, sg->slot_ranges_num);
            for (int j = 0; j < sg->slot_ranges_num; j++) {
                ShardGroupSlotRange  *r = &sg->slot_ranges[j];
                RedisModule_SaveUnsigned(rdb, r->start_slot);
                RedisModule_SaveUnsigned(rdb, r->end_slot);
                RedisModule_SaveUnsigned(rdb, r->type);
            }
            RedisModule_SaveUnsigned(rdb, sg->nodes_num);
            for (int j = 0; j < sg->nodes_num; j++) {
                ShardGroupNode *n = &sg->nodes[j];
                RedisModule_SaveStringBuffer(rdb, n->node_id, strlen(n->node_id));
                RedisModule_SaveStringBuffer(rdb, n->addr.host, strlen(n->addr.host));
                RedisModule_SaveUnsigned(rdb, n->addr.port);
            }

        }
        RedisModule_DictIteratorStop(iter);
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

        size_t len;
        char *buf = RedisModule_LoadStringBuffer(rdb, &len);
        RedisModule_Assert(len < sizeof(sg.id));
        memcpy(sg.id, buf, len);
        sg.id[len] = '\0';
        RedisModule_Free(buf);

        /* Load Slot Range */
        sg.slot_ranges_num = RedisModule_LoadUnsigned(rdb);
        sg.slot_ranges = RedisModule_Calloc(sg.slot_ranges_num, sizeof(ShardGroupSlotRange));
        for (int j = 0; j < sg.slot_ranges_num; j++) {
            ShardGroupSlotRange  *r = &sg.slot_ranges[j];
            r->start_slot = RedisModule_LoadUnsigned(rdb);
            r->end_slot = RedisModule_LoadUnsigned(rdb);
            r->type = RedisModule_LoadUnsigned(rdb);
        }

        /* Load nodes */
        sg.nodes_num = RedisModule_LoadUnsigned(rdb);
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

        ShardGroupTerm(&sg);
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
    for (int h = 0; h < new_sg->slot_ranges_num; h++) {
        if (!HashSlotRangeValid(new_sg->slot_ranges[h].start_slot, new_sg->slot_ranges[h].end_slot)) {
            LOG_ERROR("Invalid shardgroup: bad slots range %u-%u",
                      new_sg->slot_ranges[h].start_slot, new_sg->slot_ranges[h].end_slot);
            return RR_ERROR;
        }

        for (int i = new_sg->slot_ranges[h].start_slot; i <= new_sg->slot_ranges[h].end_slot; i++) {
            if (si->stable_slots_map[i] != 0) {
                LOG_ERROR("Invalid shardgroup: hash slot already mapped: %u", i);
                return RR_ERROR;
            }
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
    if (!memcmp(rr->snapshot_info.dbid, new_sg->id, sizeof(new_sg->id))) {
        /* TODO: when we can update slots, that is the only thing that will be updated here */
    } else {
        ShardGroup *sg = getShardGroupById(rr, new_sg->id);
        if (sg == NULL) {
            return RR_ERROR;
        }

        sg->nodes_num = new_sg->nodes_num;
        sg->nodes = RedisModule_Realloc(sg->nodes, sizeof(ShardGroupNode) * sg->nodes_num);
        memcpy(sg->nodes, new_sg->nodes, sizeof(ShardGroupNode) * sg->nodes_num);
    }

    return RR_OK;
}

ShardGroup *getShardGroupById(RedisRaftCtx *rr, char *id)
{
    ShardingInfo *si = rr->sharding_info;

    return RedisModule_DictGetC(si->shard_group_map, id, strlen(id), NULL);
}

/* Add a new ShardGroup to the active ShardingInfo. Validation is done according to
 * ShardingInfoValidateShardGroup() above.
 */

RRStatus ShardingInfoAddShardGroup(RedisRaftCtx *rr, ShardGroup *new_sg)
{
    ShardingInfo *si = rr->sharding_info;

    /* Validate first */
    if (ShardingInfoValidateShardGroup(rr, new_sg) != RR_OK) {
        return RR_ERROR;
    }

    if (strlen(new_sg->id) >= sizeof(new_sg->id)) {
        return RR_ERROR;
    }
    ShardGroup *sg = ShardGroupCreate();
    memcpy(sg->id, new_sg->id, RAFT_DBID_LEN);
    sg->id[RAFT_DBID_LEN] = '\0';

    if (RedisModule_DictSetC(si->shard_group_map, sg->id, strlen(sg->id), sg) != REDISMODULE_OK) {
        ShardGroupFree(sg);
        return RR_ERROR;
    }

    si->shard_groups_num++;

    sg->slot_ranges_num = new_sg->slot_ranges_num;
    if (sg->slot_ranges_num) {
        sg->slot_ranges = RedisModule_Calloc(sg->slot_ranges_num, sizeof(*sg->slot_ranges));
        memcpy(sg->slot_ranges, new_sg->slot_ranges, sizeof(*sg->slot_ranges) * new_sg->slot_ranges_num);
    }

    sg->next_redir = 0;
    sg->use_conn_addr = false;
    sg->node_conn_idx = 0;
    sg->conn = NULL;
    sg->nodes_num = new_sg->nodes_num;
    if (sg->nodes_num) {
        sg->nodes = RedisModule_Calloc(new_sg->nodes_num, sizeof(*sg->nodes));
        memcpy(sg->nodes, new_sg->nodes, sizeof(*sg->nodes) * new_sg->nodes_num);
    }

    /* Do slot mapping */
    for (int i = 0; i < new_sg->slot_ranges_num; i++) {
        for (unsigned int j = new_sg->slot_ranges[i].start_slot; j <= new_sg->slot_ranges[i].end_slot; j++) {
            switch (new_sg->slot_ranges[i].type) {
                case SLOTRANGE_TYPE_STABLE:
                    si->stable_slots_map[j] = sg;
                    break;
                case SLOTRANGE_TYPE_IMPORTING:
                    si->importing_slots_map[j] = sg;
                    break;
                case SLOTRANGE_TYPE_MIGRATING:
                    si->migrating_slots_map[j] = sg;
                    break;
                default:
                    PANIC("ShardingInfoAddShardGroup: unknown slot range type");
            }
        }
    }

    /* Create a connection object for syncing. We assume that if nodes_num is zero
     * this is the shardgroup entry for our local cluster so it can be skipped.
     * */
    /* TODO: perhaps should only be called if using built in sharding mechanism */
    if (!rr->config->external_sharding && sg->nodes_num > 0) {
        sg->conn = ConnCreate(rr, sg, establishShardGroupConn, NULL);
    }

    return RR_OK;
}

/* Parse a ShardGroup specification as passed directly to RAFT.SHARDGROUP ADD.
 * Shard group syntax is as follows:
 *
 *  [shardgroup id] [num_slots] [num_nodes] ([start slot] [end slot] [slot type])* ([node-uid node-addr:node-port])*
 *
 * If parsing errors are encountered, an error reply is generated on the supplied RedisModuleCtx,
 * and RR_ERROR is returned.
 */

RRStatus ShardGroupParse(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, ShardGroup *sg)
{
    const char *id;
    size_t len;
    long long num_slots, num_nodes;

    ShardGroupInit(sg);

    id = RedisModule_StringPtrLen(argv[0], &len);
    if (len > RAFT_DBID_LEN) {
        RedisModule_ReplyWithError(ctx, "ERR invalid shard group message (shard group id length)");
        goto error;
    }
    memcpy(sg->id, id, len);
    sg->id[RAFT_DBID_LEN] = '\0';

    if (RedisModule_StringToLongLong(argv[1], &num_slots) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx, "ERR invalid shard group message (num slots)");
        goto error;
    }
    if (RedisModule_StringToLongLong(argv[2], &num_nodes) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx, "ERR invalid shard group message (num nodes)");
        goto error;
    }

    /* Validate node arguments count is correct */
    if ((argc - 3) != (num_slots * 3) + (num_nodes * 2)) {
        RedisModule_WrongArity(ctx);
        goto error;
    }
    int argidx = 3; /* Next arg to consume */

    /* Parse slots */
    sg->slot_ranges_num = num_slots;
    sg->slot_ranges = RedisModule_Calloc(num_slots, sizeof(ShardGroupSlotRange));
    for (int i = 0; i < num_slots; i++) {
        long long start_slot, end_slot, type;
        if (RedisModule_StringToLongLong(argv[argidx++], &start_slot) != REDISMODULE_OK ||
            RedisModule_StringToLongLong(argv[argidx++], &end_slot) != REDISMODULE_OK ||
            RedisModule_StringToLongLong(argv[argidx++], &type) != REDISMODULE_OK ||
            !HashSlotRangeValid(start_slot, end_slot) ||
            !SlotRangeTypeValid(type)) {
            RedisModule_ReplyWithError(ctx, "ERR invalid shard group message (slot range)");
            goto error;
        }
        sg->slot_ranges[i].start_slot = start_slot;
        sg->slot_ranges[i].end_slot = end_slot;
        sg->slot_ranges[i].type = type;
    }

    /* Parse nodes */
    sg->nodes_num = num_nodes;
    sg->nodes = RedisModule_Calloc(num_nodes, sizeof(ShardGroupNode));
    for (int i = 0; i < num_nodes; i++) {
        size_t len;
        const char *str = RedisModule_StringPtrLen(argv[argidx++], &len);

        if (len != RAFT_SHARDGROUP_NODEID_LEN) {
            RedisModule_ReplyWithError(ctx, "ERR invalid shard group message (node id length)");
            goto error;
        }

        memcpy(sg->nodes[i].node_id, str, len);
        sg->nodes[i].node_id[len] = '\0';

        str = RedisModule_StringPtrLen(argv[argidx++], &len);
        if (!NodeAddrParse(str, len, &sg->nodes[i].addr)) {
            RedisModule_ReplyWithError(ctx, "ERR invalid shard group message (node address/port)");
            goto error;
        }
    }

    return RR_OK;

error:
    ShardGroupTerm(sg);
    return RR_ERROR;
}

/* parses all the shardgroups in a replace call and validates them */
RRStatus ShardGroupsParse(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, RaftReq *req)
{
    if (RedisModule_StringToLongLong(argv[0], &req->r.shardgroups_replace.len) != REDISMODULE_OK) {
        printf("ERR invalid shard group message(1)");
        RedisModule_ReplyWithError(ctx, "ERR invalid shard group message(1)");
        return RR_ERROR;
    }
    req->r.shardgroups_replace.shardgroups =
            RedisModule_Calloc(req->r.shardgroups_replace.len, sizeof(ShardGroup *));
    argv++;
    argc--;

    int i;
    for(i = 0; i < req->r.shardgroups_replace.len; i++) {
        ShardGroup *sg = ShardGroupCreate();
        req->r.shardgroups_replace.shardgroups[i] = sg;

        long long num_slots, num_nodes;
        if (RedisModule_StringToLongLong(argv[1], &num_slots) != REDISMODULE_OK ) {
            printf("ERR invalid shard group message (num slots)");
            RedisModule_ReplyWithError(ctx, "ERR invalid shard group message (num slots)");
            goto fail;
        }
        if (RedisModule_StringToLongLong(argv[2], &num_nodes) != REDISMODULE_OK) {
            printf("ERR invalid shard group message (num nodes)");
            RedisModule_ReplyWithError(ctx, "ERR invalid shard group message (num nodes)");
            goto fail;
        }

        int num_argv_entries = (int) (3 + num_slots * 3 + num_nodes * 2);
        if (num_argv_entries > argc) {
            printf("ERR invalid shard group message (num entries)");
            RedisModule_ReplyWithError(ctx, "ERR invalid shard group message (num entries)");
            goto fail;
        }

        if (ShardGroupParse(ctx, argv, num_argv_entries, sg) != RR_OK) {
            printf("ShardGroupParse failed!\n");
            goto fail;
        }

        argv += num_argv_entries;
        argc -= num_argv_entries;
    }

    /* basic validation, no shard groups have slotranges that overlap another */
    ShardGroup * stable[REDIS_RAFT_HASH_MAX_SLOT];
    memset(stable, 0, sizeof(ShardGroup*)*REDIS_RAFT_HASH_MAX_SLOT);
    ShardGroup * importing[REDIS_RAFT_HASH_MAX_SLOT];
    memset(importing, 0, sizeof(ShardGroup *)*REDIS_RAFT_HASH_MAX_SLOT);
    ShardGroup * migrating[REDIS_RAFT_HASH_MAX_SLOT];
    memset(migrating, 0, sizeof(ShardGroup *)*REDIS_RAFT_HASH_MAX_SLOT);

    for (int j = 0; j < req->r.shardgroups_replace.len; j++) {
        ShardGroup * sg = req->r.shardgroups_replace.shardgroups[j];
        for (int k = 0; k < sg->slot_ranges_num; k++) {
            ShardGroupSlotRange * sr = &sg->slot_ranges[k];
            switch (sr->type) {
                case SLOTRANGE_TYPE_STABLE:
                    for(unsigned int l=sr->start_slot; l <= sr->end_slot; l++) {
                        if (stable[l]) {
                            RedisModule_ReplyWithError(ctx, "ERR Unable to validate shard groups: shard group already owns this slot");
                            goto fail;
                        } else if (importing[l] || migrating[l]) {
                            RedisModule_ReplyWithError(ctx, "ERR Unable to validate shard groups: slot marked for resharding");
                            goto fail;
                        }
                        stable[l] = sg;
                    }
                    break;
                case SLOTRANGE_TYPE_IMPORTING:
                    for(unsigned int l=sr->start_slot; l <= sr->end_slot; l++) {
                        if (stable[l]) {
                            RedisModule_ReplyWithError(ctx, "ERR Unable to validate shard groups: shard group already owns this slot");
                            goto fail;
                        } else if (importing[l]) {
                            RedisModule_ReplyWithError(ctx, "ERR Unable to validate shard groups: shard group already importing this slot");
                            goto fail;
                        }
                        importing[l] = sg;
                    }
                    break;
                case SLOTRANGE_TYPE_MIGRATING:
                    for(unsigned int l=sr->start_slot; l <= sr->end_slot; l++) {
                        if (stable[l]) {
                            RedisModule_ReplyWithError(ctx, "ERR Unable to validate shard groups: shard group already owns this slot");
                            goto fail;
                        } else if (migrating[l]) {
                            RedisModule_ReplyWithError(ctx, "ERR Unable to validate shard groups: shard group already migrating this slot");
                            goto fail;
                        }
                        migrating[l] = sg;
                    }
                    break;
                default:
                    RedisModule_ReplyWithError(ctx, "ERR Unable to validate shard groups: unknown shard group type");
                    goto fail;
            }
        }
    }

    return RR_OK;

fail:
    for (; i >= 0; i--) {
        ShardGroupFree(req->r.shardgroups_replace.shardgroups[i]);
    }

    RedisModule_Free(req->r.shardgroups_replace.shardgroups);
    req->r.shardgroups_replace.shardgroups = NULL;
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

    if (si->shard_group_map != NULL) {
        RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(si->shard_group_map, "^", NULL, 0);

        size_t key_len;
        void *data;

        while (RedisModule_DictNextC(iter, &key_len, &data) != NULL) {
            ShardGroupFree(data);
        }
        RedisModule_DictIteratorStop(iter);
        RedisModule_FreeDict(rr->ctx, si->shard_group_map);
        si->shard_group_map = NULL;
    }

    si->shard_group_map = RedisModule_CreateDict(rr->ctx);

    si->shard_groups_num = 0;

    /* Reset array */
    for (int i = 0; i < REDIS_RAFT_HASH_SLOTS; i++) {
        si->stable_slots_map[i] = NULL;
        si->importing_slots_map[i] = NULL;
        si->migrating_slots_map[i] = NULL;
    }
}

/* Compute the hash slot for a RaftRedisCommandArray list of commands and update
 * the entry.
 */

RRStatus computeHashSlot(RedisRaftCtx *rr, RaftReq *req)
{
    int slot = -1;

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
                    RedisModule_ReplyWithError(req->ctx, "CROSSSLOT Keys in request don't hash to the same slot");
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

/* Returns a string representation of the hash slot range assigned to the
 * specified shardgroup.
 *
 * Currently, doesn't handle importing/migrating yet.
 */
RedisModuleString *generateSlots(RedisModuleCtx *ctx, ShardGroup *sg)
{
    RedisModuleString *ret;

    if (sg->slot_ranges[0].start_slot != sg->slot_ranges[0].end_slot) {
        ret = RedisModule_CreateStringPrintf(ctx, "%d-%d", sg->slot_ranges[0].start_slot, sg->slot_ranges[0].end_slot);
    } else {
        ret = RedisModule_CreateStringPrintf(ctx, "%d", sg->slot_ranges[0].start_slot);
    }

    for (int i = 1; i < sg->slot_ranges_num; i++) {
        RedisModuleString *tmp;
        const char *str;
        size_t len;

        if (sg->slot_ranges[i].start_slot != sg->slot_ranges[i].end_slot) {
            tmp = RedisModule_CreateStringPrintf(ctx, " %d-%d", sg->slot_ranges[i].start_slot, sg->slot_ranges[i].end_slot);
        } else {
            tmp = RedisModule_CreateStringPrintf(ctx, " %d", sg->slot_ranges[i].start_slot);
        }

        str = RedisModule_StringPtrLen(tmp, &len);
        RedisModule_StringAppendBuffer(ctx, ret, str, len);
        RedisModule_FreeString(ctx, tmp);
    }

    return ret;
}

/* Formats a CLUSTER NODES line and appends it to ret */
static void appendClusterNodeString(RedisModuleString *ret, char node_id[41], NodeAddr *addr, const char *flags,
                                    const char *master, int ping_sent, int pong_recv, raft_term_t epoch, const char *link_state,
                                    RedisModuleString *slots)
{
    size_t len;
    const char *temp;
    size_t slots_len;
    const char *slots_str;

    slots_str = RedisModule_StringPtrLen(slots, &slots_len);
    RedisModuleString* str = RedisModule_CreateStringPrintf(NULL,
                                                           "%s %s:%d@%d %s %s %d %d %ld %s %.*s\r\n",
                                                           node_id,
                                                           addr->host,
                                                           addr->port,
                                                           addr->port,
                                                           flags,
                                                           master,
                                                           ping_sent,
                                                           pong_recv,
                                                           epoch,
                                                           link_state,
                                                           (int) slots_len, slots_str);

    temp = RedisModule_StringPtrLen(str, &len);
    RedisModule_StringAppendBuffer(NULL, ret, temp, len);

    RedisModule_FreeString(NULL, str);
}

/* Formats a CLUSTER NODES line from a raft node structure and appends it to ret. */
static void addClusterNodeReplyFromNode(RedisRaftCtx *rr,
                                        RedisModuleString *ret,
                                        raft_node_t *raft_node,
                                        RedisModuleString *slots)
{
    Node *node = raft_node_get_udata(raft_node);
    NodeAddr *addr;

    int leader = (raft_node_get_id(raft_node) == raft_get_leader_id(rr->raft));
    int self = (raft_node_get_id(raft_node) == raft_get_nodeid(rr->raft));

    /* Stale nodes should not exist but we prefer to be defensive.
     * Our own node doesn't have a connection so we don't expect a Node object.
     */
    if (node) {
        addr = &node->addr;
    } else if (raft_get_my_node(rr->raft) == raft_node) {
        addr = &rr->config->addr;
    } else {
        return;
    }

    /* should we record heartbeat and reply times for ping/pong */
    char *flags = self ? "myself" : "noflags";
    char *master = leader ? "master" : "slave";
    int ping_sent = 0;
    int pong_recv = 0;

    char node_id[RAFT_SHARDGROUP_NODEID_LEN+1];
    snprintf(node_id, sizeof(node_id), "%.32s%08x", rr->log->dbid, raft_node_get_id(raft_node));

    raft_term_t epoch = raft_get_current_term(redis_raft.raft);
    char *link_state = "connected";

    appendClusterNodeString(ret, node_id, addr, flags, master, ping_sent, pong_recv, epoch, link_state, slots);
}

/* Produce a CLUSTER NODES compatible reply, including:
 *
 * 1. Local cluster's slot range and nodes
 * 2. All configured shardgroups with their slot ranges and nodes.
 */

static void addClusterNodesReply(RedisRaftCtx *rr, RaftReq *req)
{
    raft_node_t *leader_node = getLeaderNodeOrReply(rr, req);
    if (!leader_node) {
        return;
    }
    ShardingInfo *si = rr->sharding_info;

    RedisModuleString *ret = RedisModule_CreateString(req->ctx, "", 0);

    if (si->shard_group_map != NULL) {
        size_t key_len;
        ShardGroup *sg;

        RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(si->shard_group_map, "^", NULL, 0);
        while (RedisModule_DictNextC(iter, &key_len, (void **) &sg) != NULL) {
            RedisModuleString *slots = generateSlots(req->ctx, sg);

            if (*sg->id == 0) {
                for (int j = 0; j < raft_get_num_nodes(rr->raft); j++) {
                    raft_node_t *raft_node = raft_get_node_from_idx(rr->raft, j);
                    if (!raft_node_is_active(raft_node)) {
                        continue;
                    }
                    addClusterNodeReplyFromNode(rr, ret, raft_node, slots);
                }
            } else {
                for (int j = 0; j < sg->nodes_num; j++) {
                    char *flags = "noflags";
                    /* SHARDGROUP GET only works on leader
                     * SHARDGROUP GET lists nodes in order of idx, but 0 will always be self, i.e. leader
                     */
                    char *master = j == 0 ? "master" : "slave";
                    int ping_sent = 0;
                    int pong_recv = 0;
                    int epoch = 0;
                    char *link_state = "connected";

                    appendClusterNodeString(ret, sg->nodes[j].node_id, &sg->nodes[j].addr, flags, master, ping_sent,
                                            pong_recv, epoch, link_state, slots);
                }
            }
            RedisModule_FreeString(req->ctx, slots);
        }

        RedisModule_DictIteratorStop(iter);
    }

    RedisModule_ReplyWithString(req->ctx, ret);
    RedisModule_FreeString(req->ctx, ret);
}

/* Produce a CLUSTER SLOTS compatible reply, including:
 *
 * 1. Local cluster's slot range and nodes.
 * 2. All configured shardgroups with their slot ranges and nodes.
 */

static void addClusterSlotsReply(RedisRaftCtx *rr, RaftReq *req)
{
    raft_node_t *leader_node = getLeaderNodeOrReply(rr, req);
    if (!leader_node) {
        return;
    }

    ShardingInfo *si = rr->sharding_info;
    if (!si->shard_group_map) {
        RedisModule_ReplyWithArray(req->ctx, 0);
        return;
    }

    /* Count slots and initiate array reply */
    unsigned int num_slots = 0;
    size_t key_len;
    ShardGroup *sg;

    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(si->shard_group_map, "^", NULL, 0);
    while (RedisModule_DictNextC(iter, &key_len, (void **) &sg) != NULL) {
        num_slots += sg->slot_ranges_num;
    }
    RedisModule_DictIteratorStop(iter);
    RedisModule_ReplyWithArray(req->ctx, num_slots);

    /* Return array elements */
    iter = RedisModule_DictIteratorStartC(si->shard_group_map, "^", NULL, 0);
    while (RedisModule_DictNextC(iter, &key_len, (void **) &sg) != NULL) {
        for (int j = 0; j < sg->slot_ranges_num; j++) {
            RedisModule_ReplyWithArray(req->ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

            int slot_len = 0;

            RedisModule_ReplyWithLongLong(req->ctx, sg->slot_ranges[j].start_slot); /* Start slot */
            RedisModule_ReplyWithLongLong(req->ctx, sg->slot_ranges[j].end_slot);   /* End slot */
            slot_len += 2;

            /* Dump Raft nodes now. Leader (master) first, followed by others */
            if (*sg->id == 0) {
                /* Local cluster's ShardGroup: we list the leader node first,
                 * followed by all cluster nodes we know. This information does not
                 * come from the ShardGroup.
                 */

                slot_len += addClusterSlotNodeReply(rr, req->ctx, leader_node);
                for (int j = 0; j < raft_get_num_nodes(rr->raft); j++) {
                    raft_node_t *raft_node = raft_get_node_from_idx(rr->raft, j);
                    if (raft_node_get_id(raft_node) == raft_get_leader_id(rr->raft) ||
                        !raft_node_is_active(raft_node)) {
                        continue;
                    }

                    slot_len += addClusterSlotNodeReply(rr, req->ctx, raft_node);
                }
            } else {
                /* Remote cluster: we simply dump what the ShardGroup configuration
                 * tells us.
                 */

                for (int j = 0; j < sg->nodes_num; j++) {
                    slot_len += addClusterSlotShardGroupNodeReply(rr, req->ctx, &sg->nodes[j]);
                }
            }

            RedisModule_ReplySetArrayLength(req->ctx, slot_len);
        }
    }

    RedisModule_DictIteratorStop(iter);
}

/* Process CLUSTER commands, as intercepted earlier by the Raft module.
 *
 * Currently only supports:
 *   - SLOTS.
 *   - NODES.
 */
void handleClusterCommand(RedisRaftCtx *rr, RaftReq *req)
{
    RaftRedisCommand *cmd = req->r.redis.cmds.commands[0];

    if (cmd->argc < 2) {
        /* Note: we can't use RM_WrongArity here because our req->ctx is a thread-safe context
         * with a synthetic client that no longer has the original argv.
         */
        RedisModule_ReplyWithError(req->ctx, "ERR wrong number of arguments for 'cluster' command");
        goto exit;
    }

    size_t cmd_len;
    const char *cmd_str = RedisModule_StringPtrLen(cmd->argv[1], &cmd_len);

    if (cmd_len == 5 && !strncasecmp(cmd_str, "SLOTS", 5) && cmd->argc == 2) {
        addClusterSlotsReply(rr, req);
        goto exit;
    } else if (cmd_len == 5 && !strncasecmp(cmd_str, "NODES", 5) && cmd->argc == 2) {
        addClusterNodesReply(rr, req);
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
    JoinLinkState *state = ConnGetPrivateData(conn);

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
                state->failed = true;
            } else {
                LOG_VERBOSE("Shardgroup link: %s:%u: received configuration, propagating to Raft log.",
                            state->addr_iter->addr.host, state->addr_iter->addr.port);
                if (ShardGroupAppendLogEntry(ConnGetRedisRaftCtx(conn), &recv_sg,
                                         RAFT_LOGTYPE_ADD_SHARDGROUP, state->req) == RR_OK) {
                    state->req = NULL;

                    ConnAsyncTerminate(conn);
                    ShardGroupTerm(&recv_sg);
                    return;
                }
            }
            ShardGroupTerm(&recv_sg);
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

    JoinLinkState *state = ConnGetPrivateData(conn);
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

/* Handle a RAFT.SHARDGROUP LINK request.
 */
void handleShardGroupLink(RedisRaftCtx *rr, RaftReq *req)
{
    const char * type = "link";

    /* Must be done on a leader */
    if (checkRaftState(rr, req) == RR_ERROR ||
        checkLeader(rr, req, NULL) == RR_ERROR) {
        goto exit_fail;
    }

    LOG_INFO("Attempting to link shardgroup %s:%u",
             req->r.shardgroup_link.addr.host,
             req->r.shardgroup_link.addr.port);

    JoinLinkState *state = RedisModule_Calloc(1, sizeof(*state));
    state->type = RedisModule_Calloc(1, strlen(type)+1);
    strcpy(state->type, type);
    state->connect_callback = linkSendRequest;
    time(&(state->start));
    NodeAddrListAddElement(&state->addr, &req->r.shardgroup_link.addr);
    state->req = req;

    state->conn = ConnCreate(rr, state, joinLinkIdleCallback, joinLinkFreeCallback);

    return;
exit_fail:
    RaftReqFree(req);

}
