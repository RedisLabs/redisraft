/*
 * Copyright Redis Ltd. 2020 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "log.h"
#include "redisraft.h"

#include <stdlib.h>
#include <string.h>
#include <strings.h>

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

    /* shard group id */
    buf = catsnprintf(buf, &buf_size, "%s\n", sg->id);

    /* number of slot ranges and nodes */
    buf = catsnprintf(buf, &buf_size, "%u\n%u\n", sg->slot_ranges_num, sg->nodes_num);

    for (unsigned int i = 0; i < sg->slot_ranges_num; i++) {
        /* individual slot ranges */
        ShardGroupSlotRange *sr = &sg->slot_ranges[i];
        buf = catsnprintf(buf, &buf_size, "%u\n%u\n%u\n%llu\n", sr->start_slot, sr->end_slot, sr->type, sr->migration_session_key);
    }

    for (unsigned int i = 0; i < sg->nodes_num; i++) {
        /* individual nodes */
        NodeAddr *addr = &sg->nodes[i].addr;
        buf = catsnprintf(buf, &buf_size, "%s\n%s:%d\n", sg->nodes[i].node_id, addr->host, addr->port);
    }

    return buf;
}

/* Deserialize a ShardGroup from the specified buffer. The target ShardGroup is assumed
 * to be uninitialized, and the nodes array will be allocated on demand.
 */
ShardGroup *ShardGroupDeserialize(const char *buf, size_t buf_len)
{
    ShardGroup *sg = ShardGroupCreate();

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
    s = nl + 1;

    /* Read in slot ranges */
    sg->slot_ranges = RedisModule_Calloc(sg->slot_ranges_num, sizeof(ShardGroupSlotRange));
    for (unsigned int i = 0; i < sg->slot_ranges_num; i++) {
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

        nl = memchr(s, '\n', end - s);
        if (!nl) {
            goto error;
        }

        r->migration_session_key = strtoull(s, &endptr, 10);
        RedisModule_Assert(endptr == nl);
        s = nl + 1;
    }

    /* read in shard group nodes */
    sg->nodes = RedisModule_Calloc(sg->nodes_num, sizeof(ShardGroupNode));
    for (unsigned int i = 0; i < sg->nodes_num; i++) {
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
        if (!nl || !NodeAddrParse(s, nl - s, &n->addr)) {
            goto error;
        }

        s = nl + 1;
    }

    return sg;

error:
    ShardGroupFree(sg);
    return NULL;
}

/* Initialize a (previously allocated) shardgroup structure.
 * Basically just zero-initializing everything, but a place holder
 * for the future.
 */
void ShardGroupInit(ShardGroup *sg)
{
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

ShardGroup *ShardGroupCreate()
{
    ShardGroup *sg = RedisModule_Calloc(1, sizeof(*sg));
    ShardGroupInit(sg);
    return sg;
}

/* Unlike C heap function, this should only be passed non NULL (valid) pointers */
void ShardGroupFree(ShardGroup *sg)
{
    RedisModule_Assert(sg != NULL);

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
static int compareShardGroups(ShardGroup *a, ShardGroup *b)
{
    if (a->nodes_num != b->nodes_num) {
        return a->nodes_num - b->nodes_num;
    }

    for (unsigned int i = 0; i < a->nodes_num; i++) {
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
        reply->element[1]->type != REDIS_REPLY_ARRAY ||  /* slots array */
        reply->element[2]->type != REDIS_REPLY_ARRAY) {  /* nodes array */
        return RR_ERROR;
    }

    strncpy(sg->id, reply->element[0]->str, RAFT_DBID_LEN);
    sg->id[RAFT_DBID_LEN] = '\0';
    sg->slot_ranges_num = reply->element[1]->elements;
    sg->nodes_num = reply->element[2]->elements;

    sg->slot_ranges = RedisModule_Calloc(sg->slot_ranges_num, sizeof(ShardGroupSlotRange));
    for (unsigned int i = 0; i < sg->slot_ranges_num; i++) {
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
    for (unsigned int i = 0; i < sg->nodes_num; i++) {
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
    int e = raft_recv_entry(rr->raft, entry, NULL);
    raft_entry_release(entry);

    if (e != 0) {
        LOG_WARNING("Failed to append shardgroup entry, error %d", e);
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
    int e = raft_recv_entry(rr->raft, entry, NULL);
    raft_entry_release(entry);

    if (e != 0) {
        LOG_WARNING("Failed to append shardgroups entry, error %d", e);
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
        LOG_WARNING("RAFT.SHARDGROUP GET failed: connection dropped.");
    } else if (reply->type == REDIS_REPLY_ERROR) {
        /* -MOVED? */
        if (strlen(reply->str) > 6 && !strncmp(reply->str, "MOVED ", 6)) {
            if (!parseMovedReply(reply->str, &sg->conn_addr)) {
                LOG_WARNING("RAFT.SHARDGROUP GET failed: invalid MOVED response: %s", reply->str);
            } else {
                LOG_VERBOSE("RAFT.SHARDGROUP GET redirected to leader: %s:%d",
                            sg->conn_addr.host, sg->conn_addr.port);
                sg->use_conn_addr = true;
            }
        } else {
            LOG_WARNING("RAFT.SHARDGROUP GET failed: %s", reply->str);
        }
    } else {
        ShardGroup recv_sg;
        ShardGroupInit(&recv_sg);

        if (parseShardGroupReply(reply, &recv_sg) == RR_ERROR) {
            LOG_WARNING("RAFT.SHARDGROUP GET invalid reply.");
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
        RedisModule_Milliseconds() - sg->last_updated < rr->config.shardgroup_update_interval) {
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
            if (!sg->nodes_num || !sg->conn || mstime - sg->last_updated < rr->config.shardgroup_update_interval ||
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

    RedisModule_SaveUnsigned(rdb, si->shard_groups_num);
    if (si->shard_group_map != NULL) {
        RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(si->shard_group_map, "^", NULL, 0);

        size_t key_len;
        ShardGroup *sg;

        while (RedisModule_DictNextC(iter, &key_len, (void **) &sg) != NULL) {
            char *buf = ShardGroupSerialize(sg);
            RedisModule_SaveStringBuffer(rdb, buf, strlen(buf));
            RedisModule_Free(buf);
        }
        RedisModule_DictIteratorStop(iter);
    }

    for (int i = 0; i < REDIS_RAFT_HASH_SLOTS; i++) {
        RedisModule_SaveUnsigned(rdb, si->max_importing_term[i]);
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
    ShardingInfoReset(rr->ctx, si);

    /* Load individual shard groups */
    for (unsigned int i = 0; i < rdb_shard_groups_num; i++) {
        size_t len;
        char *buf = RedisModule_LoadStringBuffer(rdb, &len);
        ShardGroup *sg = ShardGroupDeserialize(buf, len);
        RedisModule_Free(buf);
        RedisModule_Assert(sg != NULL);

        /* set local flag */
        if (!strncmp(sg->id, rr->meta.dbid, RAFT_DBID_LEN)) {
            sg->local = true;
        }

        /* This also handles all validation so serious violations, although it
         * should never exist, will be caught.
         */
        RRStatus ret = ShardingInfoAddShardGroup(rr, sg);
        RedisModule_Assert(ret == RR_OK);
    }

    for (int i = 0; i < REDIS_RAFT_HASH_SLOTS; i++) {
        si->max_importing_term[i] = (raft_term_t) RedisModule_LoadUnsigned(rdb);
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
    for (unsigned int h = 0; h < new_sg->slot_ranges_num; h++) {
        if (!HashSlotRangeValid(new_sg->slot_ranges[h].start_slot, new_sg->slot_ranges[h].end_slot)) {
            LOG_WARNING("Invalid shardgroup: bad slots range %u-%u",
                        new_sg->slot_ranges[h].start_slot, new_sg->slot_ranges[h].end_slot);
            return RR_ERROR;
        }

        for (unsigned int i = new_sg->slot_ranges[h].start_slot; i <= new_sg->slot_ranges[h].end_slot; i++) {
            if (si->stable_slots_map[i] != NULL) {
                LOG_WARNING("Invalid shardgroup: hash slot already mapped as stable: %u", i);
                return RR_ERROR;
            }
            if (new_sg->slot_ranges[h].type == SLOTRANGE_TYPE_MIGRATING) {
                if (si->migrating_slots_map[i] != NULL) {
                    LOG_WARNING("Invalid shardgroup: hash slot already mapped as migrating: %u", i);
                    return RR_ERROR;
                }
            } else if (new_sg->slot_ranges[h].type == SLOTRANGE_TYPE_IMPORTING) {
                if (si->importing_slots_map[i] != NULL) {
                    LOG_WARNING("Invalid shardgroup: hash slot already mapped as importing: %u", i);
                    return RR_ERROR;
                }
            }
        }
    }

    return RR_OK;
}

ShardGroup *GetShardGroupById(RedisRaftCtx *rr, const char *id)
{
    ShardingInfo *si = rr->sharding_info;

    return RedisModule_DictGetC(si->shard_group_map, (void *) id, strlen(id), NULL);
}

/* Update an existing ShardGroup in the active ShardingInfo.
 *
 * FIXME: We currently only handle updating nodes but don't support remapping
 *        hash slots, via this mechanism.
 */

RRStatus ShardingInfoUpdateShardGroup(RedisRaftCtx *rr, ShardGroup *new_sg)
{
    RRStatus ret = RR_ERROR;

    if (!memcmp(rr->snapshot_info.dbid, new_sg->id, sizeof(new_sg->id))) {
        /* TODO: when we can update slots, that is the only thing that will be updated here */
    } else {
        ShardGroup *sg = GetShardGroupById(rr, new_sg->id);
        if (sg == NULL) {
            goto out;
        }

        sg->nodes_num = new_sg->nodes_num;
        sg->nodes = RedisModule_Realloc(sg->nodes, sizeof(ShardGroupNode) * sg->nodes_num);
        memcpy(sg->nodes, new_sg->nodes, sizeof(ShardGroupNode) * sg->nodes_num);
    }

    ret = RR_OK;

out:
    ShardGroupFree(new_sg);
    return ret;
}

/* Add a new ShardGroup to the active ShardingInfo. Validation is done according to
 * ShardingInfoValidateShardGroup() above.
 */

RRStatus ShardingInfoAddShardGroup(RedisRaftCtx *rr, ShardGroup *sg)
{
    ShardingInfo *si = rr->sharding_info;

    /* Validate first */
    if (ShardingInfoValidateShardGroup(rr, sg) != RR_OK) {
        goto fail;
    }

    if (strlen(sg->id) >= sizeof(sg->id)) {
        goto fail;
    }

    if (RedisModule_DictSetC(si->shard_group_map, sg->id, strlen(sg->id), sg) != REDISMODULE_OK) {
        goto fail;
    }

    si->shard_groups_num++;
    sg->next_redir = 0;
    sg->use_conn_addr = false;
    sg->node_conn_idx = 0;
    sg->conn = NULL;

    /* Do slot mapping */
    for (unsigned int i = 0; i < sg->slot_ranges_num; i++) {
        for (unsigned int j = sg->slot_ranges[i].start_slot; j <= sg->slot_ranges[i].end_slot; j++) {
            switch (sg->slot_ranges[i].type) {
                /* we only reset max_importing_term when we aren't in an importing state (i.e. stable/migrating).
                 * this is because when importing, we might have previously been importing and want to keep the same
                 * value
                 */
                case SLOTRANGE_TYPE_STABLE:
                    if (sg->local) {
                        si->max_importing_term[j] = 0;
                    }
                    si->stable_slots_map[j] = sg;
                    break;
                case SLOTRANGE_TYPE_IMPORTING:
                    si->importing_slots_map[j] = sg;
                    break;
                case SLOTRANGE_TYPE_MIGRATING:
                    if (sg->local) {
                        si->max_importing_term[j] = 0;
                    }
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
    if (!rr->config.external_sharding && sg->nodes_num > 0) {
        sg->conn = ConnCreate(rr, sg, establishShardGroupConn, NULL,
                              rr->config.cluster_user,
                              rr->config.cluster_password);
    }

    if (!si->is_sharding) {
        if (si->shard_groups_num != 1) {
            si->is_sharding = true;
        } else {
            size_t key_len;
            ShardGroup *s;

            RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(si->shard_group_map, "^", NULL, 0);
            RedisModule_DictNextC(iter, &key_len, (void **) &s);
            RedisModule_DictIteratorStop(iter);

            if (!s->local || s->slot_ranges_num != 1 ||
                s->slot_ranges[0].start_slot != REDIS_RAFT_HASH_MIN_SLOT ||
                s->slot_ranges[0].end_slot != REDIS_RAFT_HASH_MAX_SLOT ||
                s->slot_ranges[0].type != SLOTRANGE_TYPE_STABLE) {
                si->is_sharding = true;
            }
        }
    }

    return RR_OK;

fail:
    ShardGroupFree(sg);
    return RR_ERROR;
}

/* Parse a ShardGroup specification as passed directly to RAFT.SHARDGROUP ADD.
 * Shard group argv syntax is as follows:
 *
 *  [shardgroup id] [num_slots] [num_nodes] ([start slot] [end slot] [slot type] [migration session key])* ([node-uid] [node-addr:node-port])*
 *
 * If parsing errors are encountered, an error reply is generated on the supplied RedisModuleCtx and NULL is returned
 * If it was processed successfully, a pointer to an allocated ShardGroup is returned and the number of consumed
 * argv elements is set to the integer pointed to by num_argv_entries
 */

ShardGroup *ShardGroupParse(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, int base_argv_idx, int *num_argv_entries)
{
    const char *id;
    size_t len;
    long long num_slots, num_nodes;
    int argidx = 0;

    ShardGroup *sg = ShardGroupCreate();

    id = RedisModule_StringPtrLen(argv[0], &len);
    if (len != RAFT_DBID_LEN) {
        LOG_WARNING("ShardGroupParse failed; argv[%d] shard_group id is wrong (%ld instead of %d)", base_argv_idx + argidx, len, RAFT_DBID_LEN);
        RedisModule_ReplyWithError(ctx, "ERR invalid shard group message (shard group id length)");
        goto error;
    }
    argidx++;

    memcpy(sg->id, id, len);
    sg->id[RAFT_DBID_LEN] = '\0';

    if (RedisModule_StringToLongLong(argv[1], &num_slots) != REDISMODULE_OK) {
        LOG_WARNING("ShardGroupParse failed; argv[%d]: couldn't parse num_slots", base_argv_idx + argidx);
        RedisModule_ReplyWithError(ctx, "ERR invalid shard group message (num slots)");
        goto error;
    }
    argidx++;

    if (RedisModule_StringToLongLong(argv[2], &num_nodes) != REDISMODULE_OK) {
        LOG_WARNING("ShardGroupParse failed; argv[%d]: couldn't parse num_nodes", base_argv_idx + argidx);
        RedisModule_ReplyWithError(ctx, "ERR invalid shard group message (num nodes)");
        goto error;
    }
    argidx++;

    /* Validate node arguments count is correct */
    *num_argv_entries = (int) (3 + num_slots * 4 + num_nodes * 2);
    if (*num_argv_entries > argc) {
        LOG_WARNING("ShardGroupParse failed; not enough args to parse: want %d have %d", *num_argv_entries, argc);
        RedisModule_WrongArity(ctx);
        goto error;
    }

    /* Parse slots */
    sg->slot_ranges_num = num_slots;
    sg->slot_ranges = RedisModule_Calloc(num_slots, sizeof(ShardGroupSlotRange));
    for (int i = 0; i < num_slots; i++) {
        long long start_slot, end_slot, type, key;
        if (RedisModule_StringToLongLong(argv[argidx++], &start_slot) != REDISMODULE_OK ||
            RedisModule_StringToLongLong(argv[argidx++], &end_slot) != REDISMODULE_OK ||
            RedisModule_StringToLongLong(argv[argidx++], &type) != REDISMODULE_OK ||
            RedisModule_StringToLongLong(argv[argidx++], &key) != REDISMODULE_OK ||
            !HashSlotRangeValid((int) start_slot, (int) end_slot) ||
            !SlotRangeTypeValid(type) ||
            !MigrationSessionKeyValid(key)) {
            LOG_WARNING("ShardGroupParse failed; argv[%d]:argv[%d]: invalid parse slot_range", base_argv_idx + argidx - 3, base_argv_idx + argidx - 1);
            RedisModule_ReplyWithError(ctx, "ERR invalid shard group message (slot range)");
            goto error;
        }
        sg->slot_ranges[i].start_slot = start_slot;
        sg->slot_ranges[i].end_slot = end_slot;
        sg->slot_ranges[i].type = type;
        sg->slot_ranges[i].migration_session_key = key;
    }

    /* Parse nodes */
    sg->nodes_num = num_nodes;
    sg->nodes = RedisModule_Calloc(num_nodes, sizeof(ShardGroupNode));
    for (int i = 0; i < num_nodes; i++) {
        const char *str = RedisModule_StringPtrLen(argv[argidx], &len);

        if (len != RAFT_SHARDGROUP_NODEID_LEN) {
            LOG_WARNING("ShardGroupParse failed; argv[%d] node id length is wrong (%ld instead of %d)", base_argv_idx + argidx, len, RAFT_SHARDGROUP_NODEID_LEN);
            RedisModule_ReplyWithError(ctx, "ERR invalid shard group message (node id length)");
            goto error;
        }
        argidx++;

        memcpy(sg->nodes[i].node_id, str, len);
        sg->nodes[i].node_id[len] = '\0';

        str = RedisModule_StringPtrLen(argv[argidx], &len);
        if (!NodeAddrParse(str, len, &sg->nodes[i].addr)) {
            LOG_WARNING("ShardGroupParse failed; argv[%d] failed to parse node", base_argv_idx + argidx);
            RedisModule_ReplyWithError(ctx, "ERR invalid shard group message (node address/port)");
            goto error;
        }
        argidx++;
    }

    return sg;

error:
    ShardGroupFree(sg);
    return NULL;
}

/* parses all the shardgroups in a replace call and validates them */
/*
 * It is able to validate them at this point, because the replace call has a view of the entire world
 * and replaces the entire world.  Therefore, it's not dependent on the state of the ShardGroups at any
 * point in time.
 */
ShardGroup **ShardGroupsParse(RedisModuleCtx *ctx,
                              RedisModuleString **argv, int argc, int *len)
{
    int num_shards;
    long long val;
    ShardGroup **shards = NULL;

    *len = 0;

    if (RedisModule_StringToLongLong(argv[0], &val) != REDISMODULE_OK ||
        val < 0 || val >= REDIS_RAFT_HASH_SLOTS) {
        LOG_WARNING("Invalid shard group message: argv[0] (number of shard groups)");
        RedisModule_ReplyWithError(ctx, "ERR invalid shard group message(1)");
        return NULL;
    }

    num_shards = (int) val;
    shards = RedisModule_Calloc(num_shards, sizeof(*shards));

    argv++;
    argc--;
    int argv_idx = 1;

    unsigned int created_shard_count = 0;
    for (int i = 0; i < num_shards; i++) {
        ShardGroup *sg;

        int num_argv_entries;
        if ((sg = ShardGroupParse(ctx, argv, argc, argv_idx, &num_argv_entries)) == NULL) {
            goto fail;
        }

        shards[i] = sg;
        created_shard_count++;

        argv += num_argv_entries;
        argc -= num_argv_entries;
        argv_idx += num_argv_entries;
    }

    if (argc != 0) {
        LOG_WARNING("Invalid shard group message: extra unexpected arguments");
        RedisModule_ReplyWithError(ctx, "ERR invalid shard group message (extra arguments)");
        goto fail;
    }

    /* basic validation, no shard groups have slotranges that overlap another */
    ShardGroup *stable[REDIS_RAFT_HASH_MAX_SLOT + 1] = {0};
    ShardGroup *importing[REDIS_RAFT_HASH_MAX_SLOT + 1] = {0};
    ShardGroup *migrating[REDIS_RAFT_HASH_MAX_SLOT + 1] = {0};

    for (int j = 0; j < num_shards; j++) {
        ShardGroup *sg = shards[j];
        for (unsigned int k = 0; k < sg->slot_ranges_num; k++) {
            ShardGroupSlotRange *sr = &sg->slot_ranges[k];
            switch (sr->type) {
                case SLOTRANGE_TYPE_STABLE:
                    for (unsigned int l = sr->start_slot; l <= sr->end_slot; l++) {
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
                    for (unsigned int l = sr->start_slot; l <= sr->end_slot; l++) {
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
                    for (unsigned int l = sr->start_slot; l <= sr->end_slot; l++) {
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

    *len = num_shards;

    return shards;

fail:
    *len = 0;

    for (unsigned int j = 0; j < created_shard_count; j++) {
        ShardGroupFree(shards[j]);
    }

    RedisModule_Free(shards);
    return NULL;
}

/* Initialize ShardingInfo and add our local RedisRaft cluster as the first
 * ShardGroup.
 */

void ShardingInfoInit(RedisModuleCtx *ctx, ShardingInfo **si)
{
    *si = RedisModule_Calloc(1, sizeof(ShardingInfo));

    ShardingInfoReset(ctx, *si);
}

static void freeShardGroupMap(RedisModuleCtx *ctx, RedisModuleDict *shard_group_map)
{
    if (shard_group_map != NULL) {
        RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(shard_group_map, "^", NULL, 0);

        size_t key_len;
        void *data;

        while (RedisModule_DictNextC(iter, &key_len, &data) != NULL) {
            ShardGroupFree(data);
        }
        RedisModule_DictIteratorStop(iter);
        RedisModule_FreeDict(ctx, shard_group_map);
    }
}

/* Free the ShardingInfo structure. */
void ShardingInfoFree(RedisModuleCtx *ctx, ShardingInfo *si)
{
    freeShardGroupMap(ctx, si->shard_group_map);
    si->shard_group_map = NULL;
    RedisModule_Free(si);
}

/* Free and reset the ShardingInfo structure.
 *
 * This is called after ShardingInfo has already been allocated, and typically
 * right before loading serialized ShardGroups from a snapshot.
 */
void ShardingInfoReset(RedisModuleCtx *ctx, ShardingInfo *si)
{
    freeShardGroupMap(ctx, si->shard_group_map);
    si->shard_group_map = NULL;
    si->shard_group_map = RedisModule_CreateDict(ctx);

    si->shard_groups_num = 0;

    /* Reset array */
    /* Internal mem was already released in freeShardGroupMap() (point to the same obj in shard_group_map */
    for (int i = 0; i < REDIS_RAFT_HASH_SLOTS; i++) {
        si->stable_slots_map[i] = NULL;
        si->importing_slots_map[i] = NULL;
        si->migrating_slots_map[i] = NULL;
    }

    si->is_sharding = false;
}

/* Compute the hash slot for a RaftRedisCommandArray list of commands and update
 * the entry or reply with an error or if it can't be done
 */
RRStatus HashSlotCompute(RedisRaftCtx *rr,
                         RaftRedisCommandArray *cmds,
                         int *slot)
{
    *slot = -1;
    for (int i = 0; i < cmds->len; i++) {
        RaftRedisCommand *cmd = cmds->commands[i];

        /* Iterate command keys */
        int num_keys = 0;
        int *keyindex = RedisModule_GetCommandKeys(rr->ctx, cmd->argv, cmd->argc, &num_keys);
        for (int j = 0; j < num_keys; j++) {
            RedisModuleString *key = cmd->argv[keyindex[j]];

            int thisslot = (int) keyHashSlotRedisString(key);

            if (*slot == -1) {
                /* First key */
                *slot = thisslot;
            } else {
                if (*slot != thisslot) {
                    RedisModule_Free(keyindex);
                    return RR_ERROR;
                }
            }
        }

        RedisModule_Free(keyindex);
    }

    return RR_OK;
}

/* Produces a CLUSTER SLOTS compatible reply entry for the specified local cluster node.
 */
static int addClusterSlotNodeReply(RedisRaftCtx *rr, RedisModuleCtx *ctx, raft_node_t *raft_node)
{
    Node *node = raft_node_get_udata(raft_node);
    NodeAddr *addr;
    char node_id[RAFT_SHARDGROUP_NODEID_LEN + 1];

    /* Stale nodes should not exist but we prefer to be defensive.
     * Our own node doesn't have a connection so we don't expect a Node object.
     */
    if (node) {
        addr = &node->addr;
    } else if (raft_get_my_node(rr->raft) == raft_node) {
        addr = &rr->config.addr;
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

    raftNodeToString(node_id, rr->meta.dbid, raft_node);
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
 */
RedisModuleString *generateSlots(RedisModuleCtx *ctx, ShardGroup *sg)
{
    RedisModuleString *ret = RedisModule_CreateString(ctx, "", 0);

    if (sg->slot_ranges_num == 0) {
        return ret;
    }

    bool first_slot = true;
    for (unsigned int i = 0; i < sg->slot_ranges_num; i++) {
        if (sg->slot_ranges[i].type == SLOTRANGE_TYPE_IMPORTING) {
            continue;
        }

        char slot_str[REDIS_RAFT_MAX_SLOT_CHARS * 2 + 1 + 1] = {0};

        char *slot_ptr = slot_str;
        if (!first_slot) {
            *slot_str = ' ';
            slot_ptr++;
        }

        if (sg->slot_ranges[i].start_slot != sg->slot_ranges[i].end_slot) {
            sprintf(slot_ptr, "%d-%d", sg->slot_ranges[i].start_slot, sg->slot_ranges[i].end_slot);
        } else {
            sprintf(slot_ptr, "%d", sg->slot_ranges[i].start_slot);
        }

        first_slot = false;

        RedisModule_StringAppendBuffer(ctx, ret, slot_str, strlen(slot_str));
    }

    return ret;
}

/* Formats a CLUSTER NODES line and appends it to ret */
static void appendClusterNodeString(RedisModuleString *ret, char node_id[41], NodeAddr *addr, const char *flags,
                                    const char *master_node_id, int ping_sent, int pong_recv, raft_term_t epoch, const char *link_state,
                                    RedisModuleString *slots)
{
    size_t len;
    const char *temp;
    size_t slots_len;
    const char *slots_str;

    slots_str = RedisModule_StringPtrLen(slots, &slots_len);
    const char *master = (master_node_id != NULL) ? master_node_id : "-";
    RedisModuleString *str = RedisModule_CreateStringPrintf(NULL,
                                                            "%s %s:%d@%d %s %s %d %d %ld %s %.*s",
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

static void appendSpecialClusterNodeString(RedisModuleString *ret, unsigned int j, SlotRangeType type, ShardGroupNode *node)
{
    size_t len;
    const char *temp;
    char *direction;

    switch (type) {
        case SLOTRANGE_TYPE_IMPORTING:
            direction = "<";
            break;
        case SLOTRANGE_TYPE_MIGRATING:
            direction = ">";
            break;
        default:
            return;
    }

    RedisModuleString *str = RedisModule_CreateStringPrintf(NULL, " [%u-%s-%.*s]", j, direction, 40, node->node_id);
    temp = RedisModule_StringPtrLen(str, &len);
    RedisModule_StringAppendBuffer(NULL, ret, temp, len);
    RedisModule_FreeString(NULL, str);
}

/* Formats a CLUSTER NODES line from a raft node structure and appends it to ret. */
static void addClusterNodeReplyFromNode(RedisRaftCtx *rr,
                                        RedisModuleString *ret,
                                        raft_node_t *raft_node,
                                        ShardGroup *sg,
                                        RedisModuleString *slots)
{
    ShardGroup **importing_map = rr->sharding_info->importing_slots_map;
    ShardGroup **migrating_map = rr->sharding_info->migrating_slots_map;

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
        addr = &rr->config.addr;
    } else {
        return;
    }

    /* should we record heartbeat and reply times for ping/pong */
    char *master = NULL;
    char *myself_flag = self ? "myself," : "";
    char *role_flag = leader ? "master" : "slave";
    size_t flags_len = strlen(myself_flag) + strlen(role_flag) + 1;
    char flags[flags_len];
    snprintf(flags, flags_len, "%s%s", myself_flag, role_flag);
    int ping_sent = 0;
    int pong_recv = 0;

    char node_id[RAFT_SHARDGROUP_NODEID_LEN + 1];
    raftNodeToString(node_id, rr->meta.dbid, raft_node);

    char master_node_id[RAFT_SHARDGROUP_NODEID_LEN + 1];
    if (!leader) {
        raftNodeIdToString(master_node_id, rr->meta.dbid, raft_get_leader_id(rr->raft));
        master = master_node_id;
    }

    raft_term_t epoch = raft_get_current_term(redis_raft.raft);
    char *link_state = "connected";

    appendClusterNodeString(ret, node_id, addr, flags, master, ping_sent, pong_recv, epoch, link_state, slots);

    if (self) {
        for (unsigned int i = 0; i < sg->slot_ranges_num; i++) {
            ShardGroupSlotRange sr = sg->slot_ranges[i];

            if (sr.type == SLOTRANGE_TYPE_STABLE) {
                continue;
            }

            if (sr.type == SLOTRANGE_TYPE_IMPORTING) {
                for (unsigned int j = sr.start_slot; j <= sr.end_slot; j++) {
                    RedisModule_Assert(j < REDIS_RAFT_HASH_SLOTS);
                    if (migrating_map[j] != NULL && migrating_map[j]->nodes_num > 0) {
                        appendSpecialClusterNodeString(ret, j, sr.type, &migrating_map[j]->nodes[0]);
                    }
                }
            } else if (sr.type == SLOTRANGE_TYPE_MIGRATING) {
                for (unsigned int j = sr.start_slot; j <= sr.end_slot; j++) {
                    RedisModule_Assert(j < REDIS_RAFT_HASH_SLOTS);
                    if (importing_map[j] != NULL && importing_map[j]->nodes_num > 0) {
                        appendSpecialClusterNodeString(ret, j, sr.type, &importing_map[j]->nodes[0]);
                    }
                }
            }
        }
    }

    RedisModule_StringAppendBuffer(NULL, ret, "\n", 1);
}

/* Produce a CLUSTER NODES compatible reply, including:
 *
 * 1. Local cluster's slot range and nodes
 * 2. All configured shardgroups with their slot ranges and nodes.
 */

static void addClusterNodesReply(RedisRaftCtx *rr, RedisModuleCtx *ctx)
{
    raft_node_t *leader_node = getLeaderRaftNodeOrReply(rr, ctx);
    if (!leader_node) {
        return;
    }
    ShardingInfo *si = rr->sharding_info;

    RedisModuleString *ret = RedisModule_CreateString(ctx, "", 0);

    if (si->shard_group_map != NULL) {
        size_t key_len;
        ShardGroup *sg;

        RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(si->shard_group_map, "^", NULL, 0);
        while (RedisModule_DictNextC(iter, &key_len, (void **) &sg) != NULL) {
            RedisModuleString *slots = generateSlots(ctx, sg);

            if (sg->local) {
                for (int j = 0; j < raft_get_num_nodes(rr->raft); j++) {
                    raft_node_t *raft_node = raft_get_node_from_idx(rr->raft, j);
                    if (!raft_node_is_active(raft_node)) {
                        continue;
                    }
                    addClusterNodeReplyFromNode(rr, ret, raft_node, sg, slots);
                }
            } else {
                for (unsigned int j = 0; j < sg->nodes_num; j++) {
                    /* SHARDGROUP GET only works on leader
                     * SHARDGROUP GET lists nodes in order of idx, but 0 will always be self, i.e. leader
                     */
                    char *flags = j == 0 ? "master" : "slave";
                    int ping_sent = 0;
                    int pong_recv = 0;
                    int epoch = 0;
                    char *link_state = "connected";

                    appendClusterNodeString(ret, sg->nodes[j].node_id, &sg->nodes[j].addr, flags, NULL, ping_sent,
                                            pong_recv, epoch, link_state, slots);
                    RedisModule_StringAppendBuffer(NULL, ret, "\n", 1);
                }
            }
            RedisModule_FreeString(ctx, slots);
        }

        RedisModule_DictIteratorStop(iter);
    }

    RedisModule_ReplyWithString(ctx, ret);
    RedisModule_FreeString(ctx, ret);
}

/* Produce a CLUSTER SLOTS compatible reply, including:
 *
 * 1. Local cluster's slot range and nodes.
 * 2. All configured shardgroups with their slot ranges and nodes.
 */

static void addClusterSlotsReply(RedisRaftCtx *rr, RedisModuleCtx *ctx)
{
    raft_node_t *leader_node = getLeaderRaftNodeOrReply(rr, ctx);
    if (!leader_node) {
        return;
    }

    ShardingInfo *si = rr->sharding_info;
    if (!si->shard_group_map) {
        RedisModule_ReplyWithArray(ctx, 0);
        return;
    }

    /* Count slots and initiate array reply */
    unsigned int num_slots = 0;
    size_t key_len;
    ShardGroup *sg;

    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

    /* Return array elements */
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(si->shard_group_map, "^", NULL, 0);
    while (RedisModule_DictNextC(iter, &key_len, (void **) &sg) != NULL) {
        for (unsigned int i = 0; i < sg->slot_ranges_num; i++) {
            if (sg->slot_ranges[i].type == SLOTRANGE_TYPE_IMPORTING) {
                continue;
            }
            num_slots += 1;

            RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

            int slot_len = 0;

            RedisModule_ReplyWithLongLong(ctx, sg->slot_ranges[i].start_slot); /* Start slot */
            RedisModule_ReplyWithLongLong(ctx, sg->slot_ranges[i].end_slot);   /* End slot */
            slot_len += 2;

            /* Dump Raft nodes now. Leader (master) first, followed by others */
            if (sg->local) {
                /* Local cluster's ShardGroup: we list the leader node first,
                 * followed by all cluster nodes we know. This information does not
                 * come from the ShardGroup.
                 */

                slot_len += addClusterSlotNodeReply(rr, ctx, leader_node);
                for (int j = 0; j < raft_get_num_nodes(rr->raft); j++) {
                    raft_node_t *raft_node = raft_get_node_from_idx(rr->raft, j);
                    if (raft_node_get_id(raft_node) == raft_get_leader_id(rr->raft) ||
                        !raft_node_is_active(raft_node)) {
                        continue;
                    }

                    slot_len += addClusterSlotNodeReply(rr, ctx, raft_node);
                }
            } else {
                /* Remote cluster: we simply dump what the ShardGroup configuration
                 * tells us.
                 */

                for (unsigned int j = 0; j < sg->nodes_num; j++) {
                    slot_len += addClusterSlotShardGroupNodeReply(rr, ctx, &sg->nodes[j]);
                }
            }

            RedisModule_ReplySetArrayLength(ctx, slot_len);
        }
    }

    RedisModule_DictIteratorStop(iter);
    RedisModule_ReplySetArrayLength(ctx, num_slots);
}

static void addClusterShardsNodeReply(RedisRaftCtx *rr, RedisModuleCtx *ctx, char *id, uint16_t port, char *host, char *role)
{
    RedisModule_ReplyWithMap(ctx, 7);
    RedisModule_ReplyWithCString(ctx, "id");
    RedisModule_ReplyWithCString(ctx, id);
    if (!rr->config.tls_enabled) {
        RedisModule_ReplyWithCString(ctx, "port");
        RedisModule_ReplyWithLongLong(ctx, port);
    } else {
        RedisModule_ReplyWithCString(ctx, "tls-port");
        RedisModule_ReplyWithLongLong(ctx, port);
    }
    RedisModule_ReplyWithCString(ctx, "ip");
    RedisModule_ReplyWithCString(ctx, host);
    RedisModule_ReplyWithCString(ctx, "endpoint");
    RedisModule_ReplyWithCString(ctx, host);
    RedisModule_ReplyWithCString(ctx, "role");
    RedisModule_ReplyWithCString(ctx, role);
    RedisModule_ReplyWithCString(ctx, "replication-offset");
    RedisModule_ReplyWithLongLong(ctx, 0);
    RedisModule_ReplyWithCString(ctx, "health");
    RedisModule_ReplyWithCString(ctx, "online");
}

static int addClusterShardsLocalNodeReply(RedisRaftCtx *rr, RedisModuleCtx *ctx, raft_node_t *raft_node, raft_node_t *leader)
{
    Node *node = raft_node_get_udata(raft_node);
    NodeAddr *addr;

    char *role = (raft_node_get_id(raft_node) == raft_get_leader_id(rr->raft)) ? "master" : "replica";
    int self = (raft_node_get_id(raft_node) == raft_get_nodeid(rr->raft));

    /* Stale nodes should not exist but we prefer to be defensive.
     * Our own node doesn't have a connection so we don't expect a Node object.
     */
    if (node) {
        addr = &node->addr;
    } else if (self) {
        addr = &rr->config.addr;
    } else {
        return 0;
    }

    char node_id[RAFT_SHARDGROUP_NODEID_LEN + 1];
    raftNodeToString(node_id, rr->meta.dbid, raft_node);

    addClusterShardsNodeReply(rr, ctx, node_id, addr->port, addr->host, role);

    return 1;
}

static void addClusterShardsReply(RedisRaftCtx *rr, RedisModuleCtx *ctx)
{
    raft_node_t *leader_node = getLeaderRaftNodeOrReply(rr, ctx);
    if (!leader_node) {
        return;
    }

    ShardingInfo *si = rr->sharding_info;
    if (!si->shard_group_map) {
        RedisModule_ReplyWithNullArray(ctx);
        return;
    }

    int shard_count = 0;
    size_t key_len;
    ShardGroup *sg;

    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_LEN);
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(si->shard_group_map, "^", NULL, 0);
    while (RedisModule_DictNextC(iter, &key_len, (void **) &sg) != NULL) {
        shard_count++;

        RedisModule_ReplyWithMap(ctx, 2);
        RedisModule_ReplyWithCString(ctx, "slots");

        int slot_count = 0;
        RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_LEN);
        for (unsigned int i = 0; i < sg->slot_ranges_num; i++) {
            /* we only include slot ranges for a shard that are stable / migration */
            SlotRangeType type = sg->slot_ranges[i].type;
            if (type != SLOTRANGE_TYPE_STABLE && type != SLOTRANGE_TYPE_MIGRATING) {
                continue;
            }
            slot_count += 1;
            RedisModule_ReplyWithLongLong(ctx, sg->slot_ranges[i].start_slot);
            RedisModule_ReplyWithLongLong(ctx, sg->slot_ranges[i].end_slot);
        }
        RedisModule_ReplySetArrayLength(ctx, slot_count * 2);

        RedisModule_ReplyWithCString(ctx, "nodes");
        if (sg->local) {
            int node_count = 0;
            RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_LEN);

            for (int j = 0; j < raft_get_num_nodes(rr->raft); j++) {
                raft_node_t *raft_node = raft_get_node_from_idx(rr->raft, j);
                if (!raft_node_is_active(raft_node)) {
                    continue;
                }
                node_count += addClusterShardsLocalNodeReply(rr, ctx, raft_node, leader_node);
            }
            RedisModule_ReplySetArrayLength(ctx, node_count);
        } else {
            RedisModule_ReplyWithArray(ctx, sg->nodes_num);
            for (unsigned int j = 0; j < sg->nodes_num; j++) {
                char *node_id = sg->nodes[j].node_id;
                uint16_t port = sg->nodes[j].addr.port;
                char *host = sg->nodes[j].addr.host;
                char *role = (j == 0) ? "master" : "replica";

                addClusterShardsNodeReply(rr, ctx, node_id, port, host, role);
            }
        }
    }

    RedisModule_DictIteratorStop(iter);
    RedisModule_ReplySetArrayLength(ctx, shard_count);
}

/* Process CLUSTER commands, as intercepted earlier by the Raft module.
 *
 * Currently only supports:
 *   - SLOTS.
 *   - NODES.
 *   - SHARDS,
 */
void ShardingHandleClusterCommand(RedisRaftCtx *rr,
                                  RedisModuleCtx *ctx, RaftRedisCommand *cmd)
{
    if (cmd->argc != 2) {
        RedisModule_WrongArity(ctx);
        return;
    }

    if (checkRaftState(rr, ctx) == RR_ERROR) {
        return;
    }

    size_t cmd_len;
    const char *cmd_str = RedisModule_StringPtrLen(cmd->argv[1], &cmd_len);

    if (cmd_len == 5 && !strncasecmp(cmd_str, "SLOTS", 5)) {
        addClusterSlotsReply(rr, ctx);
    } else if (cmd_len == 5 && !strncasecmp(cmd_str, "NODES", 5)) {
        addClusterNodesReply(rr, ctx);
    } else if (cmd_len == 6 && !strncasecmp(cmd_str, "SHARDS", 6)) {
        addClusterShardsReply(rr, ctx);
    } else {
        RedisModule_ReplyWithError(ctx, "ERR Unknown subcommand.");
    }
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
        LOG_WARNING("RAFT.SHARDGROUP GET failed: connection dropped.");
    } else if (reply->type == REDIS_REPLY_ERROR) {
        /* -MOVED? */
        if (strlen(reply->str) > 6 && !strncmp(reply->str, "MOVED ", 6)) {
            NodeAddr addr;
            if (!parseMovedReply(reply->str, &addr)) {
                LOG_WARNING("RAFT.SHARDGROUP GET failed: invalid MOVED response: %s", reply->str);
            } else {
                LOG_VERBOSE("RAFT.SHARDGROUP GET redirected to leader: %s:%d",
                            addr.host, addr.port);
                NodeAddrListAddElement(&state->addr, &addr);
            }
        } else {
            LOG_WARNING("RAFT.SHARDGROUP GET failed: %s", reply->str);
        }
    } else {
        ShardGroup recv_sg;
        ShardGroupInit(&recv_sg);

        if (parseShardGroupReply(reply, &recv_sg) == RR_ERROR) {
            LOG_WARNING("RAFT.SHARDGROUP GET invalid reply.");
        } else {
            /* Validate */
            if (ShardingInfoValidateShardGroup(rr, &recv_sg) != RR_OK) {
                LOG_WARNING("Received shardgroup failed validation!");
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
void ShardGroupLink(RedisRaftCtx *rr,
                    RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    (void) argc;
    size_t len;
    const char *str = RedisModule_StringPtrLen(argv[2], &len);
    NodeAddr addr = {0};

    if (!NodeAddrParse(str, len, &addr)) {
        RedisModule_ReplyWithError(ctx, "invalid address/port specified");
        return;
    }

    LOG_NOTICE("Attempting to link shardgroup %s:%u", addr.host, addr.port);

    JoinLinkState *st = RedisModule_Calloc(1, sizeof(*st));

    NodeAddrListAddElement(&st->addr, &addr);
    st->type = "link";
    st->connect_callback = linkSendRequest;
    st->start = time(NULL);
    st->req = RaftReqInit(ctx, RR_SHARDGROUP_LINK);
    st->conn = ConnCreate(rr, st, joinLinkIdleCallback, joinLinkFreeCallback,
                          rr->config.cluster_user, rr->config.cluster_password);
}

void ShardGroupGet(RedisRaftCtx *rr, RedisModuleCtx *ctx)
{
    ShardGroup *sg = GetShardGroupById(rr, rr->meta.dbid);

    /* 2 arrays
     * 1. slot ranges -> each element is a 3 element array start/end/type
     * 2. nodes -> each element is a 2 element array id/address
     */
    RedisModule_ReplyWithArray(ctx, 3);
    RedisModule_ReplyWithCString(ctx, redis_raft.snapshot_info.dbid);
    RedisModule_ReplyWithArray(ctx, sg->slot_ranges_num);

    for (unsigned int i = 0; i < sg->slot_ranges_num; i++) {
        ShardGroupSlotRange *sr = &sg->slot_ranges[i];
        RedisModule_ReplyWithArray(ctx, 3);
        RedisModule_ReplyWithLongLong(ctx, sr->start_slot);
        RedisModule_ReplyWithLongLong(ctx, sr->end_slot);
        RedisModule_ReplyWithLongLong(ctx, sr->type);
    }

    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

    int node_count = 0;
    for (int i = 0; i < raft_get_num_nodes(rr->raft); i++) {
        raft_node_t *raft_node = raft_get_node_from_idx(rr->raft, i);
        if (!raft_node_is_active(raft_node)) {
            continue;
        }

        NodeAddr addr;
        if (raft_node == raft_get_my_node(rr->raft)) {
            addr = rr->config.addr;
        } else {
            Node *node = raft_node_get_udata(raft_node);
            if (!node) {
                continue;
            }

            addr = node->addr;
        }

        node_count++;
        RedisModule_ReplyWithArray(ctx, 2);

        char node_id[RAFT_SHARDGROUP_NODEID_LEN + 1];
        raftNodeToString(node_id, rr->meta.dbid, raft_node);
        RedisModule_ReplyWithStringBuffer(ctx, node_id, strlen(node_id));

        char addrstr[512];
        snprintf(addrstr, sizeof(addrstr), "%s:%u", addr.host, addr.port);
        RedisModule_ReplyWithStringBuffer(ctx, addrstr, strlen(addrstr));
    }

    RedisModule_ReplySetArrayLength(ctx, node_count);
}

void ShardGroupAdd(RedisRaftCtx *rr,
                   RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    int ret;
    int num_elems;
    ShardGroup *sg = ShardGroupParse(ctx, &argv[2], argc - 2, 2, &num_elems);

    if (sg == NULL) {
        /* Error reply already produced by parseShardGroupFromArgs */
        return;
    }

    /* Validate now before pushing this as a log entry. */
    if (ShardingInfoValidateShardGroup(rr, sg) != RR_OK) {
        RedisModule_ReplyWithError(ctx, "ERR invalid shardgroup configuration. Consult the logs for more info.");
        goto out;
    }

    RaftReq *req = RaftReqInit(ctx, RR_SHARDGROUP_ADD);

    ret = ShardGroupAppendLogEntry(rr, sg, RAFT_LOGTYPE_ADD_SHARDGROUP, req);
    if (ret != RR_OK) {
        RedisModule_ReplyWithError(ctx, "ERR shardgroup add failed, please check logs.");
        RaftReqFree(req);
        goto out;
    }

out:
    ShardGroupFree(sg);
}

void ShardGroupReplace(RedisRaftCtx *rr,
                       RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    int ret;
    int num_elems;
    ShardGroup **sg = ShardGroupsParse(ctx, &argv[2], argc - 2, &num_elems);

    if (sg == NULL) {
        /* Error reply already produced by parseShardGroupFromArgs */
        return;
    }

    RaftReq *req = RaftReqInit(ctx, RR_SHARDGROUPS_REPLACE);

    ret = ShardGroupsAppendLogEntry(rr, num_elems, sg,
                                    RAFT_LOGTYPE_REPLACE_SHARDGROUPS, req);
    if (ret != RR_OK) {
        RedisModule_ReplyWithError(req->ctx, "failed, please check logs.");
        RaftReqFree(req);
        goto out;
    }

out:
    for (int i = 0; i < num_elems; i++) {
        ShardGroupFree(sg[i]);
    }
    RedisModule_Free(sg);
}
