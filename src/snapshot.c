/*
 * This file is part of RedisRaft.
 *
 * Copyright (c) 2020-2021 Redis Ltd.
 *
 * RedisRaft is licensed under the Redis Source Available License (RSAL).
 */

#include "entrycache.h"
#include "fsync.h"
#include "node_addr.h"
#include "redisraft.h"
#include "util.h"

#include <assert.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>

void SnapshotInit(RedisRaftCtx *ctx)
{
    ctx->outgoing_snapshot_file.mmap = NULL;
    ctx->outgoing_snapshot_file.len = 0;

    /* Generate temp file name for incoming snapshots */
    snprintf(ctx->incoming_snapshot_file, sizeof(ctx->incoming_snapshot_file),
             "%s.tmp.recv", ctx->config.snapshot_filename);

    /* Delete if there is a partial incoming snapshot file from previous run */
    int ret = unlink(ctx->incoming_snapshot_file);
    if (ret != 0 && errno != ENOENT) {
        LOG_WARNING("Unlink file:%s, error :%s \n", ctx->incoming_snapshot_file,
                    strerror(errno));
    }
}

static int readSnapshotFile()
{
    RedisRaftCtx *rr = &redis_raft;
    RaftSnapshotInfo *info = &redis_raft.snapshot_info;

    FILE *fp = fopen(rr->config.snapshot_filename, "r");
    if (!fp) {
        abort();
    }

    fscanf(fp, "%ld", &info->last_applied_term);
    fscanf(fp, "%ld", &info->last_applied_idx);

    /* Load configuration */
    int node_count;
    fscanf(fp, "%d", &node_count);

    for (int i = 0; i < node_count; i++) {
        raft_node_id_t id;
        int voting;
        NodeAddr addr = {0};

        fscanf(fp, "%d %d %s %hd", &id, &voting, addr.host, &addr.port);

        if (id == raft_get_nodeid(rr->raft)) {
            raft_node_t *rn = raft_get_node(rr->raft, id);
            assert(rn);
            assert(rn == raft_get_my_node(rr->raft));

            raft_node_set_active(rn, 1);
            if (voting != raft_node_is_voting(rn)) {
                raft_node_set_voting(rn, voting);
                raft_node_set_voting_committed(rn, voting);
            }
        } else {
            Node *n = NodeCreate(rr, id, &addr);
            raft_node_t *rn;

            if (voting) {
                rn = raft_add_node(rr->raft, n, id, 0);
            } else {
                rn = raft_add_non_voting_node(rr->raft, n, id, 0);
            }

            RedisModule_Assert(rn != NULL);

            LOG_DEBUG("Snapshot Load: adding node %d: %s:%d: voting=%s",
                      id, addr.host, addr.port, voting ? "yes" : "no");
        }
    }

    /* Load used node ids */
    fscanf(fp, "%d", &info->used_id_count);

    info->used_node_ids = RedisModule_Realloc(info->used_node_ids,
                                              sizeof(size_t) * info->used_id_count);

    for (int i = 0; i < info->used_id_count; i++) {
        raft_node_id_t id;

        fscanf(fp, "%d", &id);
        info->used_node_ids[i] = id;
    }

    fclose(fp);

    return REDISMODULE_OK;
}

static void writeSnapshotFile()
{
    RedisRaftCtx *rr = &redis_raft;
    RaftSnapshotInfo *info = &redis_raft.snapshot_info;

    char tmp[1024];
    snprintf(tmp, sizeof(tmp), "%s.tmp.%d", rr->config.snapshot_filename, getpid());

    FILE *fp = fopen(tmp, "w+");
    if (!fp) {
        abort();
    }

    fprintf(fp, "%ld ", info->last_applied_term);
    fprintf(fp, "%ld ", info->last_applied_idx);

    /* Create a snapshot of the nodes configuration */
    int node_count = 0;

    for (int i = 0; i < raft_get_num_nodes(rr->raft); i++) {
        raft_node_t *n = raft_get_node_from_idx(rr->raft, i);
        if (!raft_node_is_addition_committed(n)) {
            continue;
        }
        node_count++;
    }
    fprintf(fp, "%d ", node_count);

    for (int i = 0; i < raft_get_num_nodes(rr->raft); i++) {
        raft_node_t *n = raft_get_node_from_idx(rr->raft, i);
        /* Skip uncommitted nodes from the snapshot */
        if (!raft_node_is_addition_committed(n)) {
            continue;
        }

        NodeAddr *na = NULL;
        Node *node = raft_node_get_udata(n);

        if (raft_node_get_id(n) == raft_get_nodeid(rr->raft)) {
            na = &rr->config.addr;
        } else if (node != NULL) {
            na = &node->addr;
        }

        assert(na);

        fprintf(fp, "%d %d %s %hd ",
                raft_node_get_id(n),
                raft_node_is_voting_committed(n),
                na->host, na->port);
    }

    fprintf(fp, "%d ", rr->snapshot_info.used_id_count);

    for (int i = 0; i < rr->snapshot_info.used_id_count; i++) {
        fprintf(fp, "%d ", rr->snapshot_info.used_node_ids[i]);
    }

    fclose(fp);
    rename(tmp, rr->config.snapshot_filename);
}

/* Create memory-map of the snapshot file. This is used when leader wants to
 * send snapshot to followers. */
static void createOutgoingSnapshotMmap(RedisRaftCtx *ctx)
{
    if (ctx->outgoing_snapshot_file.mmap != NULL) {
        munmap(ctx->outgoing_snapshot_file.mmap, ctx->outgoing_snapshot_file.len);

        ctx->outgoing_snapshot_file.mmap = NULL;
        ctx->outgoing_snapshot_file.len = 0;
    }

    const int mode = S_IRUSR | S_IRGRP | S_IROTH;
    struct stat st;

    int fd = open(ctx->config.snapshot_filename, O_RDONLY, mode);
    if (fd == -1) {
        PANIC("Cannot open rdb file at : %s \n", ctx->config.snapshot_filename);
    }

    int rc = stat(ctx->config.snapshot_filename, &st);
    if (rc != 0) {
        PANIC("stat failed: %s \n", strerror(errno));
    }

    void *p = mmap(NULL, st.st_size, PROT_READ, MAP_SHARED, fd, 0);
    if (p == MAP_FAILED) {
        PANIC("mmap failed: %s \n", strerror(errno));
    }

    close(fd);

    ctx->outgoing_snapshot_file.mmap = p;
    ctx->outgoing_snapshot_file.len = st.st_size;
}

void SnapshotLoad(RedisRaftCtx *rr)
{
    SnapshotInit(rr);

    if (access(rr->config.snapshot_filename, F_OK) != 0) {
        /* Snapshot file does not exist. */
        return;
    }

    readSnapshotFile();
    createOutgoingSnapshotMmap(rr);
}

int SnapshotSave(RedisRaftCtx *rr)
{
    LOG_DEBUG("Initiating snapshot.");

    FsyncThreadWaitUntilCompleted(&rr->fsync_thread);

    if (RaftLogSync(rr->log, true) != RR_OK) {
        PANIC("RaftLogSync() failed.");
    }

    if (raft_begin_snapshot(rr->raft) < 0) {
        return RR_ERROR;
    }

    writeSnapshotFile();
    createOutgoingSnapshotMmap(rr);

    char tmp_log_file[256];
    snprintf(tmp_log_file, sizeof(tmp_log_file) - 1, "%s.tmp", rr->config.log_filename);

    unsigned long num_log_entries;

    /* Rewrite any additional log entries beyond the snapshot to a new
     * log file.
     */
    num_log_entries = RaftLogRewrite(rr, tmp_log_file,
                                     rr->snapshot_info.last_applied_idx,
                                     rr->snapshot_info.last_applied_term);

    RaftLog *new_log = RaftLogOpen(tmp_log_file);
    if (!new_log) {
        abort();
    }

    LOG_NOTICE("Log rewrite complete, %lu entries rewritten (from idx %lu).",
                num_log_entries, raft_get_snapshot_last_idx(rr->raft));

    RaftLogClose(rr->log);

    int ret = rename(tmp_log_file, rr->log->filename);
    if (ret != 0) {
        abort();
    }

    /* Open new log file */
    rr->log = RaftLogOpen(rr->log->filename);
    int entries = RaftLogLoadEntries(rr->log, NULL, NULL);
    if (entries < 0) {
        abort();
    }

    /* Finalize snapshot */
    raft_end_snapshot(rr->raft);

    return RR_OK;
}

/* After a snapshot is received, load it into the Raft library:
 * 1. Replace received snapshot file with the current one.
 * 2. Load rdb file.
 * 3. Configure index/term/etc.
 * 4. Reconfigure nodes based on the snapshot metadata configuration.
 * 5. Create a new snapshot memory map.
 */
int raftLoadSnapshot(raft_server_t *raft, void *user_data, raft_index_t index, raft_term_t term)
{
    int ret;
    RedisRaftCtx *rr = user_data;

    ret = rename(rr->incoming_snapshot_file, rr->config.snapshot_filename);
    if (ret != 0) {
        LOG_WARNING("rename(): %s to %s failed with error : %s \n",
                    rr->incoming_snapshot_file, rr->config.snapshot_filename, strerror(errno));
        return -1;
    }

    LOG_DEBUG("Beginning snapshot load, term=%lu, index=%lu", term, index);

    if (raft_begin_load_snapshot(rr->raft, term, index) != 0) {
        LOG_DEBUG("Cannot load snapshot: already loaded?");
        return -1;
    }

    readSnapshotFile();
    raft_end_load_snapshot(rr->raft);

    /* Restart the log where the snapshot ends */
    if (rr->log) {
        FsyncThreadWaitUntilCompleted(&rr->fsync_thread);
        RaftLogClose(rr->log);
        rr->log = RaftLogCreate(rr->config.log_filename,
                                rr->snapshot_info.last_applied_term,
                                rr->snapshot_info.last_applied_idx,
                                rr->config.id);
        EntryCacheDeleteHead(rr->logcache, raft_get_snapshot_last_idx(rr->raft) + 1);
    }

    createOutgoingSnapshotMmap(rr);

    return 0;
}

static void handleSnapshotResponse(redisAsyncContext *c, void *r, void *privdata)
{
    Node *node = privdata;
    RedisRaftCtx *rr = node->rr;

    redisReply *reply = r;

    NodeDismissPendingResponse(node);

    if (!reply) {
        ConnMarkDisconnected(node->conn);
        return;
    }

    if (reply->type == REDIS_REPLY_ERROR) {
        return;
    }

    if (reply->type != REDIS_REPLY_ARRAY || reply->elements != 5 ||
        reply->element[0]->type != REDIS_REPLY_INTEGER ||
        reply->element[1]->type != REDIS_REPLY_INTEGER ||
        reply->element[2]->type != REDIS_REPLY_INTEGER ||
        reply->element[3]->type != REDIS_REPLY_INTEGER ||
        reply->element[4]->type != REDIS_REPLY_INTEGER) {
        LOG_WARNING("invalid RAFT.SNAPSHOT reply");
        return;
    }

    raft_snapshot_resp_t response = {
        .term = reply->element[0]->integer,
        .msg_id = reply->element[1]->integer,
        .offset = reply->element[2]->integer,
        .success = reply->element[3]->integer,
        .last_chunk = reply->element[4]->integer,
    };

    raft_node_t *raft_node = raft_get_node(rr->raft, node->id);
    if (!raft_node) {
        LOG_DEBUG("RAFT.SNAPSHOT stale reply.");
        return;
    }

    int ret = raft_recv_snapshot_response(rr->raft, raft_node, &response);
    if (ret != 0) {
        LOG_DEBUG("raft_recv_snapshot_response() failed, error %d", ret);
    }
}

int raftSendSnapshot(raft_server_t *raft,
                     void *user_data,
                     raft_node_t *raft_node,
                     raft_snapshot_req_t *msg)
{
    Node *node = raft_node_get_udata(raft_node);

    char target_node_id[32];
    snprintf(target_node_id, sizeof(target_node_id), "%d", node->id);

    char source_node_id[32];
    snprintf(source_node_id, sizeof(source_node_id), "%d", raft_get_nodeid(raft));

    char msgstr[256];
    snprintf(msgstr, sizeof(msgstr), "%lu:%d:%lu:%lu:%lu:%llu:%d",
             msg->term,
             msg->leader_id,
             msg->msg_id,
             msg->snapshot_index,
             msg->snapshot_term,
             msg->chunk.offset,
             msg->chunk.last_chunk);

    const char *args[] = {
        "RAFT.SNAPSHOT",
        target_node_id,
        source_node_id,
        msgstr,
        msg->chunk.data,
    };

    size_t args_len[] = {
        strlen(args[0]),
        strlen(args[1]),
        strlen(args[2]),
        strlen(args[3]),
        msg->chunk.len,
    };

    if (!ConnIsConnected(node->conn)) {
        return -1;
    }

    int ret = redisAsyncCommandArgv(ConnGetRedisCtx(node->conn),
                                    handleSnapshotResponse, node, 5,
                                    args, args_len);
    if (ret != REDIS_OK) {
        return -1;
    }

    NodeAddPendingResponse(node);

    return 0;
}

int raftClearSnapshot(raft_server_t *raft, void *user_data)
{
    RedisRaftCtx *rr = user_data;

    int ret = unlink(rr->incoming_snapshot_file);
    if (ret != 0 && errno != ENOENT) {
        LOG_WARNING("Unlink file:%s, error :%s \n", rr->incoming_snapshot_file,
                    strerror(errno));
    }

    return 0;
}

int raftGetSnapshotChunk(raft_server_t *raft, void *user_data,
                         raft_node_t *raft_node, unsigned long long offset,
                         raft_snapshot_chunk_t *chunk)
{

    RedisRaftCtx *rr = user_data;
    Node *node = raft_node_get_udata(raft_node);

    /* To apply some backpressure, we limit max message count on the fly */
    if (!ConnIsConnected(node->conn) ||
        node->pending_raft_response_num >= rr->config.snapshot_req_max_count) {
        return RAFT_ERR_DONE;
    }

    const raft_size_t max_chunk_size = rr->config.snapshot_req_max_size;
    const raft_size_t remaining_bytes = rr->outgoing_snapshot_file.len - offset;

    chunk->len = MIN(max_chunk_size, remaining_bytes);
    if (chunk->len == 0) {
        /* All chunks are sent */
        return RAFT_ERR_DONE;
    }

    chunk->data = (char *) rr->outgoing_snapshot_file.mmap + offset;
    chunk->last_chunk = (offset + chunk->len == rr->outgoing_snapshot_file.len);

    return 0;
}


int raftStoreSnapshotChunk(raft_server_t *raft, void *user_data,
                           raft_index_t snapshot_index,
                           raft_size_t offset,
                           raft_snapshot_chunk_t *chunk)
{
    RedisRaftCtx *rr = user_data;

    if (offset == 0) {
        rr->incoming_snapshot_idx = snapshot_index;
    }

    if (rr->incoming_snapshot_idx != snapshot_index) {
        PANIC("Snapshot index was : %ld, received a chunk for %ld \n",
              rr->incoming_snapshot_idx, snapshot_index);
    }

    int flags = O_WRONLY | O_CREAT;
    if (offset == 0) {
        flags |= O_TRUNC;
    }

    int fd = open(rr->incoming_snapshot_file, flags, S_IWUSR | S_IRUSR);
    if (fd == -1) {
        LOG_WARNING("open file:%s, error:%s \n", rr->incoming_snapshot_file,
                    strerror(errno));
        return -1;
    }

    off_t ret_offset = lseek(fd, offset, SEEK_CUR);
    if (ret_offset != (off_t) offset) {
        LOG_WARNING("lseek file:%s, error:%s \n", rr->incoming_snapshot_file,
                    strerror(errno));
        close(fd);
        return -1;
    }

    size_t len = write(fd, chunk->data, chunk->len);
    if (len != chunk->len) {
        LOG_WARNING("write, written: %zu, chunk len : %llu, err :%s \n", len,
                    chunk->len, strerror(errno));
        close(fd);
        return -1;
    }

    close(fd);
    return 0;
}
