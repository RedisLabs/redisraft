//
// Created by ozan on 23.07.2022.
//

#ifndef REDISRAFT_SNAPSHOT_H
#define REDISRAFT_SNAPSHOT_H

#include "raft.h"
#include "node_addr.h"

#include <stdbool.h>

typedef struct RaftSnapshotInfo {
    raft_term_t last_applied_term;
    raft_index_t last_applied_idx;

    /* All node ids that are, or have ever been, part of this cluster */
    raft_node_id_t *used_node_ids;
    int used_id_count;
} RaftSnapshotInfo;

typedef struct SnapshotFile {
    void *mmap;
    size_t len;
} SnapshotFile;

struct RedisRaftCtx;

void SnapshotInit(struct RedisRaftCtx *ctx);
void SnapshotLoad(struct RedisRaftCtx *rr);
int SnapshotSave(struct RedisRaftCtx *rr);

/* Libraft callbacks */
int raftLoadSnapshot(raft_server_t *raft, void *udata, raft_index_t idx, raft_term_t term);
int raftSendSnapshot(raft_server_t *raft, void *udata, raft_node_t *node, raft_snapshot_req_t *msg);
int raftClearSnapshot(raft_server_t *raft, void *udata);
int raftGetSnapshotChunk(raft_server_t *raft, void *udata, raft_node_t *node, raft_size_t offset, raft_snapshot_chunk_t *chunk);
int raftStoreSnapshotChunk(raft_server_t *raft, void *udata, raft_index_t idx, raft_size_t offset, raft_snapshot_chunk_t *chunk);

#endif
