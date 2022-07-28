/*
 * This file is part of RedisRaft.
 *
 * Copyright (c) 2020-2021 Redis Ltd.
 *
 * RedisRaft is licensed under the Redis Source Available License (RSAL).
 */

#ifndef REDISRAFT_H
#define REDISRAFT_H

#include "config.h"
#include "connection.h"
#include "entrycache.h"
#include "fsync.h"
#include "log.h"
#include "meta.h"
#include "node.h"
#include "queue.h"
#include "raft.h"
#include "redismodule.h"
#include "serialization.h"
#include "snapshot.h"
#include "threadpool.h"
#include "version.h"

#include "hiredis/async.h"
#include "hiredis/hiredis.h"

#include <errno.h>
#include <netinet/in.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <time.h>
#include <unistd.h>

/* Disable GNU attributes for non-GNU compilers */
#ifndef __GNUC__
#define __attribute__(a)
#endif

#ifndef MIN
#define MIN(a, b) (((a) < (b)) ? (a) : (b))
#endif

#ifndef MAX
#define MAX(a, b) (((a) < (b)) ? (b) : (a))
#endif

extern int redisraft_loglevel;
extern const char *redisraft_loglevels[];
extern int redisraft_loglevel_enums[];
extern RedisModuleCtx *redisraft_log_ctx;

#define LOG_LEVEL_DEBUG   0
#define LOG_LEVEL_VERBOSE 1
#define LOG_LEVEL_NOTICE  2
#define LOG_LEVEL_WARNING 3
#define LOG_LEVEL_COUNT   (LOG_LEVEL_WARNING + 1)

#define LOG(level, fmt, ...)                                                 \
    do {                                                                     \
        if (level >= redisraft_loglevel) {                                   \
            RedisModule_Log(redisraft_log_ctx,                               \
                            redisraft_loglevels[level], fmt, ##__VA_ARGS__); \
        }                                                                    \
    } while (0)

#define LOG_DEBUG(fmt, ...)   LOG(LOG_LEVEL_DEBUG, fmt, ##__VA_ARGS__)
#define LOG_VERBOSE(fmt, ...) LOG(LOG_LEVEL_VERBOSE, fmt, ##__VA_ARGS__)
#define LOG_NOTICE(fmt, ...)  LOG(LOG_LEVEL_NOTICE, fmt, ##__VA_ARGS__)
#define LOG_WARNING(fmt, ...) LOG(LOG_LEVEL_WARNING, fmt, ##__VA_ARGS__)

#define PANIC(fmt, ...)                                      \
    do {                                                     \
        LOG_WARNING("\n\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n"    \
                    "REDIS RAFT PANIC\n"                     \
                    "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n\n" fmt, \
                    ##__VA_ARGS__);                          \
        abort();                                             \
    } while (0)

/* Request types. */
enum RaftReqType {
    RR_GENERIC = 1,
    RR_CLUSTER_JOIN,
    RR_CFGCHANGE_ADDNODE,
    RR_CFGCHANGE_REMOVENODE,
    RR_REDISCOMMAND,
    RR_TRANSFER_LEADER,
};

typedef struct RaftReq {
    int type;
    RedisModuleBlockedClient *client;
    RedisModuleCtx *ctx;
    Command cmd;
} RaftReq;

RaftReq *RaftReqInit(RedisModuleCtx *ctx, enum RaftReqType type);
void RaftReqFree(RaftReq *req);

/* General state of the module */
typedef enum RedisRaftState {
    REDIS_RAFT_UNINITIALIZED, /* Waiting for RAFT.CLUSTER command */
    REDIS_RAFT_UP,            /* Up and running */
    REDIS_RAFT_JOINING        /* Processing a RAFT.CLUSTER JOIN command */
} RedisRaftState;

/* Global Raft context */
typedef struct RedisRaftCtx {
    void *raft;                          /* Raft library context */
    RedisModuleCtx *ctx;                 /* Redis module thread-safe context; only used to push
                                                    commands we get from the leader. */
    RedisRaftState state;                /* Raft module state */
    ThreadPool thread_pool;              /* Thread pool for slow operations */
    FsyncThread fsync_thread;            /* Thread to call fsync on raft log file */
    RaftLog *log;                        /* Raft persistent log; May be NULL if not used */
    RaftMeta meta;                       /* Raft metadata for voted_for and term */
    EntryCache *logcache;                /* Log entry cache to keep entries in memory for faster access */
    RedisRaftConfig config;              /* User provided configuration */
    raft_index_t incoming_snapshot_idx;  /* Incoming snapshot's last included idx to verify chunks
                                                    belong to the same snapshot */
    char incoming_snapshot_file[256];    /* File name for incoming snapshots. When received fully,
                                                    it will be renamed to the original rdb file */
    SnapshotFile outgoing_snapshot_file; /* Snapshot file memory map to send to followers */
    RaftSnapshotInfo snapshot_info;      /* Current snapshot info */
    struct RaftReq *transfer_req;        /* RaftReq if a leader transfer is in progress */
    char *value;                         /* Single string as the Raft state (This is the data-set in a Raft cluster).  */

} RedisRaftCtx;

extern RedisRaftCtx redis_raft;

extern raft_log_impl_t RaftLogImpl;

typedef enum RRStatus {
    RR_OK = 0,
    RR_ERROR
} RRStatus;

typedef struct {
    raft_node_id_t id;
    NodeAddr addr;
} RaftCfgChange;

#endif
