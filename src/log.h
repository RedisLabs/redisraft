/*
 * Copyright Redis Ltd. 2022 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef REDISRAFT_LOG_H
#define REDISRAFT_LOG_H

#include "file.h"
#include "raft.h"
#include "redisraft.h"

#include <limits.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

extern raft_log_impl_t LogImpl;

typedef struct Log {
    uint32_t version;               /* Log file format version */
    char dbid[RAFT_DBID_LEN + 1];   /* DB unique ID */
    raft_node_id_t node_id;         /* Node ID */
    raft_term_t snapshot_last_term; /* Last term included in snapshot */
    raft_index_t snapshot_last_idx; /* Last index included in snapshot */
    raft_index_t num_entries;       /* Entries in log */
    raft_index_t index;             /* Index of last entry */
    char filename[PATH_MAX];        /* Log file name */
    char idxfilename[PATH_MAX];     /* Index file name */
    File file;                      /* Log file */
    File idxfile;                   /* Index file descriptor */
    raft_index_t fsync_index;       /* Last entry index included in the latest fsync() call */
    uint64_t fsync_count;           /* Count of fsync() calls */
    uint64_t fsync_max;             /* Slowest fsync() call in microseconds */
    uint64_t fsync_total;           /* Total time fsync() calls consumed in microseconds */
    long current_crc;               /* current running crc value for log */
} Log;

Log *LogCreate(const char *filename, const char *dbid,
               raft_term_t snapshot_term, raft_index_t snapshot_index,
               raft_node_id_t node_id);
void LogFree(Log *raft_log);
Log *LogOpen(const char *filename, bool keep_index);
int LogAppend(Log *log, raft_entry_t *entry);
int LogLoadEntries(Log *log);
int LogSync(Log *log, bool sync);
raft_entry_t *LogGet(Log *log, raft_index_t idx);
int LogDelete(Log *log, raft_index_t from_idx);
int LogReset(Log *log, raft_index_t index, raft_term_t term);
raft_index_t LogCount(Log *log);
raft_index_t LogFirstIdx(Log *log);
raft_index_t LogCurrentIdx(Log *log);
size_t LogFileSize(Log *log);
void LogArchiveFiles(Log *log);

Log *LogRewrite(RedisRaftCtx *rr, const char *filename,
               raft_index_t last_idx, raft_term_t last_term,
	       unsigned long *num_entries);
int LogRewriteSwitch(RedisRaftCtx *rr, Log *new_log,
                     raft_index_t new_log_entries);

#endif
